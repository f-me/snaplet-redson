{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

{-|

CRUD for JSON data with Redis storage.

Can be used as Backbone.sync backend.

-}

module Snap.Snaplet.Redson
    ( Redson
    , redsonInit
    , redsonInitWithHooks
    )

where

import qualified Prelude (id)
import Prelude hiding (concat, FilePath, id, read)

import Control.Applicative
import Control.Monad.State hiding (put)

import Data.Aeson as A

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as LB (ByteString)

import Data.Configurator

import Data.Lens.Common
import Data.Maybe
import qualified Data.Map as M

import Snap.Core
import Snap.Snaplet
import Snap.Snaplet.Auth
import Snap.Snaplet.RedisDB

import Network.WebSockets
import Network.WebSockets.Snap
import qualified Network.WebSockets.Util.PubSub as PS

import Database.Redis hiding (auth)


import qualified Snap.Snaplet.Redson.Snapless.CRUD as CRUD
import Snap.Snaplet.Redson.Snapless.Metamodel
import Snap.Snaplet.Redson.Snapless.Metamodel.Loader (loadModels)
import Snap.Snaplet.Redson.Permissions
import Snap.Snaplet.Redson.Util
import Snap.Snaplet.Redson.Internals

import Snap.Snaplet.Redson.Snapless.Index.Config as Ix
  (readConfig,IndexConfig(..),IndexType(..),IndexMap(..))
import Snap.Snaplet.Redson.Snapless.Index.InvertedRedis as Ix1
  (create, read, update)



------------------------------------------------------------------------------
-- | Perform action with AuthManager.
withAuth :: (MonadState (Redson b1) (m b1 v), MonadSnaplet m) =>
            m b1 (AuthManager b1) b -> m b1 v b
withAuth = (gets auth >>=) . flip withTop


------------------------------------------------------------------------------
-- | Top-level (per-form) security checking.
--
-- Reject request if no user is logged in or metamodel is unknown or
-- user has no permissions for CRUD method; otherwise perform given
-- handler action with user and metamodel as arguments. In transparent
-- mode, always perform the action without any checks.
--
-- If security checks are in effect and succeed, action is always
-- called with Just constructor of Maybe Model.
withCheckSecurity :: (Either SuperUser AuthUser -> Maybe Model
                  -> Handler b (Redson b) ())
                  -> Handler b (Redson b) ()
withCheckSecurity action = do
  mdl <- getModel
  trs <- gets transparent
  case trs of
    True -> action (Left SuperUser) mdl
    False -> do
      m <- getsRequest rqMethod
      au <- withAuth currentUser
      case (au, mdl) of
        (Nothing, _) -> handleError unauthorized
        (_, Nothing) -> handleError forbidden
        (Just user, Just model) ->
           case (elem m $ getModelPermissions (Right user) model) of
             True -> action (Right user) mdl
             False -> handleError forbidden


------------------------------------------------------------------------------
-- | Builder for WebSockets message containing JSON describing
-- creation or deletion of model instance.
modelMessage :: B.ByteString
             -> (ModelName
                 -> CRUD.InstanceId
                 -> Network.WebSockets.Message p)
modelMessage event = \model id ->
    let
        response :: [(B.ByteString, B.ByteString)]
        response = [("event", event),
                    ("id", id),
                    ("model", model)]
    in
      DataMessage $ Text $ A.encode $ M.fromList response


-- | Model instance creation message.
creationMessage :: ModelName
                -> CRUD.InstanceId
                -> Network.WebSockets.Message p
creationMessage = modelMessage "create"


-- | Model instance deletion message.
deletionMessage :: ModelName
                -> CRUD.InstanceId
                -> Network.WebSockets.Message p
deletionMessage = modelMessage "delete"


------------------------------------------------------------------------------
-- | Encode Redis HGETALL reply to B.ByteString with JSON.
commitToJson :: Commit -> LB.ByteString
commitToJson = A.encode


------------------------------------------------------------------------------
applyHooks :: ModelName -> Commit -> Handler b (Redson b) Commit
applyHooks mname commit = do
  hs <- gets hookMap
  case M.lookup mname hs of
    Nothing -> return commit
    Just h  ->
      let actions = M.intersectionWith (map . flip ($)) commit h
      in  apply' commit actions
  where
    apply' c = foldM apply'' c . M.elems
    apply''  = foldM (flip ($))


------------------------------------------------------------------------------
-- | Handle instance creation request
--
-- *TODO*: Use readRequestBody
post :: Handler b (Redson b) ()
post = ifTop $ do
  withCheckSecurity $ \au mdl -> do
    -- Parse request body to list of pairs
    r <- jsonToCommit <$> getRequestBody
    case r of
      Nothing -> handleError serverError
      Just commit -> do
        when (not $ checkWrite au mdl commit) $
             handleError forbidden

        mname <- getModelName
        let commit' = maybe commit (M.union commit . defaults) mdl 
        commit'' <- applyHooks mname commit'

        ixList <- gets
            $ fromMaybe [] . M.lookup mname . ixByModel . indexMap
        newId <- runRedisDB database $ do
           Right newId <- CRUD.create mname commit''
           let fullId = B.concat [mname, ":", newId]
           forM_ ixList $ \ix@(IndexConfig{..}) ->
              case ix'type of
                Ix.Exact -> Ix1.create ix fullId commit''
                _ -> return ()
           return newId

        ps <- gets events
        liftIO $ PS.publish ps $ creationMessage mname newId

        -- http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.5:
        --
        -- the response SHOULD be 201 (Created) and contain an entity which
        -- describes the status of the request and refers to the new
        -- resource
        modifyResponse $ setContentType "application/json" . setResponseCode 201
        -- Tell client new instance id in response JSON.
        writeLBS $ A.encode $ M.insert "id" newId commit''


------------------------------------------------------------------------------
-- | Read instance from Redis.
get' :: Handler b (Redson b) ()
get' = ifTop $ do
  withCheckSecurity $ \au mdl -> do
    (mname, id) <- getInstanceKey

    Right r <- runRedisDB database $ CRUD.read mname id

    when (M.null r) $
         handleError notFound

    modifyResponse $ setContentType "application/json"
    writeLBS $ commitToJson $ filterUnreadable au mdl r


------------------------------------------------------------------------------
-- | Handle PUT request for existing instance in Redis.
--
-- TODO Report 201 if could create new instance.
-- (http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.6)
put :: Handler b (Redson b) ()
put = ifTop $ do
  withCheckSecurity $ \au mdl -> do
    -- Parse request body to list of pairs
    r <- jsonToCommit <$> getRequestBody
    case r of
      Nothing -> handleError serverError
      Just commit -> do
        when (not $ checkWrite au mdl commit) $
             handleError forbidden

        id <- getInstanceId
        mname <- getModelName
        commit' <- applyHooks mname commit
        let fullId = B.concat [mname, ":", id]

        ixList <- gets
            $ fromMaybe [] . M.lookup mname . ixByModel . indexMap
        runRedisDB database $ do
           forM_ ixList $ \ix@(IndexConfig{..}) ->
              case ix'type of
                Ix.Exact -> Ix1.update ix fullId commit'
                _ -> return ()
           Right _ <- CRUD.update mname id commit'
           return ()
        modifyResponse $ setContentType "application/json"
        writeLBS $ A.encode $ M.differenceWith
          (\a b -> if a == b then Nothing else Just a)
          commit' commit


------------------------------------------------------------------------------
-- | Delete instance from Redis (including timeline).
delete :: Handler b (Redson b) ()
delete = ifTop $ do
  withCheckSecurity $ \_ mdl -> do
    mname <- getModelName
    id <- getInstanceId

    let key = CRUD.instanceKey mname id

    r <- runRedisDB database $ do
      -- http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.7
      --
      -- A successful response SHOULD be 200 (OK) if the response includes
      -- an entity describing the status
      Right r <- hgetall key
      return r

    when (null r) $
         handleError notFound

    runRedisDB database $ CRUD.delete mname id

    modifyResponse $ setContentType "application/json"
    writeLBS (commitToJson (M.fromList r))

    ps <- gets events
    liftIO $ PS.publish ps $ deletionMessage mname id


------------------------------------------------------------------------------
-- | Serve list of 10 latest instances stored in Redis.
--
-- *TODO*: Adjustable item limit.
timeline :: Handler b (Redson b) ()
timeline = ifTop $ do
  withCheckSecurity $ \_ _ -> do
    mname <- getModelName

    r <- runRedisDB database $ do
      Right r <- lrange (CRUD.modelTimeline mname) 0 9
      return r

    modifyResponse $ setContentType "application/json"
    writeLBS (enc' r)
      where
          enc' :: [B.ByteString] -> LB.ByteString
          enc' r = A.encode r


------------------------------------------------------------------------------
-- | WebSockets handler which pushes instance creation/deletion events
-- to client.
--
-- TODO: Check for login?
modelEvents :: Handler b (Redson b) ()
modelEvents = ifTop $ do
  ps <- gets events
  liftSnap $ runWebSocketsSnap (\r -> do
                                  acceptRequest r
                                  PS.subscribe ps)


------------------------------------------------------------------------------
-- | Serve JSON metamodel with respect to current user and field
-- permissions.
--
-- TODO: Cache this wrt user permissions cache.
metamodel :: Handler b (Redson b) ()
metamodel = ifTop $ do
  withCheckSecurity $ \au mdl -> do
    case mdl of
      Nothing -> handleError notFound
      Just m -> do
        modifyResponse $ setContentType "application/json"
        writeLBS (A.encode $ stripModel au m)


------------------------------------------------------------------------------
-- | Serve JSON array of readable models to user. Every array element
-- is an object with fields "name" and "title". In transparent mode,
-- serve all models.
--
-- TODO: Cache this.
listModels :: Handler b (Redson b) ()
listModels = ifTop $ do
  au <- withAuth currentUser
  trs <- gets transparent
  readables <- case trs of
    True -> gets (M.toList . models)
    False ->
      case au of
        -- Won't get to serving [] anyways.
        Nothing -> handleError unauthorized >> return []
        -- Leave only readable models.
        Just user ->
            gets (filter (elem GET
                          . getModelPermissions (Right user) . snd)
                  . M.toList . models)
  modifyResponse $ setContentType "application/json"
  writeLBS (A.encode $
             map (\(n, m) -> M.fromList $
                             [("name"::B.ByteString, n),
                              ("title", title m)])
             readables)


-----------------------------------------------------------------------------
-- | Serve model instances which have index values containing supplied
-- search parameters.
--
-- Currently not available in transparent mode.
search :: Handler b (Redson b) ()
search = ifTop $ withCheckSecurity $ \_ mdl -> do
  case mdl of
    Nothing -> handleError notFound
    Just m -> do
      let mname = modelName m
      Just ixName <- getParam "ix"
      Just query  <- getParam "q"
      fields      <- getParam "fields"
      Just ix     <- gets $ M.lookup ixName . ixByName . indexMap
      res <- runRedisDB database $ Ix1.read ix query
      modifyResponse $ setContentType "application/json"
      case fields of
        Nothing -> writeLBS $ A.encode res
        Just fs -> do
          res' <- runRedisDB database $ forM res $ hgetFields $ B8.split ',' fs
          writeLBS $ A.encode res'
        -- FIXME: take, drop

-- FIXME: error handling
hgetFields :: [FieldName] -> CRUD.InstanceId -> Redis [FieldValue]
hgetFields fs objId
  = either (error . show) (map (fromMaybe ""))
  <$> hmget objId fs


-----------------------------------------------------------------------------
-- | CRUD routes for models.
routes :: [(B.ByteString, Handler b (Redson b) ())]
routes = [ (":model/timeline", method GET timeline)
         , (":model/events", modelEvents)
         , (":model/model", method GET metamodel)
         , ("_models", method GET listModels)
         , (":model", method POST post)
         , (":model/:id", method GET get')
         , (":model/:id", method PUT put)
         , (":model/:id", method DELETE delete)
         , (":model/search/", method GET search)
         ]


------------------------------------------------------------------------------
-- | Initialize Redson. AuthManager from parent snaplet is required.
--
-- Connect to Redis, read configuration and set routes.
--
-- > appInit :: SnapletInit MyApp MyApp
-- > appInit = makeSnaplet "app" "App with Redson" Nothing $
-- >           do
-- >             r <- nestSnaplet "_" redson $ redsonInit auth
-- >             s <- nestSnaplet "session" session $ initCookieSessionManager
-- >                                                  sesKey "_session" sessionTimeout
-- >             a <- nestSnaplet "auth" auth $ initJsonFileAuthManager defAuthSettings
-- >             return $ MyApp r s a
redsonInit:: Lens b (Snaplet (AuthManager b))
           -> SnapletInit b (Redson b)
redsonInit = (`redsonInitWithHooks` M.empty)

redsonInitWithHooks :: Lens b (Snaplet (AuthManager b))
           -> HookMap b
           -> SnapletInit b (Redson b)
redsonInitWithHooks topAuth hooks = makeSnaplet
                     "redson"
                     "CRUD for JSON data with Redis storage"
                     Nothing $
          do
            r <- nestSnaplet "db" database $ redisDBInit defaultConnectInfo
            p <- liftIO PS.newPubSub

            cfg <- getSnapletUserConfig
            ixPath <- liftIO $
                      lookupDefault "resources/indices.json"
                                    cfg "indices-config-file"

            ixConfig <- liftIO $ Ix.readConfig ixPath

            mdlDir <- liftIO $
                      lookupDefault "resources/models/"
                                    cfg "models-directory"

            transp <- liftIO $
                      lookupDefault False
                                    cfg "transparent-mode"

            grpDef <- liftIO $
                      lookupDefault "resources/field-groups.json"
                                    cfg "field-groups-file"

            mdls <- liftIO $ loadModels mdlDir grpDef
            addRoutes routes
            return $ Redson r topAuth p mdls transp ixConfig hooks
