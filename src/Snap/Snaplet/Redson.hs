{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}

{-|

CRUD for JSON data with Redis storage.

Can be used as Backbone.sync backend.

-}

module Snap.Snaplet.Redson (Redson
                           , redsonInit)
where

import Prelude hiding (concat, FilePath)

import Control.Monad.State
import Control.Monad.Trans
import Data.Functor

import Data.Aeson as A

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB (ByteString, readFile)
import qualified Data.ByteString.UTF8 as BU (fromString)

import Data.Configurator

import Data.Lens.Common
import Data.Lens.Template

import qualified Data.Map as M

import Data.Maybe

import Snap.Core
import Snap.Snaplet
import Snap.Snaplet.Auth
import Snap.Snaplet.RedisDB
import Snap.Util.FileServe

import Network.WebSockets
import Network.WebSockets.Snap
import qualified Network.WebSockets.Util.PubSub as PS

import Database.Redis hiding (auth)

import System.EasyFile

import Snap.Snaplet.Redson.CRUD
import Snap.Snaplet.Redson.Metamodel
import Snap.Snaplet.Redson.Util

------------------------------------------------------------------------------
-- | Redson snaplet state type.
--
-- *TODO*: Use HashMap to store models?
data Redson b = Redson
             { _database :: Snaplet RedisDB
             , auth :: Lens b (Snaplet (AuthManager b))
             , events :: PS.PubSub Hybi10
             , models :: M.Map ModelName Model
             , transparent :: Bool
             -- ^ Operate in transparent mode (not security checks).
             }

makeLens ''Redson


------------------------------------------------------------------------------
-- | Extract model name from request path parameter.
getModelName:: MonadSnap m => m B.ByteString
getModelName = fromParam "model"


------------------------------------------------------------------------------
-- | Extract model instance id from request parameter.
getModelId:: MonadSnap m => m B.ByteString
getModelId = fromParam "id"



------------------------------------------------------------------------------
-- | Extract model instance Redis key from request parameters.
getInstanceKey :: MonadSnap m => m B.ByteString
getInstanceKey = liftM2 instanceKey getModelName getModelId

------------------------------------------------------------------------------
-- | Try to get Model for current request.
getModel :: (MonadSnap m, MonadState (Redson b) m) => m (Maybe Model)
getModel = liftM2 M.lookup getModelName (gets models)


------------------------------------------------------------------------------
-- | Perform action with AuthManager.
withAuth :: (MonadState (Redson b1) (m b1 v), MonadSnaplet m) =>
            m b1 (AuthManager b1) b -> m b1 v b
withAuth action = do
  am <- gets auth
  return =<< withTop am action


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
        (Nothing, _) -> unauthorized
        (_, Nothing) -> forbidden
        (Just user, Just model) ->
           case (elem m $ getModelPermissions (Right user) model) of
             True -> action (Right user) mdl
             False -> forbidden


------------------------------------------------------------------------------
-- | Builder for WebSockets message containing JSON describing
-- creation or deletion of model instance.
modelMessage :: B.ByteString
             -> (B.ByteString
                 -> B.ByteString
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
creationMessage :: B.ByteString -- ^ Model name
                -> B.ByteString -- ^ Instance ID
                -> Network.WebSockets.Message p
creationMessage = modelMessage "create"

-- | Model instance deletion message.
deletionMessage :: B.ByteString -- ^ Model name
                -> B.ByteString -- ^ Instance ID
                -> Network.WebSockets.Message p
deletionMessage = modelMessage "delete"


------------------------------------------------------------------------------
-- | Encode Redis HGETALL reply to B.ByteString with JSON.
--
-- Note using explicit B.ByteString type over BS s as suggested by
-- redis because BS s doesn't imply ToJSON s.
hgetallToJson :: Commit -> LB.ByteString
hgetallToJson r = A.encode $ M.fromList r


------------------------------------------------------------------------------
-- | Decode B.ByteString with JSON to list of hash keys & values for
-- Redis HMSET
--
-- Return Nothing if parsing failed.
jsonToHmset :: LB.ByteString -> Maybe Commit
jsonToHmset s =
    let
        j = A.decode s
    in
      case j of
        Nothing -> Nothing
        Just m ->
             -- Omit fields with null values and "id" key
            Just (map (\(k, v) -> (k, fromJust v)) $
                  filter (\(k, v) -> (isJust v && k /= "id")) $
                  M.toList m)


------------------------------------------------------------------------------
-- | Handle instance creation request
--
-- *TODO*: Use readRequestBody
post :: Handler b (Redson b) ()
post = ifTop $ do
  withCheckSecurity $ \au (Just mdl) -> do
    -- Parse request body to list of pairs
    r <- jsonToHmset <$> getRequestBody
    case r of
      Nothing -> serverError
      Just commit -> do
        when (not $ checkWrite au mdl commit)
             forbidden

        name <- getModelName
        newId <- runRedisDB database $ do
          create name commit

        ps <- gets events
        liftIO $ PS.publish ps $ creationMessage name newId

        -- http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.5:
        --
        -- the response SHOULD be 201 (Created) and contain an entity which
        -- describes the status of the request and refers to the new
        -- resource
        modifyResponse $ (setContentType "application/json" . setResponseCode 201)
        -- Tell client new instance id in response JSON.
        writeLBS $ A.encode $ M.fromList $ ("id", newId):commit
        return ()


------------------------------------------------------------------------------
-- | Read instance from Redis.
read' :: Handler b (Redson b) ()
read' = ifTop $ do
  -- Pass to index page handler (Snap routing bug workaround)
  id <- fromParam "id"
  when (B.null id)
       pass

  withCheckSecurity $ \au (Just mdl) -> do
    key <- getInstanceKey
    r <- runRedisDB database $ do
      Right r <- hgetall key
      return r

    when (null r)
         notFound

    modifyResponse $ setContentType "application/json"
    writeLBS $ hgetallToJson (filterUnreadable au mdl r)
    return ()


------------------------------------------------------------------------------
-- | Update existing instance in Redis.
--
-- *TODO* Report 201 if previously existed
update :: Handler b (Redson b) ()
update = ifTop $ do
  withCheckSecurity $ \au (Just mdl) -> do
    -- Parse request body to list of pairs
    r <- jsonToHmset <$> getRequestBody
    case r of
      Nothing -> serverError
      Just j -> do
        when (not $ checkWrite au mdl j)
             forbidden

        key <- getInstanceKey
        runRedisDB database $ hmset key j
        modifyResponse $ setResponseCode 204
        return ()


------------------------------------------------------------------------------
-- | Delete instance from Redis (including timeline).
delete :: Handler b (Redson b) ()
delete = ifTop $ do
  withCheckSecurity $ \_ _ -> do
    id <- getModelId
    name <- getModelName
    key <- getInstanceKey

    r <- runRedisDB database $ do
      -- http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html#sec9.7
      --
      -- A successful response SHOULD be 200 (OK) if the response includes
      -- an entity describing the status
      Right r <- hgetall key
      return r

    when (null r)
         notFound

    runRedisDB database $ lrem (modelTimeline name) 1 id >> del [key]

    modifyResponse $ setContentType "application/json"
    writeLBS (hgetallToJson r)

    ps <- gets events
    liftIO $ PS.publish ps $ deletionMessage name id


------------------------------------------------------------------------------
-- | Serve list of 10 latest instances stored in Redis.
--
-- *TODO*: Adjustable item limit.
timeline :: Handler b (Redson b) ()
timeline = ifTop $ do
  withCheckSecurity $ \_ _ -> do
    name <- getModelName

    r <- runRedisDB database $ do
      Right r <- lrange (modelTimeline name) 0 9
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
      Nothing -> notFound
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
        Nothing -> unauthorized >> return []
        -- Leave only readable models.
        Just user ->
            gets (filter (\(n, m) -> elem GET $
                                     getModelPermissions (Right user) m)
                  . M.toList . models)
  modifyResponse $ setContentType "application/json"
  writeLBS (A.encode $ 
             map (\(n, m) -> M.fromList $ 
                             [("name"::B.ByteString, n), 
                              ("title", title m)])
             readables)


-----------------------------------------------------------------------------
-- | CRUD routes for models.
routes :: [(B.ByteString, Handler b (Redson b) ())]
routes = [ (":model/timeline", method GET timeline)
         , (":model/events", modelEvents)
         , (":model/model", method GET metamodel)
         , ("_models", method GET listModels)
         , (":model", method POST post)
         , (":model/:id", method GET read')
         , (":model/:id", method PUT update)
         , (":model/:id", method DELETE delete)
         ]


-- | Build metamodel name from its file path.
pathToModelName :: FilePath -> ModelName
pathToModelName path = BU.fromString $ takeBaseName path


-- | Read all models from directory to a map.
--
-- TODO: Perhaps rely on special directory file which explicitly lists
-- all models.
loadModels :: FilePath -> IO (M.Map ModelName Model)
loadModels dir =
    let
        parseModel filename = do
              j <- LB.readFile filename
              case (A.decode j) of
                Just model -> return model
                Nothing -> error $ "Could not parse " ++ filename
    in
      do
        dirEntries <- getDirectoryContents dir
        -- Leave out non-files
        files <- filterM doesFileExist (map (\f -> dir ++ "/" ++ f) dirEntries)
        models <- mapM parseModel files
        return $ M.fromList $ zip (map pathToModelName files) models


------------------------------------------------------------------------------
-- | Connect to Redis and set routes.
redsonInit :: Lens b (Snaplet (AuthManager b))
           -> SnapletInit b (Redson b)
redsonInit topAuth = makeSnaplet
                     "redson"
                     "CRUD for JSON data with Redis storage"
                     Nothing $
          do
            r <- nestSnaplet "db" database $ redisDBInit defaultConnectInfo
            p <- liftIO PS.newPubSub

            cfg <- getSnapletUserConfig
            mdlDir <- liftIO $
                      lookupDefault "resources/models/"
                                    cfg "models-directory"

            transp <- liftIO $
                      lookupDefault False
                                    cfg "transparent-mode"

            mdls <- liftIO $ loadModels mdlDir
            addRoutes routes
            return $ Redson r topAuth p mdls transp
