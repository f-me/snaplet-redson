{-# LANGUAGE OverloadedStrings #-}

{-|

Snap-agnostic low-level CRUD operations. No model definitions are used
on this level. Instead, objects must be 

This module may be used for batch uploading of database data.

-}
module Snap.Snaplet.Redson.Snapless.CRUD
    ( -- * CRUD operations
      create
    , read
    , update
    , delete
    -- * Redis helpers
    , InstanceId
    , instanceKey
    , modelIndex
    , modelTimeline
    , collateValue
    , onlyFields
    )
where

import Prelude hiding (id, read)

import Control.Arrow (second)
import Control.Monad.State
import Data.Functor
import Data.Maybe

import Data.Char
import qualified Data.ByteString.Char8 as B
import qualified Data.Text as T
import qualified Data.Text.Encoding as E
import qualified Data.ByteString.UTF8 as BU (fromString, toString)
import qualified Data.Map as M

import Database.Redis

import Snap.Snaplet.Redson.Snapless.Index
import Snap.Snaplet.Redson.Snapless.Metamodel
import Snap.Snaplet.Redson.Util

type InstanceId = B.ByteString

------------------------------------------------------------------------------
-- | Build Redis key given model name and instance id.
instanceKey :: ModelName -> InstanceId -> B.ByteString
instanceKey model id = B.concat [model, ":", id]


------------------------------------------------------------------------------
-- | Get Redis key which stores id counter for model.
modelIdKey :: ModelName -> B.ByteString
modelIdKey model = B.concat ["global:", model, ":id"]


------------------------------------------------------------------------------
-- | Get Redis key which stores timeline for model.
modelTimeline :: ModelName -> B.ByteString
modelTimeline model = B.concat ["global:", model, ":timeline"]


------------------------------------------------------------------------------
-- | Build Redis key for field index of model.
modelIndex :: ModelName
           -> B.ByteString -- ^ Field name
           -> B.ByteString -- ^ Field value
           -> B.ByteString
modelIndex model field value = B.concat [model, ":", field, ":", value]

------------------------------------------------------------------------------
-- | Strip value of punctuation, spaces, convert all to lowercase.
collateValue :: FieldValue -> FieldValue
collateValue = E.encodeUtf8 . T.toLower .
          (T.filter (\c -> (not (isSpace c || isPunctuation c)))) .
          E.decodeUtf8


------------------------------------------------------------------------------
-- | Perform provided action for every indexed field in commit.
--
-- Action is called with index field name and its value in commit.
forIndices :: Commit
           -> IndicesTable
           -> (FieldName -> FieldValue -> IndexType -> Redis ())
           -> Redis ()
forIndices commit indsTbl action =
    mapM_ (\(i, o) -> case (M.lookup i commit) of
                       Just v -> action i v o
                       Nothing -> return ())
          (M.toList indsTbl)

------------------------------------------------------------------------------
-- | Create reverse indices for new commit.
createIndices :: ModelName
              -> InstanceId
              -> Commit
              -> IndicesTable
              -> Redis ()
createIndices mname id commit findices =
    forIndices commit findices $
                   \i rawVal typ -> 
                       let 
                           v = collateValue rawVal
                           mdv = maybeRead $ BU.toString v
                       in
                         when (v /= "") $
                           case typ of
                             Sorted ->
                               case mdv of
                                 Just dv ->
                                   void $ zadd (instanceKey mname i) [(dv, id)]
                                 Nothing -> return ()
                             Reverse ->
                               void $ sadd (modelIndex mname i v) [id]
                             FullText ->
                               return ()

------------------------------------------------------------------------------
-- | Remove indices previously created by commit (should contain all
-- indexed fields only).
deleteIndices :: ModelName
              -> InstanceId                           -- ^ Instance id.
              -> [(FieldName, FieldValue, IndexType)] -- ^ Commit with old
                                                     -- indexed values (zipped
                                                     -- from HMGET).
              -> Redis ()
deleteIndices mname id commit =
    mapM_ (\(i, v, t) -> 
            case t of 
              Sorted -> void $ zrem (instanceKey mname i) [id]
              Reverse -> void $ srem (modelIndex mname i v) [id]
              FullText -> return ())
          commit


------------------------------------------------------------------------------
-- | Get old values of index fields stored under key.
getOldIndices :: B.ByteString -> [FieldName] -> Redis [Maybe B.ByteString]
getOldIndices key findices = do
  reply <- hmget key findices
  return $ case reply of
             Left _ -> []
             Right l -> l


------------------------------------------------------------------------------
-- | Extract values of named fields from commit.
onlyFields :: Commit -> [FieldName] -> [Maybe FieldValue]
onlyFields commit names = map (flip M.lookup commit) names


------------------------------------------------------------------------------
-- | Create new instance in Redis and indices for it.
--
-- Bump model id counter and update timeline, return new instance id.
--
-- TODO: Support pubsub from here
create :: ModelName           -- ^ Model name
       -> Commit              -- ^ Key-values of instance data
       -> IndicesTable
       -> Redis (Either Reply InstanceId)
create mname commit indsTbl = do
  -- Take id from global:model:id
  Right n <- incr $ modelIdKey mname
  newId <- return $ (B.pack . show) n

  -- Save new instance
  _ <- hmset (instanceKey mname newId) (M.toList commit)
  _ <- lpush (modelTimeline mname) [newId]

  createIndices mname newId commit indsTbl
  return (Right newId)


------------------------------------------------------------------------------
-- | Read existing instance from Redis.
read :: ModelName
     -> InstanceId
     -> Redis (Either Reply Commit)
read mname id = (fmap M.fromList) <$> hgetall key
    where
      key = instanceKey mname id

------------------------------------------------------------------------------
-- | Modify existing instance in Redis, updating indices.
--
-- TODO: Handle non-existing instance as error here?
update :: ModelName
       -> InstanceId
       -> Commit
       -> IndicesTable
       -> Redis (Either Reply ())
update mname id commit indsTbl =
  let
      key = instanceKey mname id
      unpacked = M.toList commit
      newFields = map fst unpacked
      inds = M.toList indsTbl
  in do
    old <- getOldIndices key $ map fst inds
    hmset key unpacked

    deleteIndices mname id $
                  zipWith (\(a, b) c -> (a, c, b))
                      (filter (flip elem newFields . fst) inds)
                      (catMaybes old)
    createIndices mname id commit indsTbl

    return (Right ())

------------------------------------------------------------------------------
-- | Remove existing instance in Redis, cleaning up old indices.
--
-- Does not check if instance exists.
delete :: ModelName
       -> InstanceId
       -> IndicesTable
       -> Redis (Either Reply ())
delete mname id indsTbl =
    let
        key = instanceKey mname id
        inds = M.toList indsTbl
    in do
      old <- getOldIndices key $ map fst inds
      lrem (modelTimeline mname) 1 id >> del [key]
      deleteIndices mname id (zipWith (\(a, b) c -> (a, c, b)) inds (catMaybes old))
      return (Right ())
