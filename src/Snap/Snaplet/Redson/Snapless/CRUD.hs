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
    , modelTimeline
    , collate
    , onlyFields
    )

where

import Prelude hiding (id, read)

import Data.Functor

import Data.Char
import qualified Data.ByteString.Char8 as B
import qualified Data.Text as T
import qualified Data.Text.Encoding as E
import qualified Data.Map as M

import Database.Redis

import Snap.Snaplet.Redson.Snapless.Metamodel


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
-- | Strip value of punctuation, spaces, convert all to lowercase.
collate :: FieldValue -> FieldValue
collate = E.encodeUtf8 . T.toLower .
          (T.filter (\c -> (not (isSpace c || isPunctuation c)))) .
          E.decodeUtf8


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
       -> Redis (Either Reply InstanceId)
create mname commit = do
  -- Take id from global:model:id
  Right n <- incr $ modelIdKey mname
  newId <- return $ (B.pack . show) n

  -- Save new instance
  Right _ <- hmset (instanceKey mname newId) (M.toList commit)
  Right _ <- lpush (modelTimeline mname) [newId]

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
       -> Redis (Either Reply ())
update mname id commit =
  let
      key = instanceKey mname id
      unpacked = M.toList commit
  in  fmap (const ()) <$> hmset key unpacked

------------------------------------------------------------------------------
-- | Remove existing instance in Redis, cleaning up old indices.
--
-- Does not check if instance exists.
delete :: ModelName
       -> InstanceId
       -> Redis (Either Reply ())
delete mname id =
    let
        key = instanceKey mname id
    in do
      lrem (modelTimeline mname) 1 id >> del [key]
      return (Right ())
