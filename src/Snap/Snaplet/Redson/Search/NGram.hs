{-# LANGUAGE OverloadedStrings #-}

module Snap.Snaplet.Redson.Search.NGram
    ( create
    , update
    , delete
    , getRecord
    , modifyIndex
    ) where
import Control.Arrow (second)
import Control.Monad.IO.Class
import Data.Maybe (mapMaybe, isJust, fromJust)

import qualified Data.Map as M

import Control.Concurrent.MVar
import Database.Redis

import qualified Data.ByteString as B
import qualified Text.Search.NGram as NG
import qualified Data.Text as T
import qualified Data.Text.Encoding as E

import Snap.Snaplet.Redson.Snapless.Index
import Snap.Snaplet.Redson.Snapless.Metamodel
import Snap.Snaplet.Redson.Snapless.CRUD (InstanceId)

-- | Decode from UTF-8
decodeS :: B.ByteString -> String
decodeS = T.unpack . E.decodeUtf8

-- | Index for one record
record :: IndicesTable -> Commit -> NG.IndexValue
record fs c = concatMap (NG.ngram 3) values where
    values = map decodeS $ mapMaybe (`M.lookup` c) (M.keys fs)

-- | Create index for new record and update indices with it
create
    :: InstanceId
    -> IndicesTable
    -> Commit
    -> NG.IndexAction InstanceId
create i fs c = NG.insert (record fs) c i

-- | Update existing record
update
    :: InstanceId
    -> IndicesTable
    -> Commit
    -> Commit
    -> NG.IndexAction InstanceId
update i fs c c' = NG.update (record fs) v c' i where
    v = M.intersectionWith const c c' -- remove only overlapped keys

-- | Remove record
delete
    :: InstanceId
    -> IndicesTable
    -> Commit
    -> NG.IndexAction InstanceId
delete i fs c = NG.remove (record fs) c i

-- | Key for record
recordKey :: ModelName -> InstanceId -> B.ByteString
recordKey mname i = B.concat [mname, ":", i]

-- | Get data by id
getRecord
    :: ModelName
    -> InstanceId
    -> IndicesTable
    -> Redis (Either Reply Commit)
getRecord mname i fs =
    let
        key = recordKey mname i
        fs' = M.keys fs
    in do
        values <- hmget key fs'
        let toCommit v =
                M.fromList
                $ map (second fromJust)
                $ filter (isJust . snd)
                $ (fs' `zip` v)
        return $ fmap toCommit values

-- | Modify index with action
modifyIndex
    :: Maybe (MVar (NG.Index InstanceId))
    -> (NG.IndexAction InstanceId)
    -> Redis ()
modifyIndex (Just mvar) act = liftIO $ modifyMVar_ mvar (return . NG.apply act)
modifyIndex Nothing _ = return ()

