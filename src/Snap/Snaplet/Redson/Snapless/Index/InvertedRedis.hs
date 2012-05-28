{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}


module Snap.Snaplet.Redson.Snapless.Index.InvertedRedis
  (create
  ,update
  ,read
  ,InvertedRedisException(..)
  ) where

import Prelude hiding (read)
import Control.Exception
import Data.Functor ((<$>))

import Data.Typeable
import Data.Maybe
import Data.Either
import Data.List (nub,(\\))
import Data.Map (Map)
import qualified Data.Map as M
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B

import Database.Redis
import Snap.Snaplet.Redson.Snapless.Index.Config

-- obj in read(ix, val) <=> exist f in ix'from . obj[f] == val

-- FIXME: add more info (like `val` or `commit`)
data InvertedRedisException
  = CreateException String
  | UpdateException String
  | ReadException String
  deriving (Show, Typeable)

instance Exception InvertedRedisException

type InstanceId = ByteString
type Commit = Map FieldName FieldValue

create :: IndexConfig -> InstanceId -> Commit -> Redis ()
create (IndexConfig{..}) objId commit
  = case M.lookup ix'to commit of
    Nothing -> return ()
    Just to -> do
      let vals = nub $ M.elems $ spliceMap ix'from commit
      let mkKey v = B.concat [ix'name, ":", v]
      res <- sequence [sadd (mkKey val) [to] | val <- vals]
      case lefts res of
        [] -> return ()
        errs -> throw $ CreateException $ show errs


-- FIXME: throw exceptions
update :: IndexConfig -> InstanceId -> Commit -> Redis ()
update (IndexConfig{..}) objId commit = do
  Right mTo <- hget objId ix'to
  let to = fromMaybe
        (error $ "InvertedRedis.update: no such field " ++ show ix'to)
        mTo
  Right from <- hmgetMap objId ix'from
  let from' = M.union (spliceMap ix'from commit) from
  let vals  = nub $ M.elems from
  let vals' = nub $ M.elems from'
  let (to', remVals, addVals) = case M.lookup ix'to commit of
        Just to' -> (to', vals, vals')
        Nothing  -> (to, vals \\ vals', vals' \\ vals)
  let mkKey v = B.concat [ix'name, ":", v]
  res1 <- sequence [srem (mkKey val) [to]  | val <- remVals]
  res2 <- sequence [sadd (mkKey val) [to'] | val <- addVals]
  case lefts $ res1 ++ res2 of
    [] -> return ()
    errs -> throw $ UpdateException $ show errs


----------------------------------------------------------------------
read :: IndexConfig -> FieldValue -> Redis [FieldValue]
read (IndexConfig{..}) val = do
  let key = B.concat [ix'name, ":", val]
  res <- smembers key
  case res of
    Left err -> throw $ ReadException $ show err
    Right r  -> return r


mkMap :: Ord k => [k] -> [Maybe a] -> Map k a
mkMap ks = M.fromList . catMaybes . zipWith (\k v -> (k,) <$> v) ks

spliceMap :: Ord k => [k] -> Map k a -> Map k a
spliceMap ks m = mkMap ks $ map (`M.lookup` m) ks

hmgetMap :: InstanceId -> [FieldName] -> Redis (Either Reply Commit)
hmgetMap objId fields = fmap (mkMap fields) <$> hmget objId fields
