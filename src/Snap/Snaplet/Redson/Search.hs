{-# LANGUAGE OverloadedStrings #-}

{-|

Ad-hoc Redis search backed by field indices.

-}

module Snap.Snaplet.Redson.Search
    ( redisReverseSearch
    , redisRangeSearch
    , redisFullTextSearch
    )

where

import Control.Applicative ((<$>))
import Control.Concurrent.MVar
import Control.Monad (void, when, forM, liftM)
import Control.Monad.IO.Class (liftIO)
import Data.Maybe (catMaybes)

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C8

import qualified Data.Text as T
import qualified Data.Text.Encoding as E

import Database.Redis

import Snap.Snaplet.Redson.Snapless.CRUD
import Snap.Snaplet.Redson.Snapless.Metamodel

import qualified Text.Search.NGram as NG

------------------------------------------------------------------------------
-- | Redis action which returns list of matching instance id's for
-- every search term.
redisReverseSearch 
    :: Model -- ^ Model instances of which are being searched
    -> [FieldName] -- ^ Requested index field name
    -> FieldValue -- ^ Requested index field value
    -> Redis [InstanceId]
redisReverseSearch model fNames fValue =
    liftM concat $ mapM search_ fNames
  where
    search_ fName =
      do
        Right sets <- smembers $ modelIndex mname fName fValue
        return sets
    mname = modelName model

------------------------------------------------------------------------------
-- | Redis action which returns list of matching instance id's for
-- terms lied in range
redisRangeSearch :: Model
            -- ^ Model instances of which are being searched
            -> [FieldName]
            -- ^ Searching index name
            -> Double
            -- ^ Lower bound of interval
            -> Double
            -- ^ Upper bound of interval
            -> Redis [InstanceId]
redisRangeSearch model fNames lower upper =
    liftM concat $ mapM search_ fNames
  where
    search_ fName =
      (zrangebyscore (instanceKey mname fName) lower upper 
        >>= (\(Right sets) -> return sets))
    mname = modelName model

-- | Search for term, return list of matching instances
redisFullTextSearch
    :: Model
    -> [FieldName]
    -> B.ByteString
    -> Redis [InstanceId]
redisFullTextSearch mdl fNames term = 
    case ngramIndex mdl of
      Just mVar -> do
        ix <- liftIO $ readMVar mVar
        let
            mName = modelName mdl
            term' = T.unpack $ E.decodeUtf8 term
            matches = map fst $ NG.search ix (NG.ngram 3) term'
            mName' = C8.unpack mName
            modelId i = C8.pack $ mName' ++ ":" ++ C8.unpack i
        preciseMatches <- catMaybes <$> (forM matches $ \m -> do
          Right matched <- hmget (modelId m) fNames
          let matchFields = map (T.unpack . E.decodeUtf8) (catMaybes matched)
          return $ if NG.matchPrecise matchFields term'
            then Just m
            else Nothing)
        return preciseMatches
      Nothing -> return []

