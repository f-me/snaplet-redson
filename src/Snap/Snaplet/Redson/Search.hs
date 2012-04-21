{-# LANGUAGE OverloadedStrings #-}

{-|

Ad-hoc Redis search backed by field indices.

-}

module Snap.Snaplet.Redson.Search
    ( PatternFunction
    , SearchTerm
    , prefixMatch
    , substringMatch
    , redisSearch
    , redisRangeSearch
    )

where

import qualified Data.ByteString as B

import Database.Redis

import Snap.Snaplet.Redson.Snapless.CRUD
import Snap.Snaplet.Redson.Snapless.Metamodel

------------------------------------------------------------------------------
-- | A function which fill build Redis key pattern for certain way of
-- matching values of index field.
type PatternFunction = ModelName -> SearchTerm -> B.ByteString


-----------------------------------------------------------------------------
-- | Describe that field should somehow match the provided value.
type SearchTerm = (FieldName, FieldValue)


------------------------------------------------------------------------------
-- | Match prefixes.
prefixMatch :: PatternFunction
prefixMatch model (field, value) = B.append (modelIndex model field value) "*"


------------------------------------------------------------------------------
-- | Match substrings.
substringMatch :: PatternFunction
substringMatch model (field, value) =
    B.concat [model, ":", field, ":*", value, "*"]


------------------------------------------------------------------------------
-- | Redis action which returns list of matching instance id's for
-- every search term.
redisSearch :: Model
            -- ^ Model instances of which are being searched
            -> [SearchTerm]
            -- ^ List of requested index field values
            -> PatternFunction
            -- ^ How to build pattern for matching keys
            -> Redis [[InstanceId]]
redisSearch model searchTerms patFunction =
    let
        mname = modelName model
        -- Get list of ids which match single search term
        getTermIds pattern = do
          Right sets <- keys pattern
          case sets of
            -- Do not attempt sunion with no arguments.
            [] -> return []
            _ -> do
              -- TODO Maybe use sunionstore and perform further
              -- operations on Redis as well.
              Right ids <- sunion sets
              return ids
     in
       -- Try to get search results for every index field
       mapM (getTermIds . (patFunction mname)) searchTerms

------------------------------------------------------------------------------
-- | Redis action which returns list of matching instance id's for
-- terms lied in range
redisRangeSearch :: Model
            -- ^ Model instances of which are being searched
            -> FieldName
            -- ^ Searching index name
            -> Double
            -- ^ Lower bound of interval
            -> Double
            -- ^ Upper bound of interval
            -> Redis [InstanceId]
redisRangeSearch model name lower upper =
    do
        let mname = modelName model

        Right sets <- zrangebyscore (modelIndexSorted mname name) lower upper

        return sets

