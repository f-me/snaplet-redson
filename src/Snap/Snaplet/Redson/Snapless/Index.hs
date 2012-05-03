{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Snap.Snaplet.Redson.Snapless.Index

where

import Control.Applicative
import qualified Data.Map as M

import Data.Aeson
import qualified Data.ByteString as B

data IndexType = Sorted | FullText | Reverse
      deriving (Show, Eq, Ord)

type IndicesTable = M.Map B.ByteString IndexType

instance FromJSON IndexType where
    parseJSON (String s) 
        | s == "sorted" = pure Sorted
        | s == "reverse" = pure Reverse
        | s == "fullText" = pure FullText
        | otherwise = error "Could not parse index type"
    parseJSON _ = error "Could not parse index type"

instance ToJSON IndexType where
    toJSON Sorted = String "sorted"
    toJSON Reverse = String "reverse"
    toJSON FullText = String "fullText"

instance FromJSON (B.ByteString, IndexType) where
    parseJSON (Object v) =
         (,) <$>
           v .: "name" <*>
           v .: "type"
    parseJSON _          = error "Could not parse field index properties"

instance ToJSON (B.ByteString, IndexType) where
    toJSON (name, typ) = object
      [ "name" .= name
      , "type" .= typ
      ]

