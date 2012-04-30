{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Snap.Snaplet.Redson.Snapless.Index

where

import Control.Applicative
import qualified Data.Map as M

import Data.Aeson
import qualified Data.ByteString as B

data FieldIndex =
    FieldIndex { collate   :: Bool
               , sorted    :: Bool
               }
      deriving Show

type IndicesTable = M.Map B.ByteString FieldIndex

instance FromJSON (B.ByteString, FieldIndex) where
    parseJSON (Object v) =
         (,) <$>
           v .: "name" <*>
           (FieldIndex   <$>
             v .:? "collate" .!= False <*>
             v .:? "sorted"  .!= False)
    parseJSON _          = error "Could not parse field index properties"

instance ToJSON (B.ByteString, FieldIndex) where
    toJSON (name, fi) = object
      [ "collate"   .= collate fi
      , "name"      .= name 
      , "sorted"    .= sorted fi
      ]

