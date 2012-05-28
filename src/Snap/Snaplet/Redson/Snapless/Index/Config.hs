{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Snap.Snaplet.Redson.Snapless.Index.Config
  where


import Data.Aeson as Aeson
import Data.Aeson.TH
import Data.Attoparsec.ByteString.Lazy (Result(..))
import qualified Data.Attoparsec.ByteString.Lazy as Atto

import Data.List (foldl')
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Map (Map)
import qualified Data.Map as M


type ModelName = B.ByteString
type FieldName = B.ByteString
type FieldValue = B.ByteString
type IndexName = B.ByteString

data IndexType = Exact | Substring | Range

instance FromJSON IndexType where
  parseJSON (String s) = return $ case s of
    "exact"     -> Exact
    "substring" -> Substring
    "range"     -> Range
    _ -> error "Invalid IndexType"
  parseJSON _ = error "Could not parse IndexType"

data IndexConfig = IndexConfig
  {ix'type  :: IndexType
  ,ix'from  :: [FieldName]
  ,ix'to    :: FieldName
  ,ix'model :: ModelName
  ,ix'name  :: IndexName
  }
data InternalIndexConfig = InternalIndexConfig
  {iix'type  :: IndexType
  ,iix'from  :: [FieldName]
  ,iix'to    :: FieldName
  ,iix'model :: ModelName
  }

$(deriveFromJSON (drop 4) ''InternalIndexConfig)

data IndexMap = IndexMap
  {ixByName  :: Map IndexName IndexConfig
  ,ixByModel :: Map ModelName [IndexConfig]
  }

readConfig :: FilePath -> IO IndexMap
readConfig jsonFile = do
  txt <- L.readFile jsonFile
  case Atto.parse Aeson.json' txt of
    Done _ jsn -> case Aeson.fromJSON jsn of
      Success o -> return $ mkIndexMap o
      Error err -> fail $ "Could not parse index config "
                           ++ "\n\t" ++ err
    err -> fail $ "Could not parse index config "
                   ++ "\n\t" ++ show err

mkIndexMap :: Map IndexName InternalIndexConfig -> IndexMap
mkIndexMap = foldl' go empty . M.toList
  where
    empty = IndexMap M.empty M.empty
    go imap (ixName, InternalIndexConfig{..}) = imap
      {ixByName  = M.insert ixName ix' $ ixByName imap
      ,ixByModel = M.insertWith' (++) iix'model [ix'] $ ixByModel imap
      }
      where
        ix' = IndexConfig iix'type iix'from iix'to iix'model ixName
