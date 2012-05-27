{-# LANGUAGE TemplateHaskell #-}

module Snap.Snaplet.Redson.Snapless.Index.Config
  where


import Data.Aeson as Aeson
import Data.Aeson.TH
import Data.Attoparsec.ByteString.Lazy (Result(..))
import qualified Data.Attoparsec.ByteString.Lazy as Atto

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import qualified Data.Map as M


type ModelName = B.ByteString
type FieldName = B.ByteString
type FieldValue = B.ByteString

data IndexType = Exact | Substring | Range
data IndexConfig = IndexConfig
  {ix'type  :: IndexType
  ,ix'name  :: B.ByteString
  ,ix'from  :: [FieldName] -- FIXME: check if field really exists in model
  ,ix'to    :: FieldName
  }

$(deriveFromJSON id ''IndexType)
$(deriveFromJSON (drop 3) ''IndexConfig)

type IndexMap = M.Map ModelName [IndexConfig]

readConfig :: FilePath -> IO IndexMap
readConfig jsonFile = do
  txt <- L.readFile jsonFile
  case Atto.parse Aeson.json' txt of
    Done _ jsn -> case Aeson.fromJSON jsn of
      Success o -> return o
      Error err -> fail $ "Could not parse index config "
                           ++ "\n\t" ++ err
    err -> fail $ "Could not parse index config "
                   ++ "\n\t" ++ show err

