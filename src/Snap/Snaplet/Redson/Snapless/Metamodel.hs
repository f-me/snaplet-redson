{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | Model partial parser which allows to extract field
-- permissions data.

module Snap.Snaplet.Redson.Snapless.Metamodel

where

import Control.Applicative

import Data.Aeson
import qualified Data.ByteString as B

import Data.Lens.Template
import Data.List

import qualified Data.Map as M

type ModelName = B.ByteString

-- | Field name.
type FieldName = B.ByteString

-- | Field value.
type FieldValue = B.ByteString

-- | Name of indexed field and collation flag.
type FieldIndex = (FieldName, Bool)

-- | List of field key-value pairs.
--
-- Suitable for using with 'Database.Redis.hmset'.
type Commit = M.Map FieldName FieldValue


-- | Field permissions property.
data Permissions = Roles [B.ByteString]
                 | Everyone
                 | Nobody
                 deriving Show

-- | Map of field annotations which are transparently handled by
-- server without any logic.
type FieldMeta = M.Map FieldName Value

-- | Form field object.
data Field = Field { name           :: FieldName
                   , fieldType      :: B.ByteString
                   , index          :: Bool
                   , indexCollate   :: Bool
                   , groupName      :: Maybe B.ByteString
                   , meta           :: Maybe FieldMeta
                   , canRead        :: Permissions
                   , canWrite       :: Permissions
                   }
             deriving Show


-- | Model describes fields and permissions.
--
-- Models are built from JSON definitions (using FromJSON instance for
-- Model) with further group splicing ('spliceGroups') and index
-- caching ('cacheIndices').
data Model = Model { modelName      :: ModelName
                   , title          :: B.ByteString
                   , fields         :: [Field]
                   , _canCreateM    :: Permissions
                   , _canReadM      :: Permissions
                   , _canUpdateM    :: Permissions
                   , _canDeleteM    :: Permissions
                   , indices        :: [FieldIndex]
                   -- ^ Cached list of index fields.
                   }
             deriving Show

makeLenses [''Model]

-- | Used when field type is not specified in model description.
defaultFieldType :: B.ByteString
defaultFieldType = "text"


instance FromJSON Model where
    parseJSON (Object v) = Model          <$>
        v .: "name"                       <*>
        v .: "title"                      <*>
        v .: "fields"                     <*>
        v .:? "canCreate" .!= Nobody      <*>
        v .:? "canRead"   .!= Nobody      <*>
        v .:? "canUpdate" .!= Nobody      <*>
        v .:? "canDelete" .!= Nobody      <*>
        pure []
    parseJSON _          = error "Could not parse model description"

instance ToJSON Model where
    toJSON mdl = object
      [ "name"       .= modelName mdl
      , "title"      .= title mdl
      , "fields"     .= fields mdl
      , "indices"    .= indices mdl
      , "canCreate"  .= _canCreateM mdl
      , "canRead"    .= _canReadM mdl
      , "canUpdate"  .= _canUpdateM mdl
      , "canDelete"  .= _canDeleteM mdl
      ]


instance FromJSON Permissions where
    parseJSON (Bool True)  = return Everyone
    parseJSON (Bool False) = return Nobody
    parseJSON v@(Array _)  = Roles <$> parseJSON v
    parseJSON _            = error "Could not permissions"

instance ToJSON Permissions where
    toJSON Everyone  = Bool True
    toJSON Nobody    = Bool False
    toJSON (Roles r) = toJSON r


instance FromJSON Field where
    parseJSON (Object v) = Field        <$>
      v .: "name"                       <*>
      v .:? "type" .!= defaultFieldType <*>
      v .:? "index"        .!= False    <*>
      v .:? "indexCollate" .!= False    <*>
      v .:? "groupName"                 <*>
      v .:? "meta"                      <*>
      v .:? "canRead"  .!= Nobody       <*>
      v .:? "canWrite" .!= Nobody
    parseJSON _          = error "Could not parse field properties"

instance ToJSON Field where
    toJSON f = object
      [ "name"          .= name f
      , "type"          .= fieldType f
      , "index"         .= index f
      , "indexCollate"  .= indexCollate f
      , "groupName"     .= groupName f
      , "canRead"       .= canRead f
      , "canWrite"      .= canWrite f
      , "meta"          .= meta f
      ]


type Groups = M.Map B.ByteString [Field]

-- | Build new name `f:gK` for every field of group `g` to which field
-- `f` is spliced into.
groupFieldName :: FieldName
               -- ^ Name of field which is spliced into group
               -> FieldName
               -- ^ Name of group field
               -> FieldName
groupFieldName parent field = B.concat [parent, ":", field]

-- | Replace all model fields having `group` type with actual group
-- fields.
spliceGroups :: Groups -> Model -> Model
spliceGroups groups model =
    let
        origFields = fields model
    in
      model{fields = concat $
            map (\f -> 
                 case groupName f of
                   Just n -> 
                       case (M.lookup n groups) of
                         Just grp -> 
                             map (\gf -> gf{ groupName = Just n
                                           , name = groupFieldName (name f) (name gf)
                                           }) grp
                         Nothing -> [f]
                   _ -> [f]
                ) origFields}


-- | Set indices field of model to list of 'FieldIndex'es
cacheIndices :: Model -> Model
cacheIndices model = 
    model{indices = foldl' 
                    (\l f -> case (index f, indexCollate f) of
                               (True, c) -> (name f, c):l
                               _ -> l
                    ) 
          [] (fields model)}