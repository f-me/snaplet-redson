{-|
  
  Model definitions loader.

-}

module Snap.Snaplet.Redson.Snapless.Metamodel.Loader
    ( -- * Lower-level operations
      loadGroups
    , loadModel
    -- * High-level helper
    , loadModels
    )

where

import Control.Monad
import Control.Monad.Trans
import Data.Functor

import qualified Data.Map as M

import Data.Aeson as A
import System.EasyFile
import Snap.Snaplet
import Snap.Snaplet.RedisDB

import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy as LB (readFile)

import Snap.Snaplet.Redson.Snapless.Metamodel

parseFile :: FromJSON a => FilePath -> IO (Maybe a)
parseFile filename = A.decode <$> LB.readFile filename


-- | Load groups from definitions file.
loadGroups :: FilePath -> IO (Maybe Groups)
loadGroups = parseFile


-- | Load model from specified location, performing group splicing,
-- applications and filling index cache.
loadModel :: FilePath
          -- ^ Path to model definition file
          -> Groups 
          -- ^ Group definitions
          -> IO (Maybe Model)
loadModel modelFile groups
    =  (fmap $ doApplications
             . spliceGroups groups)
    <$> parseFile modelFile

loadIndices :: FilePath
            -> Model
            -> IO Model
loadIndices indicesFile mdl
    = do
        isExist <- doesFileExist indicesFile
        minds <- if isExist then parseFile indicesFile else return Nothing
        return $ case minds of
          Just inds -> mdl { indices = M.fromList inds }
          Nothing -> mdl

-- | Build metamodel name from its file path.
pathToModelName :: FilePath -> ModelName
pathToModelName filepath = C8.pack $ takeBaseName filepath


-- | Read all models from directory to a map.
--
-- TODO: Perhaps rely on special directory file which explicitly lists
-- all models.
loadModels :: FilePath -- ^ Models directory
           -> FilePath -- ^ Indices directory
           -> FilePath -- ^ Group definitions file
           -> IO (M.Map ModelName Model)
loadModels directory idir groupsFile =
      do
        dirEntries <- getDirectoryContents directory
        -- Leave out non-files
        mdlFiles <- filterM (doesFileExist . fst)
                 (map (liftM2 (,) (directory </>)
                                  (idir </>))
                      dirEntries)
        gs <- loadGroups groupsFile
        case gs of
          Just groups -> do
                  mdls <- mapM (\(m, i) -> do
                                  mres <- loadModel m groups
                                  case mres of
                                    Just mdl -> loadIndices i mdl
                                    Nothing -> error $ "Could not parse " ++ m
                               ) mdlFiles
                  -- Splice groups & cache indices for served models
                  return $ M.fromList $
                         zip (map (pathToModelName . fst) mdlFiles) mdls
          Nothing -> error $ "Bad groups file " ++ groupsFile
