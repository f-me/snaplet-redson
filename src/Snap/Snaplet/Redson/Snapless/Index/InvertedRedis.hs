
module Snap.Snaplet.Redson.Snapless.Index.InvertedRedis
  (create
  ,update
  ,read
  ) where

import Control.Exception
import Database.Redis
import Snap.Snaplet.Redson.Snapless.Index.Config


-- FIXME: add more info (like `val` or `commit`)
data InvertedRedisException
  = UpdateException String
  | ReadException String
  deriving (Exception, Show)


create :: IndexConfig -> Commit -> Redis ()
create = update


update :: IndexConfig -> Commit -> Redis ()
update (IndexConfig{..}) c = do
  let from = catMaybes $ map (`M.lookup` c) ix'from
  let to   = fromMaybe
      (error $ "InvertedRedis.update: no " ++ show ix'to ++ " in commit")
      (M.lookup ix'to c)
  let mkKey val = B.concat [ix'name, ":", val]
  res <- sequence [sadd (mkKey val) [to] | val <- from]
  case res of
    Left err -> throw $ UpdateException $ show err
    Right _  -> return ()


read :: IndexConfig -> FieldValue -> Redis [FieldValue]
read (IndexConfig{..}) val = do
  let key = B.concat [ix'name, ":", val]
  res <- smembers key
  case res of
    Left err -> throw $ ReadException $ show err
    Right r  -> return r
