from tkpweb.settings import MONGODB
from pymongo import Connection
from gridfs import GridFS
from contextlib import closing
import pyfits

def fetch_hdu_from_mongo(filename):
    if MONGODB["enabled"]:
        with closing(
            Connection(host=MONGODB["host"], port=MONGODB["port"])
        ) as mongo_connection:
            gfs = GridFS(mongo_connection[MONGODB["database"]])
            return pyfits.open(gfs.get_version(filename), mode="readonly")
