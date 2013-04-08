from tkpweb.settings import MONGODB
from pymongo import Connection
from gridfs import GridFS
from contextlib import closing
from tempfile import mkstemp
from os import fdopen
import pyfits

def fetch_hdu_from_mongo(filename):
    if MONGODB["enabled"]:
        with closing(
            Connection(host=MONGODB["host"], port=MONGODB["port"])
        ) as mongo_connection:
            gfs = GridFS(mongo_connection[MONGODB["database"]])
            return pyfits.open(gfs.get_version(filename), mode="readonly")

def fetch_file_from_mongo(filename):
    if MONGODB["enabled"]:
        handle, tempfilename = mkstemp()
        with fdopen(handle, "w") as output, closing(
            Connection(host=MONGODB["host"], port=MONGODB["port"])
        ) as mongo_connection:
            gfs = GridFS(mongo_connection[MONGODB["database"]])
            output.write(gfs.get_version(filename).read())
        return tempfilename
