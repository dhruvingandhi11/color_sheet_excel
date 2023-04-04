
from pymongo.collection import Collection
from pymongo.database import Database
from typing import List
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from bson import ObjectId

import pandas as pd

from pathlib import Path
import traceback
from datetime import datetime, date, time,timedelta




# DB_CONN_STR = "mongodb+srv://testing:testingMAC-56@ty-production.s7emg.mongodb.net/"
# DB_NAMES = "trailer_yard-01-31-09-41-31"

DB_CONN_STR = "mongodb+srv://testing:testingMAC-56@cluster1.s7emg.mongodb.net/"
DB_NAMES = "trailer_yard__4-02-02-08-03-17"

COLLECTION_NAME = "yard_locations"


try:
    conn = MongoClient(DB_CONN_STR)
    print("Connected successfully!!!")
except:
    print("Could not connect to MongoDB")

    

db = conn[DB_NAMES]
collection = db[COLLECTION_NAME]

    

def get_all_data():
    return collection.aggregate([
        # {
        #     "$match": {
        #         "frame_number":15601
        #     },
        # },
        # {
        #   "$limit": 1,  
        # },
        {
            "$sort": {"event_time": 1}
        },
    ],allowDiskUse=True)
    
all_frames = list(get_all_data())
print("Got all frames = ", len(all_frames))


