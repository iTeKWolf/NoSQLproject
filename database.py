from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import config


client = MongoClient(config.MONGO_URI, server_api=ServerApi('1'))
try:
    client.admin.command('ping')
    print("You successfully connected to MongoDB!")
except Exception as e:
    print(e)
db=client["entertainment"]
collection=db["films"]

def get_db():
    return db
def get_collection():
    return collection