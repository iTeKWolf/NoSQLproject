from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from neo4j import GraphDatabase
import config


client = MongoClient(config.MONGO_URI, server_api=ServerApi('1'))
try:
    client.admin.command('ping')
    print("You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db=client["entertainment"]
collection=db["films"]

try:
    neo4j_driver= GraphDatabase.driver(config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD))
    with neo4j_driver.session() as session:
        session.run("RETURN 1")
    print("Connexion confirmee a Neo4j AuraDB !")
except Exception as e:
    print(f"Erreur de connexion a Neo4j : {e}")

def get_db():
    return db
def get_collection():
    return collection

def get_session():
    return neo4j_driver.session()