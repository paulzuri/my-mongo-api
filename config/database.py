import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))

db = client.db_tweets

collection_name = db["clean_tweets"]

test_collection = db["test_collection"]

scrape_run_collection = db["scrape_runs"]