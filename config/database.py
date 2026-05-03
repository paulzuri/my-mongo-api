import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))

db = client.db_tweets

collection_name = db["clean_tweets"]

test_collection = db["test_raw_tweets"]

test_collection_clean = db["test_clean_tweets"]

scrape_run_collection = db["test_scrape_runs"]
