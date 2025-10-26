from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")


def save_population_raw(df, table_html):
    try:
        client = MongoClient(MONGODB_URI)
        db = client[DB_NAME]
        collection = db['wiki_population_raw']
        
        
        records = df.to_dict(orient="records")
        for record in records:
            record['table_html'] = table_html
        
        collection.delete_many({})  
        collection.insert_many(records)
        print(f"saved completed population to wiki_population_raw")
        client.close()
    except Exception as e:
        print(f"error saving population raw: {e}")

def save_area_raw(df, raw_text):
    try:
        client = MongoClient(MONGODB_URI)
        db = client[DB_NAME]
        collection = db['wiki_area_raw']
        
        records = df.to_dict(orient="records")
        for record in records:
            record['raw_text'] = raw_text
        
        collection.delete_many({})
        collection.insert_many(records)
        print(f"saved completed area to wiki_area_raw")
        client.close()
    except Exception as e:
        print(f"error saving area raw: {e}")

def save_joined_table(df):
    try:
        client = MongoClient(MONGODB_URI)
        db = client[DB_NAME]
        collection = db['wiki_pop_raw']
        
        records = df.to_dict(orient="records")
        
        collection.delete_many({})
        collection.insert_many(records)
        print(f"saved completed joinded table to wiki_pop_raw")
        client.close()
    except Exception as e:
        print(f"error saving joined table: {e}")


def load_from_mongodb(collection_name='wiki_pop_raw'):
    try:
        client = MongoClient(MONGODB_URI)
        db = client[DB_NAME]
        collection = db[collection_name]
        data = list(collection.find())
        df = pd.DataFrame(data)
        print(f"loaded completed from {collection_name}")
        client.close()
        return df
    except Exception as e:
        print(f"error loading from mongodb: {e}")
        return None


def save_to_csv(df, filename, location="data/"):
    try:
        if not os.path.exists(location):
            os.makedirs(location)
        df.to_csv(os.path.join(location, filename), index=False)
        print(f"saved to {location}{filename}")
    except Exception as e:
        print(f"error saving to csv: {e}")