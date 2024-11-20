import logging
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")


def load_data_to_mongodb(df, db_name, collection_type):
    try:
        pandas_df = df.toPandas()
        db = client[db_name]
        collection = db[collection_type]
        collection.insert_many(pandas_df.to_dict("records"))
        logging.info("Successfully loaded the processed data to database")
    except Exception as e:
        logging.error(f"Error loading the processing data to database: {e}")