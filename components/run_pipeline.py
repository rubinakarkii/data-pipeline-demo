import logging
import os
import asyncio
import aiohttp
from dotenv import load_dotenv

from fetch import fetch_data
from transformation import transform_qualys_data, transform_crowdstrike_data
from dedupe import merge_duplicates_from_transformed_data
from load_data import load_data_to_mongodb

load_dotenv()

API_TOKEN = os.getenv('api_token')
URL_QUALYS = "https://api.recruiting.app.silk.security/api/qualys/hosts/get?skip=0&limit=2" 
URL_CROWDSTRIKE = "https://api.recruiting.app.silk.security/api/crowdstrike/hosts/get?skip=0&limit=1"
HEADERS = {
    'token': API_TOKEN, 
    'accept': 'application/json'
}
DATABASE_NAME= os.getenv('database_name')
COLLECTION= os.getenv('collection_name')


async def etl_pipeline():
    """Run the full ETL pipeline: fetch, transform, and load data."""
    async with aiohttp.ClientSession() as session:
        data_qualys = await fetch_data(session, URL_QUALYS, HEADERS)
        data_crowdstrike = await fetch_data(session, URL_CROWDSTRIKE, HEADERS)

        if data_qualys:
            transformed_qualys = transform_qualys_data(data_qualys)
            final_qualys_data = merge_duplicates_from_transformed_data(transformed_qualys)
            load_data_to_mongodb(final_qualys_data, DATABASE_NAME, COLLECTION)
        
        if data_crowdstrike:
            transformed_crowdstrike = transform_crowdstrike_data(data_crowdstrike)
            final_crowdstrike_data = merge_duplicates_from_transformed_data(transformed_crowdstrike)
            load_data_to_mongodb(final_crowdstrike_data, DATABASE_NAME, COLLECTION)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(etl_pipeline())
