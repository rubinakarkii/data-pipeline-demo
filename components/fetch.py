import aiohttp
import asyncio
import logging
import os

logging.basicConfig(level=logging.INFO)

API_TOKEN = os.getenv('api_token')

endpoints = ["https://api.recruiting.app.silk.security/api/qualys/hosts/get?skip=0&limit=2",
            "https://api.recruiting.app.silk.security/api/crowdstrike/hosts/get?skip=0&limit=1"]


async def fetch_data(session, url):
    headers = {
        'token': API_TOKEN,
        'accept': 'application/json'
    }
    async with session.post(url, headers=headers) as response:
        try:
            return await response.json()
        except Exception as e:
            logging.error(e)


async def fetch_data_from_multiple_endpoints(endpoints):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, url) for url in endpoints]
        results = await asyncio.gather(*tasks)
        formatted_results = {endpoints[i]: results[i] for i in range(len(endpoints))}
        return formatted_results

def main():
    apis_responses = asyncio.run(fetch_data_from_multiple_endpoints(endpoints))
    for endpoint, result in apis_responses.items():
        print(f"Response from API {endpoint}: {result}")

    return apis_responses
