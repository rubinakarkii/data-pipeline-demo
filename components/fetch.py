import logging

async def fetch_data(session, url, headers):
    """Fetch data from a given API URL asynchronously."""
    try:
        async with session.post(url, headers=headers) as response:
            response_data = await response.json()
            logging.info(f"Data fetched successfully from {url}.")
            return response_data
    except Exception as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return None
