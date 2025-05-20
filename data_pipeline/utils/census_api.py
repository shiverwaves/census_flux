"""
Census API interaction utilities
"""
import requests
import logging
import time
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def fetch_census_data(base_url: str, variables: List[str], 
                      geo_filter: str, api_key: str, 
                      max_retries: int = 3) -> List[List[str]]:
    """
    Fetch data from Census API with retry mechanism
    
    Args:
        base_url: Census API base URL
        variables: List of Census variable codes to fetch
        geo_filter: Geographic filter (e.g., "state:*")
        api_key: Census API key
        max_retries: Maximum number of retry attempts
        
    Returns:
        List of lists with the data from Census API
    """
    params = {
        "get": ",".join(variables),
        "for": geo_filter,
        "key": api_key
    }
    
    # Add retry logic
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching Census data: {params}")
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            data = response.json()
            logger.info(f"Successfully fetched Census data with {len(data) - 1} rows")
            return data
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                # Exponential backoff
                wait_time = 2 ** attempt
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error("Max retries exceeded")
                raise
        except ValueError as e:
            logger.error(f"Error parsing JSON response: {str(e)}")
            raise
    
    # This should never be reached due to the raise in the loop
    raise Exception("Failed to fetch Census data after retries")