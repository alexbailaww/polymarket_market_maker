import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_positions():
    url = "https://data-api.polymarket.com/positions"
    params = {"user": os.getenv('PPA')}
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        print("Positions retrieved.")
        return response.json()
    else:
        raise Exception(f"Error retrieving positions: {response.status_code} {response.text}")