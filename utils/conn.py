import os
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

# Replace with your Polygon RPC URL
POLYGON_RPC_URL = f'https://polygon-mainnet.g.alchemy.com/v2/{os.getenv('POLYGON_RPC_URL_KEY')}'

w3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL))
if not w3.is_connected():
    raise Exception("Failed to connect to Polygon network.")

print("Connected to Polygon:", w3.is_connected())