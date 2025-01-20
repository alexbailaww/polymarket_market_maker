import os
from dotenv import set_key, load_dotenv

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from py_clob_client.constants import POLYGON

def create_client():
    load_dotenv()

    host = os.getenv('HOST')
    key = os.getenv('PK')
    chain_id = 137

    if not host:
        raise ValueError("Host not found. Please set host in the environment variables.")
    if not key:
        raise ValueError("Private key not found. Please set PK in the environment variables.")
    if not chain_id:
        raise ValueError("Private chain_id not found. Please set chain_id in the environment variables.")

    creds = ApiCreds(
        api_key=os.getenv("CLOB_API_KEY"),
        api_secret=os.getenv("CLOB_SECRET"),
        api_passphrase=os.getenv("CLOB_PASS_PHRASE"),
    )

    client = ClobClient(host, key = key, chain_id = chain_id, creds = creds)

    print('CLOB Client initialized.')

    return client

def generate_api_keys():
    load_dotenv()

    host = os.getenv('HOST')
    key = os.getenv("PK")
    chain_id = 137  
    
    if not key:
        raise ValueError("Private key not found. Please set PK in the environment variables.")
    
    client = ClobClient(host, key=key, chain_id=chain_id)
    try:
        api_creds = client.create_or_derive_api_creds()

        env_path = '.env'  # Path to your .env file
        load_dotenv(env_path)  # Load existing .env file if present

        set_key(env_path, 'CLOB_API_KEY', api_creds.api_key)
        set_key(env_path, 'CLOB_SECRET', api_creds.api_secret)
        set_key(env_path, 'CLOB_PASS_PHRASE', api_creds.api_passphrase)

        print("API credentials created.")

    except Exception as e:
        print("Error creating or deriving API credentials:", e)
