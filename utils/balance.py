from web3 import Web3
from dotenv import load_dotenv
import os

def fetch_balance():
    load_dotenv()

    # Connect to Polygon
    web3 = Web3(Web3.HTTPProvider('https://polygon-rpc.com/'))

    # Check connection
    if not web3.is_connected():
        raise ConnectionError("Failed to connect to Polygon RPC.")

    # Your wallet address
    address = os.getenv('PBK')

    # USDC contract on Polygon
    usdc_address = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
    usdc_abi = [
        {
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "type": "function",
        },
        {
            "constant": True,
            "inputs": [
                {"name": "_owner", "type": "address"},
                {"name": "_spender", "type": "address"},
            ],
            "name": "allowance",
            "outputs": [{"name": "remaining", "type": "uint256"}],
            "type": "function",
        },
    ]

    usdc_contract = web3.eth.contract(address=usdc_address, abi=usdc_abi)

    # Fetch balance
    balance_raw = usdc_contract.functions.balanceOf(address).call()
    balance = balance_raw / 10**6  # USDC has 6 decimals
    return balance