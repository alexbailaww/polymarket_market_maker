import os
from dotenv import load_dotenv

from web3 import Web3
from web3.constants import MAX_INT
from web3.middleware import ExtraDataToPOAMiddleware


def set_allowances():
    load_dotenv()

    rpc_url = os.getenv('POLYGON_RPC_URL') # Polygon rpc url
    priv_key = os.getenv('PK') # Polygon account private key (needs some MATIC)
    pub_key = os.getenv('PBK') # Polygon account public key corresponding to private key

    chain_id = 137

    erc20_approve = '''[{"constant": false,"inputs": [{"name": "_spender","type": "address" },{ "name": "_value", "type": "uint256" }],"name": "approve","outputs": [{ "name": "", "type": "bool" }],"payable": false,"stateMutability": "nonpayable","type": "function"}]'''
    erc1155_set_approval = '''[{"inputs": [{ "internalType": "address", "name": "operator", "type": "address" },{ "internalType": "bool", "name": "approved", "type": "bool" }],"name": "setApprovalForAll","outputs": [],"stateMutability": "nonpayable","type": "function"}]'''


    usdc_address = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
    ctf_address = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'

    web3 = Web3(Web3.HTTPProvider(rpc_url))
    web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    balance = web3.eth.get_balance(pub_key)

    if balance == 0:
      raise Exception('No matic in your wallet')

    print(f'Current MATIC balance: {web3.from_wei(balance, "ether")} MATIC')

    nonce = web3.eth.get_transaction_count(pub_key)

    usdc = web3.eth.contract(address=usdc_address, abi=erc20_approve)
    ctf = web3.eth.contract(address=ctf_address, abi=erc1155_set_approval)

    # CTF Exchange
    raw_usdc_approve_txn = usdc.functions.approve('0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E', int(MAX_INT, 0)
    ).build_transaction({'chainId': chain_id, 'from': pub_key, 'nonce': nonce})
    signed_usdc_approve_tx = web3.eth.account.sign_transaction(raw_usdc_approve_txn, private_key=priv_key)
    send_usdc_approve_tx = web3.eth.send_raw_transaction(signed_usdc_approve_tx.raw_transaction)
    usdc_approve_tx_receipt = web3.eth.wait_for_transaction_receipt(send_usdc_approve_tx, 600)
    print(usdc_approve_tx_receipt)

    nonce = web3.eth.get_transaction_count(pub_key)

    raw_ctf_approval_txn = ctf.functions.setApprovalForAll('0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E', True).build_transaction({'chainId': chain_id, 'from': pub_key, 'nonce': nonce})
    signed_ctf_approval_tx = web3.eth.account.sign_transaction(raw_ctf_approval_txn, private_key=priv_key)
    send_ctf_approval_tx = web3.eth.send_raw_transaction(signed_ctf_approval_tx.raw_transaction)
    ctf_approval_tx_receipt = web3.eth.wait_for_transaction_receipt(send_ctf_approval_tx, 600)
    print(ctf_approval_tx_receipt)

    nonce = web3.eth.get_transaction_count(pub_key)

    # Neg Risk CTF Exchange
    raw_usdc_approve_txn = usdc.functions.approve('0xC5d563A36AE78145C45a50134d48A1215220f80a', int(MAX_INT, 0)
    ).build_transaction({'chainId': chain_id, 'from': pub_key, 'nonce': nonce})
    signed_usdc_approve_tx = web3.eth.account.sign_transaction(raw_usdc_approve_txn, private_key=priv_key)
    send_usdc_approve_tx = web3.eth.send_raw_transaction(signed_usdc_approve_tx.raw_transaction)
    usdc_approve_tx_receipt = web3.eth.wait_for_transaction_receipt(send_usdc_approve_tx, 600)
    print(usdc_approve_tx_receipt)

    nonce = web3.eth.get_transaction_count(pub_key)

    raw_ctf_approval_txn = ctf.functions.setApprovalForAll('0xC5d563A36AE78145C45a50134d48A1215220f80a', True).build_transaction({'chainId': chain_id, 'from': pub_key, 'nonce': nonce})
    signed_ctf_approval_tx = web3.eth.account.sign_transaction(raw_ctf_approval_txn, private_key=priv_key)
    send_ctf_approval_tx = web3.eth.send_raw_transaction(signed_ctf_approval_tx.raw_transaction)
    ctf_approval_tx_receipt = web3.eth.wait_for_transaction_receipt(send_ctf_approval_tx, 600)
    print(ctf_approval_tx_receipt)

    nonce = web3.eth.get_transaction_count(pub_key)

    # Neg Risk Adapter
    raw_usdc_approve_txn = usdc.functions.approve('0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296', int(MAX_INT, 0)
    ).build_transaction({'chainId': chain_id, 'from': pub_key, 'nonce': nonce})
    signed_usdc_approve_tx = web3.eth.account.sign_transaction(raw_usdc_approve_txn, private_key=priv_key)
    send_usdc_approve_tx = web3.eth.send_raw_transaction(signed_usdc_approve_tx.raw_transaction)
    usdc_approve_tx_receipt = web3.eth.wait_for_transaction_receipt(send_usdc_approve_tx, 600)
    print(usdc_approve_tx_receipt)

    nonce = web3.eth.get_transaction_count(pub_key)

    raw_ctf_approval_txn = ctf.functions.setApprovalForAll('0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296', True).build_transaction({'chainId': chain_id, 'from': pub_key, 'nonce': nonce})
    signed_ctf_approval_tx = web3.eth.account.sign_transaction(raw_ctf_approval_txn, private_key=priv_key)
    send_ctf_approval_tx = web3.eth.send_raw_transaction(signed_ctf_approval_tx.raw_transaction)
    ctf_approval_tx_receipt = web3.eth.wait_for_transaction_receipt(send_ctf_approval_tx, 600)
    print(ctf_approval_tx_receipt)

def fetch_allowance():
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

    # Fetch allowance
    spender = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E'
    allowance_raw = usdc_contract.functions.allowance(address, spender).call()
    allowance = allowance_raw / 10**6
    
    return allowance
