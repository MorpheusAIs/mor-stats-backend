import json
from web3 import Web3
from dotenv import load_dotenv
import os
import logging

load_dotenv()

ETH_RPC_URL = os.getenv("RPC_URL")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
SLACK_URL = os.getenv("SLACK_URL")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

NOTIFICATION_CHANNEL = "mor-compute-dash-errors"

web3 = Web3(Web3.HTTPProvider(ETH_RPC_URL))

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))

SHEET_UTILS_JSON_PATH = os.path.join(project_root, 'sheet_config',
                                     'google-sheet-credentials-example.json')

distribution_abi_path = os.path.join(project_root, 'abi', 'distribution_abi.json')

with open(distribution_abi_path, 'r') as file:
    distribution_abi = json.load(file)

DISTRIBUTION_PROXY_ADDRESS = "0x47176B2Af9885dC6C4575d4eFd63895f7Aaa4790"

distribution_contract = web3.eth.contract(address=web3.to_checksum_address(DISTRIBUTION_PROXY_ADDRESS),
                                          abi=distribution_abi)
