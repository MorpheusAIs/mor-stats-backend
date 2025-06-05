import json
from web3 import Web3
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BURN_START_BLOCK = 0
MAINNET_BLOCK_1ST_JAN_2024 = 18913400  # 1st January 2024
AVERAGE_BLOCK_TIME = 15
TOTAL_SUPPLY_HISTORICAL_DAYS = 30
TOTAL_SUPPLY_HISTORICAL_START_BLOCK = 20432592  # 1st August 2024
PRICES_AND_VOLUME_DATA_DAYS = 300

BURN_FROM_ADDRESS = "0x151c2b49CdEC10B150B2763dF3d1C00D70C90956"
BURN_TO_ADDRESS = "0x000000000000000000000000000000000000dead"
SAFE_ADDRESS = "0xb1972e86B3380fd69DCb395F98D39fbF1A5f305A"
SUPPLY_PROXY_ADDRESS = "0x6CFe1dDfd88890E08276c7FA9D6DCa1cA4A224a9"
DISTRIBUTION_PROXY_ADDRESS = "0x47176B2Af9885dC6C4575d4eFd63895f7Aaa4790"
MOR_MAINNET_ADDRESS = "0xcbb8f1bda10b9696c57e13bc128fe674769dcec0"
MOR_ARBITRUM_ADDRESS = "0x092bAaDB7DEf4C3981454dD9c0A0D7FF07bCFc86"
MOR_BASE_ADDRESS = "0x7431aDa8a591C955a994a21710752EF9b882b8e3"
STETH_TOKEN_ADDRESS = '0x5300000000000000000000000000000000000004'

ETH_RPC_URL = os.getenv("RPC_URL")
ARB_RPC_URL = os.getenv("ARB_RPC_URL")
BASE_RPC_URL = os.getenv("BASE_RPC_URL")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
ARBISCAN_API_KEY = os.getenv("ARBISCAN_API_KEY")
BASESCAN_API_KEY = os.getenv("BASESCAN_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_QUERY_ID = os.getenv("DUNE_QUERY_ID")
SLACK_URL = os.getenv("SLACK_URL")
GITHUB_API_KEY = os.getenv("GITHUB_API_KEY")

NOTIFICATION_CHANNEL = "slack-example-channel"

web3 = Web3(Web3.HTTPProvider(ETH_RPC_URL))
web3_arb = Web3(Web3.HTTPProvider(ARB_RPC_URL))
web3_base = Web3(Web3.HTTPProvider(BASE_RPC_URL))

COINGECKO_HISTORICAL_PRICES = (f"https://api.coingecko.com/api/v3/coins/morpheusai/contract/"
                               f"{MOR_ARBITRUM_ADDRESS}/market_chart?"
                               f"vs_currency=usd&days={PRICES_AND_VOLUME_DATA_DAYS}")
DEXSCREENER_URL = f"https://api.dexscreener.com/latest/dex/tokens/{MOR_ARBITRUM_ADDRESS}"

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))

IMPLIED_PRICES_JSON = os.path.join(project_root,
                                   'json_files',
                                   'mor_price_data.json')

supply_abi_path = os.path.join(project_root, 'json_files', 'abi', 'supply_abi.json')
distribution_abi_path = os.path.join(project_root, 'json_files', 'abi', 'distribution_abi.json')
erc20_abi_path = os.path.join(project_root, 'json_files', 'abi', 'erc_20_abi.json')

with open(supply_abi_path, 'r') as file:
    supply_abi = json.load(file)
with open(distribution_abi_path, 'r') as file:
    distribution_abi = json.load(file)
with open(erc20_abi_path, 'r') as file:
    erc20_abi = json.load(file)

SUPPLY_ABI = supply_abi
DISTRIBUTION_ABI = distribution_abi
ERC20_ABI = erc20_abi

supply_contract = web3.eth.contract(address=web3.to_checksum_address(SUPPLY_PROXY_ADDRESS),
                                    abi=SUPPLY_ABI)
distribution_contract = web3.eth.contract(address=web3.to_checksum_address(DISTRIBUTION_PROXY_ADDRESS),
                                          abi=DISTRIBUTION_ABI)

# Uniswap ARB Config
MOR_MULTISIG_ARB = web3_arb.to_checksum_address("0x151c2b49CdEC10B150B2763dF3d1C00D70C90956")

arb_json_path_position = os.path.join(project_root, 'json_files', 'abi', 'uniswap', 'arb', 'arb_position_nft_abi.json')
arb_json_path_factory = os.path.join(project_root, 'json_files', 'abi', 'uniswap', 'arb', 'arb_uniswap_factory_abi.json')
arb_json_path_pool = os.path.join(project_root, 'json_files', 'abi', 'uniswap', 'arb', 'arb_pool_uniswap_abi.json')

with open(arb_json_path_position, 'r') as file:
    ARB_POSITIONS_NFT_ABI = json.load(file)
with open(arb_json_path_factory, 'r') as file:
    ARB_FACTORY_NFT_ABI = json.load(file)
with open(arb_json_path_pool, 'r') as file:
    ARB_POOL_ABI = json.load(file)

UNISWAP_V3_POSITIONS_NFT_ADDRESS_ARB = '0xC36442b4a4522E871399CD717aBDD847Ab11FE88'  #NonfungiblePositionManager
UNISWAP_V3_FACTORY_ADDRESS_ARB = '0x1F98431c8aD98523631AE4a59f267346ea31F984'  #UniswapV3Factory
UNISWAP_POOL_ADDRESS_ARB = "0xE5Cf22EE4988d54141B77050967E1052Bd9c7F7A"

arb_positions_nft_contract = web3_arb.eth.contract(address=web3_arb.to_checksum_address(UNISWAP_V3_POSITIONS_NFT_ADDRESS_ARB), abi=ARB_POSITIONS_NFT_ABI)
arb_factory_contract = web3_arb.eth.contract(address=web3_arb.to_checksum_address(UNISWAP_V3_FACTORY_ADDRESS_ARB), abi=ARB_FACTORY_NFT_ABI)
arb_pool_contract = web3_arb.eth.contract(address=web3_arb.to_checksum_address(UNISWAP_POOL_ADDRESS_ARB), abi=ARB_POOL_ABI)


# Uniswap BASE Config
MOR_MULTISIG_BASE = web3_base.to_checksum_address("0xf3ef00168dd40eae68a7e670d56c7b8724e0c183")
base_json_path_position = os.path.join(project_root, 'json_files', 'abi', 'uniswap', 'base', 'base_position_nft_abi.json')
base_json_path_factory = os.path.join(project_root, 'json_files', 'abi', 'uniswap', 'base', 'base_uniswap_factory_abi.json')
base_json_path_pool = os.path.join(project_root, 'json_files', 'abi', 'uniswap', 'base', 'base_pool_uniswap_abi.json')

with open(base_json_path_position, 'r') as file:
    BASE_POSITIONS_NFT_ABI = json.load(file)
with open(base_json_path_factory, 'r') as file:
    BASE_FACTORY_NFT_ABI = json.load(file)
with open(base_json_path_pool, 'r') as file:
    BASE_POOL_ABI = json.load(file)

UNISWAP_V3_POSITIONS_NFT_ADDRESS_BASE = '0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1'  #NonfungiblePositionManager
UNISWAP_V3_FACTORY_ADDRESS_BASE = '0x33128a8fC17869897dcE68Ed026d694621f6FDfD'  #UniswapV3Factory
UNISWAP_POOL_ADDRESS_BASE = "0x37ecD41f5a01B23a3d9bb3b4DdfEF4eD455d6fd3"

base_positions_nft_contract = web3_base.eth.contract(address=web3_base.to_checksum_address(UNISWAP_V3_POSITIONS_NFT_ADDRESS_BASE), abi=BASE_POSITIONS_NFT_ABI)
base_factory_contract = web3_base.eth.contract(address=web3_base.to_checksum_address(UNISWAP_V3_FACTORY_ADDRESS_BASE), abi=BASE_FACTORY_NFT_ABI)
base_pool_contract = web3_base.eth.contract(address=web3_base.to_checksum_address(UNISWAP_POOL_ADDRESS_BASE), abi=BASE_POOL_ABI)
