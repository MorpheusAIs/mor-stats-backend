import requests
from helpers.uniswap_helpers.get_uniswap_position_arb import get_arb_protocol_liquidity
from helpers.uniswap_helpers.get_uniswap_position_base import get_base_protocol_liquidity
from app.core.config import MOR_ARBITRUM_ADDRESS, STETH_TOKEN_ADDRESS

DEX_API_URL = "https://api.dexscreener.io/latest/dex/tokens/{}"


def fetch_token_price(token_address):
    """Fetches the token price in USD from the Dex Screener API."""
    api_url = DEX_API_URL.format(token_address)
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()
        if 'pairs' in data and len(data['pairs']) > 0 and 'priceUsd' in data['pairs'][0]:
            return float(data['pairs'][0]['priceUsd'])

    return None


def get_combined_uniswap_position():
    arb_position = get_arb_protocol_liquidity()
    base_position = get_base_protocol_liquidity()

    arb_position_data = arb_position['positions'][
        '0x092bAaDB7DEf4C3981454dD9c0A0D7FF07bCFc86_0x82aF49447D8a07e3bd95BD0d56f35241523fBab1_3000']
    arb_mor_balance = arb_position_data['token0']['balance']
    arb_eth_balance = arb_position_data['token1']['balance']

    base_position_data = base_position['positions'][
        '0x4200000000000000000000000000000000000006_0x7431aDa8a591C955a994a21710752EF9b882b8e3_3000']
    base_eth_balance = base_position_data['token0']['balance']
    base_mor_balance = base_position_data['token1']['balance']

    total_mor_balance = float(arb_mor_balance + base_mor_balance)
    total_eth_balance = float(arb_eth_balance + base_eth_balance)

    mor_price = fetch_token_price(MOR_ARBITRUM_ADDRESS)
    steth_price = fetch_token_price(STETH_TOKEN_ADDRESS)

    data = {}

    data['positions'] = {
        '0x092bAaDB7DEf4C3981454dD9c0A0D7FF07bCFc86_0x82aF49447D8a07e3bd95BD0d56f35241523fBab1_3000': {
            'token0': {
                'balance': total_mor_balance,
                'address': '0x092bAaDB7DEf4C3981454dD9c0A0D7FF07bCFc86'
            },
            'token1': {
                'balance': total_eth_balance,
                'address': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'
            },
            'liquidity': 0
        }
    }

    data['total_value_usd'] = (total_mor_balance * mor_price) + (total_eth_balance * steth_price)
    data['mor_value'] = total_mor_balance
    data['steth_value'] = total_eth_balance
    data['arb_pool_values'] = {
        "arb_pool_mor_balance": arb_mor_balance,
        "arb_pool_eth_balance": arb_eth_balance,
        "arb_pool_usd_value": (arb_mor_balance * mor_price) + (arb_eth_balance * steth_price)
    }
    data['base_pool_values'] = {
        "base_pool_mor_balance": base_mor_balance,
        "base_pool_eth_balance": base_eth_balance,
        "base_pool_usd_value": (base_mor_balance * mor_price) + (base_eth_balance * steth_price)
    }

    return data