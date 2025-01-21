import math
from collections import defaultdict
import requests
from app.core.config import (STETH_TOKEN_ADDRESS, MOR_ARBITRUM_ADDRESS,
                             arb_positions_nft_contract, arb_pool_contract, MOR_MULTISIG_ARB)


def fetch_all_nfts(address):
    """Fetches all NFTs owned by the address."""
    balance = arb_positions_nft_contract.functions.balanceOf(address).call()
    nfts = []
    for i in range(balance):
        token_id = arb_positions_nft_contract.functions.tokenOfOwnerByIndex(address, i).call()
        nfts.append(token_id)
    return nfts


def calculate_amounts(liquidity, sqrt_price_x96, tick_current, tick_lower, tick_upper):
    """Calculates the token balances based on the tick and liquidity."""
    sqrt_ratio_a = math.sqrt(1.0001 ** tick_lower)
    sqrt_ratio_b = math.sqrt(1.0001 ** tick_upper)
    sqrt_price = sqrt_price_x96 / (1 << 96)

    if tick_current < tick_lower:
        amount0 = liquidity * (1 / sqrt_ratio_a - 1 / sqrt_ratio_b)
        amount1 = 0
    elif tick_current < tick_upper:
        amount0 = liquidity * (1 / sqrt_price - 1 / sqrt_ratio_b)
        amount1 = liquidity * (sqrt_price - sqrt_ratio_a)
    else:
        amount0 = 0
        amount1 = liquidity * (sqrt_ratio_b - sqrt_ratio_a)

    return amount0 / (10 ** 18), amount1 / (10 ** 18)  # Convert to standard unit


def get_asset_balances(token_id):
    """Fetches the asset balances for a specific NFT position."""
    position = arb_positions_nft_contract.functions.positions(token_id).call()

    # Extract relevant information
    token0 = position[2]
    token1 = position[3]
    fee = position[4]
    tick_lower = position[5]
    tick_upper = position[6]
    liquidity = position[7]

    # Fetch current tick and sqrt price
    slot0 = arb_pool_contract.functions.slot0().call()
    sqrt_price_x96 = slot0[0]
    current_tick = slot0[1]

    # Calculate amounts
    amount0, amount1 = calculate_amounts(liquidity, sqrt_price_x96, current_tick, tick_lower, tick_upper)

    return {
        'token0': {'address': token0, 'amount': amount0},
        'token1': {'address': token1, 'amount': amount1},
        'fee': fee,
        'liquidity': liquidity,
        'tick_lower': tick_lower,
        'tick_upper': tick_upper,
        'current_tick': current_tick
    }


def protocol_liquidity(address):
    """Fetches and calculates the protocol's liquidity and returns it in USD, MOR, and stETH values."""
    nft_ids = fetch_all_nfts(address)

    if not nft_ids:
        # print(f"No NFTs found for address {address}")
        return

    mor_price = 1
    steth_price = 1

    if mor_price is None or steth_price is None:
        raise Exception("Could not fetch MOR or stETH prices.")

    aggregated_positions = defaultdict(lambda: {'token0': {'balance': 0, 'address': None},
                                                'token1': {'balance': 0, 'address': None},
                                                'liquidity': 0})

    total_value_usd = 0

    for nft_id in nft_ids:
        # Token 0 is MOR and Token 1 is ETH
        balances = get_asset_balances(nft_id)
        key = f"{balances['token0']['address']}_{balances['token1']['address']}_{balances['fee']}"

        aggregated_positions[key]['token0']['address'] = balances['token0']['address']
        aggregated_positions[key]['token1']['address'] = balances['token1']['address']

        aggregated_positions[key]['token0']['balance'] += balances['token0']['amount']
        aggregated_positions[key]['token1']['balance'] += balances['token1']['amount']

        aggregated_positions[key]['liquidity'] += balances['liquidity']

        # Calculate the total value in USD for the position
        token0_value_usd = balances['token0']['amount'] * mor_price
        token1_value_usd = balances['token1']['amount'] * steth_price
        total_value_usd += token0_value_usd + token1_value_usd

    # Convert defaultdict to regular dict for JSON serialization
    aggregated_positions = dict(aggregated_positions)

    # Return USD, MOR, and stETH values
    return {
        "positions": aggregated_positions,
        "total_value_usd": 0,
        "mor_value": 0,
        "steth_value": 0
    }


def get_arb_protocol_liquidity():
    result = protocol_liquidity(MOR_MULTISIG_ARB)

    return result