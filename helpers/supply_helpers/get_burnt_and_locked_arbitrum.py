import json
import asyncio
from datetime import datetime
from web3 import Web3
from pathlib import Path
import sys
from app.core.config import (erc20_abi, ARB_RPC_URL, MOR_ARBITRUM_ADDRESS, BURN_FROM_ADDRESS, BURN_TO_ADDRESS,
                             SAFE_ADDRESS, BURN_START_BLOCK)


def set_web3_on_arbitrum():
    w3 = Web3(Web3.HTTPProvider(ARB_RPC_URL))  # ARBITRUM CONNECTION

    if not w3.is_connected():
        raise ConnectionError("Failed to connect to Arbitrum via Alchemy")

    token_contract = w3.eth.contract(address=w3.to_checksum_address(MOR_ARBITRUM_ADDRESS), abi=erc20_abi)

    return w3, token_contract


def create_event_filter(w3, token_contract, from_address, to_address):
    return token_contract.events.Transfer.create_filter(
        from_block=BURN_START_BLOCK,
        to_block='latest',
        argument_filters={
            'from': w3.to_checksum_address(from_address),
            'to': w3.to_checksum_address(to_address)
        }
    )


def process_events(w3, events):
    amounts_by_date = {}
    total_amount = 0

    for event in events:
        amount = float(event['args']['value']) / pow(10, 18)

        # Fetch block timestamp
        block = w3.eth.get_block(event['blockNumber'])
        block_timestamp = block['timestamp']
        txn_date = datetime.utcfromtimestamp(block_timestamp).strftime('%d/%m/%Y')

        total_amount += amount

        # Update cumulative amount for the date
        if txn_date in amounts_by_date:
            amounts_by_date[txn_date] += amount
        else:
            amounts_by_date[txn_date] = total_amount

    return amounts_by_date, total_amount


async def get_amounts(from_address, to_address, label):
    w3, token_contract = set_web3_on_arbitrum()

    event_filter = create_event_filter(w3, token_contract, from_address, to_address)
    events = event_filter.get_all_entries()

    amounts_by_date, total_amount = process_events(w3, events)

    # Create result dictionary
    result = {
        label: amounts_by_date,
        f"total_{label.split('_')[-1]}_till_now": total_amount
    }
    result_json = json.dumps(result, indent=4)
    return result_json


async def get_burned_amounts():
    return await get_amounts(BURN_FROM_ADDRESS, BURN_TO_ADDRESS, "cumulative_mor_burnt")


async def get_locked_amounts():
    return await get_amounts(BURN_FROM_ADDRESS, SAFE_ADDRESS, "cumulative_mor_locked")