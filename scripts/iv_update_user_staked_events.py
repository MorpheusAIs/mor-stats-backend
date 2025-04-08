import logging
import os
from datetime import datetime
from web3 import Web3
import pandas as pd
from configuration.config import ETH_RPC_URL, distribution_contract
from sheet_config.google_utils import (
    download_sheet, append_new_data, clear_and_upload_new_records
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_BLOCK = 20180927
BATCH_SIZE = 1000000
SHEET_NAME = "UserStaked"

RPC_URL = ETH_RPC_URL

web3 = Web3(Web3.HTTPProvider(RPC_URL))
contract = distribution_contract


def get_events_in_batches(start_block, end_block, event_name):
    current_start = start_block
    while current_start <= end_block:
        current_end = min(current_start + BATCH_SIZE, end_block)
        try:
            yield from get_events(current_start, current_end, event_name)
        except Exception as e:
            logger.error(f"Error getting events from block {current_start} to {current_end}: {str(e)}")
        current_start = current_end + 1


def get_events(from_block, to_block, event_name):
    try:
        event_filter = getattr(contract.events, event_name).create_filter(from_block=from_block, to_block=to_block)
        return event_filter.get_all_entries()
    except Exception as e:
        logger.error(f"Error getting events for {event_name} from block {from_block} to {to_block}: {str(e)}")
        return []


def get_event_headers(event_name):
    event_abi = next((e for e in contract.abi if e['type'] == 'event' and e['name'] == event_name), None)
    if not event_abi:
        raise ValueError(f"Event {event_name} not found in ABI")
    return ['Timestamp', 'TransactionHash', 'BlockNumber'] + [input['name'] for input in event_abi['inputs']]


def get_last_block_from_sheet(sheet_data):
    try:
        df = pd.read_csv(sheet_data)
        return max(df['BlockNumber'].astype(int))
    except (ValueError, KeyError):
        logger.warning(f"Sheet is empty or corrupted. Starting from default block.")
        return None


def process_user_staked_events(event_name="UserStaked"):
    try:
        latest_block = web3.eth.get_block('latest')['number']
        headers = get_event_headers(event_name)

        # Download existing sheet data
        existing_sheet = download_sheet(SHEET_NAME)
        last_processed_block = get_last_block_from_sheet(existing_sheet)

        if last_processed_block is None:
            start_block = START_BLOCK
        else:
            start_block = last_processed_block + 1

        events = list(get_events_in_batches(start_block, latest_block, event_name))
        logger.info(f"Processing {len(events)} new {event_name} events from block {start_block} to {latest_block}")

        if events:
            new_data = []
            for event in events:
                row = {
                    'Timestamp': datetime.fromtimestamp(
                        web3.eth.get_block(event['blockNumber'])['timestamp']).isoformat(),
                    'TransactionHash': event['transactionHash'].hex(),
                    'BlockNumber': event['blockNumber'],
                    'PoolId': event['args'].get('poolId', ''),
                    'User': event['args'].get('user', ''),
                    'Amount': event['args'].get('amount', '')
                }
                new_data.append(row)

            # Append new data to existing sheet data
            updated_csv = append_new_data(existing_sheet, new_data, f'updated_{SHEET_NAME}.csv')

            # Upload updated data to Google Sheets
            clear_and_upload_new_records(SHEET_NAME, updated_csv)

            # Clean up temporary files
            os.remove(existing_sheet)
            os.remove(updated_csv)

            logger.info(f"Successfully processed and uploaded new events for {event_name}")
            # slack_notification(f"Successfully processed and uploaded new events for {event_name}")
        else:
            logger.info(f"No new events found for {event_name}.")
            # slack_notification(f"No new events found for {event_name}.")

    except Exception as e:
        logger.error(f"An error occurred in process_events: {str(e)}")
        logger.exception("Exception details:")
        # slack_notification(f"Error in processing events for {event_name}: {str(e)}")
