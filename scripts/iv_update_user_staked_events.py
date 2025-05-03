import logging
from datetime import datetime

from app.core.config import distribution_contract, ETH_RPC_URL
from app.web3.web3_wrapper import Web3Provider
from helpers.database_helpers.db_helper import get_last_block_from_db
from helpers.database_helpers.user_staked_events_db_helper import ensure_user_staked_events_table_exists, \
    insert_events_to_db
from helpers.slack import send_slack_notification
from helpers.web3_helper import get_events_in_batches

logger = logging.getLogger(__name__)

START_BLOCK = 20180927
BATCH_SIZE = 1000000
TABLE_NAME = "user_staked_events"

RPC_URL = ETH_RPC_URL
web3 = Web3Provider.get_instance()
contract = distribution_contract

def process_user_staked_events():
    event_name = "UserStaked"
    """Main function to process UserStaked events and store them in PostgreSQL"""
    try:
        # Ensure database table exists
        ensure_user_staked_events_table_exists()

        # Get the latest block number from the chain
        latest_block = web3.eth.get_block('latest')['number']

        # Get the last processed block from the database
        last_processed_block = get_last_block_from_db(TABLE_NAME)

        if last_processed_block is None:
            start_block = START_BLOCK
        else:
            start_block = last_processed_block + 1

        # Check if there are new blocks to process
        if start_block > latest_block:
            logger.info("No new blocks to process.")
            return 0

        # Get events in batches
        events = list(get_events_in_batches(start_block, latest_block, event_name, BATCH_SIZE))
        logger.info(f"Processing {len(events)} new {event_name} events from block {start_block} to {latest_block}")

        if events:
            # Process events
            processed_events = []
            for event in events:
                block_timestamp = web3.eth.get_block(event['blockNumber'])['timestamp']

                # Format event data for storage
                event_data = {
                    'timestamp': datetime.fromtimestamp(block_timestamp),
                    'transaction_hash': event['transactionHash'].hex(),
                    'block_number': event['blockNumber'],
                    'pool_id': int(event['args'].get('poolId', 0)),
                    'user_address': event['args'].get('user', ''),
                    'amount': int(event['args'].get('amount', 0))  # Store raw amount
                }
                processed_events.append(event_data)

            # Insert events into database
            inserted_count = insert_events_to_db(processed_events)

            logger.info(f"Successfully processed and stored {inserted_count} new events for {event_name}")
            return inserted_count
        else:
            logger.info(f"No new events found for {event_name}.")
            return 0

    except Exception as e:
        logger.error(f"An error occurred in process_events: {str(e)}")
        logger.exception("Exception details:")
        raise

if __name__ == "__main__":
    try:
        count = process_user_staked_events()
        if count > 0:
            send_slack_notification(f"Successfully processed and stored {count} new UserStaked events")
        else:
            send_slack_notification("No new UserStaked events found")
    except Exception as e:
        send_slack_notification(f"Error processing UserStaked events: {str(e)}")