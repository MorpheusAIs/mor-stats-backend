import logging
from datetime import datetime
from decimal import Decimal

from app.core.config import ETH_RPC_URL, distribution_contract
from app.models.database_models import UserWithdrawnEvent
from app.repository import UserWithdrawnEventsRepository
from app.web3.web3_wrapper import Web3Provider
from helpers.database_helpers.db_helper import get_last_block_from_db
from helpers.web3_helper import get_events_in_batches

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_BLOCK = 20180927
BATCH_SIZE = 1000000
TABLE_NAME = "user_withdrawn_events"
EVENT_NAME = "UserWithdrawn"

RPC_URL = ETH_RPC_URL
web3 = Web3Provider.get_instance()
contract = distribution_contract


def insert_user_withdrawn_events(user_withdrawn_events: list[UserWithdrawnEvent]):
    """Insert user withdrawn events into the database using the repository"""
    try:
        if not user_withdrawn_events:
            return 0
            
        repository = UserWithdrawnEventsRepository()
        inserted_count = repository.bulk_insert(user_withdrawn_events)
        
        logger.info(f"Inserted {inserted_count} new {EVENT_NAME} events into database")
        return inserted_count
    except Exception as e:
        logger.error(f"Error inserting events to database: {str(e)}")
        raise


def process_user_withdrawn_events():
    """Main function to process UserWithdrawn events and store them in PostgreSQL"""
    try:
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
        events = list(get_events_in_batches(start_block, latest_block, EVENT_NAME, BATCH_SIZE))
        logger.info(f"Processing {len(events)} new {EVENT_NAME} events from block {start_block} to {latest_block}")

        if events:
            # Process events
            user_withdrawn_events = []
            for event in events:
                block_timestamp = web3.eth.get_block(event['blockNumber'])['timestamp']

                # Create UserWithdrawnEvent object
                user_withdrawn_event = UserWithdrawnEvent(
                    id=None,
                    timestamp=datetime.fromtimestamp(block_timestamp),
                    transaction_hash=event['transactionHash'].hex(),
                    block_number=event['blockNumber'],
                    pool_id=int(event['args'].get('poolId', 0)),
                    user_address=event['args'].get('user', ''),
                    amount=Decimal(event['args'].get('amount', 0))  # Store raw amount
                )
                user_withdrawn_events.append(user_withdrawn_event)

            # Insert events into database
            inserted_count = insert_user_withdrawn_events(user_withdrawn_events)

            logger.info(f"Successfully processed and stored {inserted_count} new {EVENT_NAME} events")
            return inserted_count
        else:
            logger.info(f"No new {EVENT_NAME} events found.")
            return 0

    except Exception as e:
        logger.error(f"An error occurred in process_user_withdrawn_events: {str(e)}")
        logger.exception("Exception details:")
        raise

if __name__ == "__main__":
    try:
        count = process_user_withdrawn_events()
        if count > 0:
            logger.info(f"Successfully processed and stored {count} new UserWithdrawn events")
        else:
            logger.info("No new UserWithdrawn events found")
    except Exception as e:
        logger.info(f"Error processing UserWithdrawn events: {str(e)}")