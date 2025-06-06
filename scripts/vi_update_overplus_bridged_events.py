import logging
from datetime import datetime
from decimal import Decimal

from app.core.config import ETH_RPC_URL, distribution_contract
from app.db.database import get_db
from app.models.database_models import OverplusBridgedEvent
from app.repository import OverplusBridgedEventsRepository
from app.web3.web3_wrapper import Web3Provider
from helpers.database_helpers.db_helper import get_last_block_from_db
from helpers.web3_helper import get_events_in_batches

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_BLOCK = 20180927
BATCH_SIZE = 1000000
TABLE_NAME = "overplus_bridged_events"
EVENT_NAME = "OverplusBridged"  # The actual event name in the contract

RPC_URL = ETH_RPC_URL
web3 = Web3Provider.get_instance()
contract = distribution_contract


def insert_overplus_bridged_events(overplus_bridged_events: list[OverplusBridgedEvent]):
    """Insert overplus bridged events into the database using the repository"""
    try:
        if not overplus_bridged_events:
            return 0
            
        repository = OverplusBridgedEventsRepository()
        inserted_count = repository.bulk_insert(overplus_bridged_events)
        
        logger.info(f"Inserted {inserted_count} new {EVENT_NAME} events into database")
        return inserted_count
    except Exception as e:
        logger.error(f"Error inserting {EVENT_NAME} events to database: {str(e)}")
        raise


def process_overplus_bridged_events():
    """Main function to process OverplusBridged events and store them in PostgreSQL"""
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

        # Get events in batches using the EVENT_NAME constant
        events = list(get_events_in_batches(start_block, latest_block, EVENT_NAME, BATCH_SIZE))
        logger.info(f"Processing {len(events)} new {EVENT_NAME} events from block {start_block} to {latest_block}")

        if events:
            # Process events
            overplus_bridged_events = []
            for event in events:
                block_timestamp = web3.eth.get_block(event['blockNumber'])['timestamp']

                # Note the special handling for uniqueId - converting to hex
                unique_id_hex = event['args'].get('uniqueId', b'').hex()

                # Create OverplusBridgedEvent object
                overplus_bridged_event = OverplusBridgedEvent(
                    id=None,
                    timestamp=datetime.fromtimestamp(block_timestamp),
                    transaction_hash=event['transactionHash'].hex(),
                    block_number=event['blockNumber'],
                    amount=Decimal(event['args'].get('amount', 0)),  # Store raw amount
                    unique_id=unique_id_hex
                )
                overplus_bridged_events.append(overplus_bridged_event)

            # Insert events into database
            inserted_count = insert_overplus_bridged_events(overplus_bridged_events)

            logger.info(f"Successfully processed and stored {inserted_count} new {EVENT_NAME} events")
            return inserted_count
        else:
            logger.info(f"No new {EVENT_NAME} events found.")
            return 0

    except Exception as ex:
        logger.error(f"An error occurred in process_overplus_bridged_events: {str(ex)}")
        logger.exception("Exception details:")
        raise ex


if __name__ == "__main__":
    try:
        count = process_overplus_bridged_events()
        if count > 0:
            logger.info(f"Successfully processed and stored {count} new {EVENT_NAME} events")
        else:
            logger.info(f"No new {EVENT_NAME} events found")
    except Exception as e:
        logger.info(f"Error processing {EVENT_NAME} events: {str(e)}")