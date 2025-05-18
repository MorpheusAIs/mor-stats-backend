import logging
from datetime import datetime

from app.core.config import distribution_contract, ETH_RPC_URL
from app.db.database import get_db
from app.models.database_models import UserStakedEvent
from app.repository import UserStakedEventsRepository
from app.web3.web3_wrapper import Web3Provider
from helpers.database_helpers.db_helper import get_last_block_from_db
from helpers.slack import send_slack_notification
from helpers.web3_helper import get_events_in_batches

logger = logging.getLogger(__name__)

START_BLOCK = 20180927
BATCH_SIZE = 1000000
TABLE_NAME = "user_staked_events"
EVENT_NAME = "UserStaked"

RPC_URL = ETH_RPC_URL
web3 = Web3Provider.get_instance()
contract = distribution_contract

def ensure_user_staked_events_table_exists():
    """Check if the table exists - table creation is now handled by the seed script"""
    try:
        db = get_db()
        
        with db.cursor() as cursor:
            # Just check if the table exists
            check_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = '{TABLE_NAME}'
            );
            """
            cursor.execute(check_query)
            exists = cursor.fetchone()['exists']
            
            if not exists:
                logger.error(f"Table {TABLE_NAME} does not exist. Run 'make seed' first to create all tables.")
                raise Exception(f"Table {TABLE_NAME} does not exist")
                
            logger.info(f"Table {TABLE_NAME} exists")
    except Exception as e:
        logger.error(f"Error checking if table exists: {str(e)}")
        raise

def insert_user_staked_events(user_staked_events: list[UserStakedEvent]):
    """Insert user staked events into the database using the repository"""
    try:
        repository = UserStakedEventsRepository()
        inserted_count = repository.bulk_insert(user_staked_events)
        logger.info(f"Inserted {inserted_count} new {EVENT_NAME} events into database")
        return inserted_count
    except Exception as e:
        logger.error(f"Error inserting events to database: {str(e)}")
        raise

def process_user_staked_events():
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
        events = list(get_events_in_batches(start_block, latest_block, EVENT_NAME, BATCH_SIZE))
        logger.info(f"Processing {len(events)} new {EVENT_NAME} events from block {start_block} to {latest_block}")

        if events:
            # Process events
            user_staked_events = []
            for event in events:
                block_timestamp = web3.eth.get_block(event['blockNumber'])['timestamp']

                # Create UserStakedEvent object
                user_staked_event = UserStakedEvent(
                    id=None,
                    timestamp=datetime.fromtimestamp(block_timestamp),
                    transaction_hash=event['transactionHash'].hex(),
                    block_number=event['blockNumber'],
                    pool_id=int(event['args'].get('poolId', 0)),
                    user_address=event['args'].get('user', ''),
                    amount=int(event['args'].get('amount', 0))  # Store raw amount
                )
                user_staked_events.append(user_staked_event)

            # Insert events into database
            inserted_count = insert_user_staked_events(user_staked_events)

            logger.info(f"Successfully processed and stored {inserted_count} new events for {EVENT_NAME}")
            return inserted_count
        else:
            logger.info(f"No new events found for {EVENT_NAME}.")
            return 0

    except Exception as e:
        logger.error(f"An error occurred in process_events: {str(e)}")
        logger.exception("Exception details:")
        raise

if __name__ == "__main__":
    try:
        count = process_user_staked_events()
        if count > 0:
            logger.info(f"Successfully processed and stored {count} new {EVENT_NAME} events")
        else:
            logger.info(f"No new {EVENT_NAME} events found")
    except Exception as e:
        logger.error(f"Error processing {EVENT_NAME} events: {str(e)}")