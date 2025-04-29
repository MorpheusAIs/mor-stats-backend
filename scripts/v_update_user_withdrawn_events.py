import logging
from datetime import datetime
from web3 import Web3
from psycopg2.extras import execute_values

from app.core.config import ETH_RPC_URL, distribution_contract
from app.db.database import get_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_BLOCK = 20180927
BATCH_SIZE = 1000000
TABLE_NAME = "user_withdrawn_events"

RPC_URL = ETH_RPC_URL
web3 = Web3(Web3.HTTPProvider(RPC_URL))
contract = distribution_contract

def ensure_table_exists():
    """Create the table if it doesn't exist"""

    db = get_db()
    try:
        with db.cursor() as cursor:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                transaction_hash TEXT NOT NULL,
                block_number BIGINT NOT NULL,
                pool_id INTEGER NOT NULL,
                user_address TEXT NOT NULL,
                amount NUMERIC(78, 0) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)

            # Create indexes for efficient lookups
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_block ON {TABLE_NAME} (block_number)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_user ON {TABLE_NAME} (user_address)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_pool ON {TABLE_NAME} (pool_id)")

            # Create a unique index on transaction hash and block number
            # to prevent duplicate event processing
            cursor.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_tx_unique 
            ON {TABLE_NAME} (transaction_hash, block_number)
            """)

            logger.info(f"Ensured table {TABLE_NAME} exists with required structure")
    except Exception as e:
        logger.error(f"Error ensuring table exists: {str(e)}")
        raise


def get_last_block_from_db():
    """Get the last processed block number from the database"""
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(f"SELECT MAX(block_number) FROM {TABLE_NAME}")
            result = cursor.fetchone()[0]
            return int(result) if result else None
    except Exception as e:
        logger.warning(f"Error getting last block from database: {str(e)}")
        return None


def get_events_in_batches(start_block, end_block, event_name):
    """Process blockchain events in batches to handle large block ranges"""
    current_start = start_block
    while current_start <= end_block:
        current_end = min(current_start + BATCH_SIZE, end_block)
        try:
            yield from get_events(current_start, current_end, event_name)
        except Exception as e:
            logger.error(f"Error getting events from block {current_start} to {current_end}: {str(e)}")
        current_start = current_end + 1


def get_events(from_block, to_block, event_name):
    """Get blockchain events for the specified block range"""
    try:
        event_filter = getattr(contract.events, event_name).create_filter(from_block=from_block, to_block=to_block)
        return event_filter.get_all_entries()
    except Exception as e:
        logger.error(f"Error getting events for {event_name} from block {from_block} to {to_block}: {str(e)}")
        return []


def insert_events_to_db(events_data):
    """Insert event data into the database"""
    if not events_data:
        return 0

    db = get_db()
    try:
        with db.cursor() as cursor:
            # Prepare values for insertion
            values = [
                (
                    event['timestamp'],
                    event['transaction_hash'],
                    event['block_number'],
                    event['pool_id'],
                    event['user_address'],
                    event['amount']
                )
                for event in events_data
            ]

            # Insert data with ON CONFLICT DO NOTHING to handle any duplicates
            insert_query = f"""
            INSERT INTO {TABLE_NAME} 
            (timestamp, transaction_hash, block_number, pool_id, user_address, amount)
            VALUES %s
            ON CONFLICT (transaction_hash, block_number) DO NOTHING
            """

            execute_values(cursor, insert_query, values)

            inserted_count = cursor.rowcount
            logger.info(f"Inserted {inserted_count} new withdrawal events into database")
            return inserted_count
    except Exception as e:
        logger.error(f"Error inserting withdrawal events to database: {str(e)}")
        raise


def process_user_withdrawn_events(event_name="UserWithdrawn"):
    """Main function to process UserWithdrawn events and store them in PostgreSQL"""
    try:
        # Ensure database table exists
        ensure_table_exists()

        # Get the latest block number from the chain
        latest_block = web3.eth.get_block('latest')['number']

        # Get the last processed block from the database
        last_processed_block = get_last_block_from_db()

        if last_processed_block is None:
            start_block = START_BLOCK
        else:
            start_block = last_processed_block + 1

        # Check if there are new blocks to process
        if start_block > latest_block:
            logger.info("No new blocks to process.")
            return 0

        # Get events in batches
        events = list(get_events_in_batches(start_block, latest_block, event_name))
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

            logger.info(f"Successfully processed and stored {inserted_count} new withdrawal events")
            return inserted_count
        else:
            logger.info(f"No new withdrawal events found.")
            return 0

    except Exception as e:
        logger.error(f"An error occurred in process_withdrawal_events: {str(e)}")
        logger.exception("Exception details:")
        raise


def send_slack_notification(message):
    """Placeholder for slack notification function"""
    # This would be implemented elsewhere or imported
    logger.info(f"NOTIFICATION: {message}")


if __name__ == "__main__":
    try:
        count = process_user_withdrawn_events()
        if count > 0:
            send_slack_notification(f"Successfully processed and stored {count} new UserWithdrawn events")
        else:
            send_slack_notification("No new UserWithdrawn events found")
    except Exception as e:
        send_slack_notification(f"Error processing UserWithdrawn events: {str(e)}")