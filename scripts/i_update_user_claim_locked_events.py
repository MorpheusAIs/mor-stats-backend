import logging
from datetime import datetime
from psycopg2.extras import execute_values

from app.core.config import ETH_RPC_URL, distribution_contract
from app.db.database import get_db
from app.web3.web3_wrapper import Web3Provider
from helpers.database_helpers.db_helper import get_last_block_from_db
from helpers.web3_helper import get_events_in_batches, get_event_headers

logger = logging.getLogger(__name__)

START_BLOCK = 20180927
BATCH_SIZE = 1000000
TABLE_NAME = "user_claim_locked"

RPC_URL = ETH_RPC_URL
web3 = Web3Provider.get_instance()
contract = distribution_contract

def ensure_table_exists(headers):
    """Create the table if it doesn't exist, with columns based on event structure"""
    try:
        db = get_db()

        with db.cursor() as cursor:
            # Get column definitions, assume all event args are text for flexibility
            columns = [
                "timestamp TIMESTAMP",
                "transaction_hash TEXT",
                "block_number BIGINT"
            ]

            # Add columns for each event argument
            for header in headers[3:]:
                columns.append(f"{header} TEXT")

            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                {', '.join(columns)}
            )
            """
            cursor.execute(create_table_query)

            # Create index on block_number for efficient lookups
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_block_number ON {TABLE_NAME} (block_number)")

            cursor.commit()
            logger.info(f"Ensured table {TABLE_NAME} exists with required structure")
    except Exception as e:
        logger.error(f"Error ensuring table exists: {str(e)}")
        raise


def insert_events_to_db(events_data, headers):
    try:
        db = get_db()

        with db.cursor() as cursor:

            # Prepare column names for the insert query
            columns = ', '.join(headers)

            # Prepare values for the insert
            values = []
            for event in events_data:
                row_values = []
                for header in headers:
                    row_values.append(event.get(header, None))
                values.append(tuple(row_values))

            # Perform batch insert
            insert_query = f"""
            INSERT INTO {TABLE_NAME} ({columns})
            VALUES %s
            """
            execute_values(cursor, insert_query, values)

            cursor.commit()
            logger.info(f"Inserted {len(values)} events into database")
    except Exception as e:
        logger.error(f"Error inserting events to database: {str(e)}")
        raise


def process_user_claim_locked_events():
    event_name = "UserClaimLocked"
    try:
        latest_block = web3.eth.get_block('latest')['number']
        headers = get_event_headers(event_name)

        # Ensure the database table exists with correct structure
        ensure_table_exists(headers)

        # Get the last processed block
        last_processed_block = get_last_block_from_db(TABLE_NAME)

        if last_processed_block is None:
            start_block = START_BLOCK
        else:
            start_block = last_processed_block + 1

        events = list(get_events_in_batches(start_block, latest_block, event_name, BATCH_SIZE))
        logger.info(f"Processing {len(events)} new {event_name} events from block {start_block} to {latest_block}")

        if events:
            new_data = []
            for event in events:
                # Convert header names to lowercase for PostgreSQL convention
                row = {
                    'timestamp': datetime.fromtimestamp(
                        web3.eth.get_block(event['blockNumber'])['timestamp']),
                    'transaction_hash': event['transactionHash'].hex(),
                    'block_number': event['blockNumber']
                }

                # Add event arguments with lowercase keys
                for key, value in event['args'].items():
                    row[key.lower()] = str(value) if value is not None else None

                new_data.append(row)

            # Insert the new events into the database
            insert_events_to_db(new_data, headers)

            logger.info(f"Successfully processed and stored new events for {event_name}")
        else:
            logger.info(f"No new events found for {event_name}.")

    except Exception as e:
        logger.error(f"An error occurred in process_events: {str(e)}")
        logger.exception("Exception details:")


if __name__ == "__main__":
    process_user_claim_locked_events()