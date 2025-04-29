import asyncio
import logging
from dateutil import parser
from web3 import AsyncWeb3
from decimal import Decimal
from psycopg2.extras import execute_values

from app.core.config import ETH_RPC_URL, distribution_contract
from app.db.database import get_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BATCH_SIZE = 50

INPUT_TABLE_NAME = "user_claim_locked"
OUTPUT_TABLE_NAME = "user_multiplier"

RPC_URL = ETH_RPC_URL
w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(ETH_RPC_URL))
contract = w3.eth.contract(address=distribution_contract.address, abi=distribution_contract.abi)


def ensure_table_exists():
    """Create the output table if it doesn't exist"""
    db = get_db()
    try:
        with db.cursor() as cursor:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                user_claim_locked_id INTEGER REFERENCES {INPUT_TABLE_NAME}(id),
                timestamp TIMESTAMP NOT NULL,
                transaction_hash TEXT NOT NULL,
                block_number BIGINT NOT NULL,
                pool_id INTEGER NOT NULL,
                user_address TEXT NOT NULL,
                multiplier NUMERIC(78, 0),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)

            # Create indexes for efficient lookups
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{OUTPUT_TABLE_NAME}_user ON {OUTPUT_TABLE_NAME} (user_address)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{OUTPUT_TABLE_NAME}_pool ON {OUTPUT_TABLE_NAME} (pool_id)")
            cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{OUTPUT_TABLE_NAME}_unique ON {OUTPUT_TABLE_NAME} (user_address, pool_id, block_number)")

            logger.info(f"Ensured table {OUTPUT_TABLE_NAME} exists with required structure")
    except Exception as e:
        logger.error(f"Error ensuring table exists: {str(e)}")
        raise


def get_unprocessed_records():
    """Get records from user_claim_locked that haven't been processed yet"""
    try:
        db = get_db()
        with db.cursor() as cursor:
            query = f"""
            SELECT ucl.id, ucl.timestamp, ucl.transaction_hash, ucl.block_number, ucl.poolid, ucl.user
            FROM {INPUT_TABLE_NAME} ucl
            LEFT JOIN {OUTPUT_TABLE_NAME} um 
            ON ucl.id = um.user_claim_locked_id
            WHERE um.id IS NULL
            """
            cursor.execute(query)

            columns = ["id", "timestamp", "transaction_hash", "block_number", "poolid", "user"]
            records = []

            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                records.append(record)

            logger.info(f"Found {len(records)} unprocessed records")
            return records
    except Exception as e:
        logger.error(f"Error getting unprocessed records: {str(e)}")
        raise


async def get_block_number(timestamp):
    latest_block = await w3.eth.get_block('latest', full_transactions=False)
    return latest_block['number']


async def get_multiplier(record):
    for attempt in range(MAX_RETRIES):
        try:
            pool_id = int(record['poolid'])
            user = w3.to_checksum_address(record['user'])

            if isinstance(record['timestamp'], str):
                timestamp = parser.parse(record['timestamp'])
            else:
                timestamp = record['timestamp']

            block_number = await get_block_number(timestamp)
            multiplier = await contract.functions.getCurrentUserMultiplier(pool_id, user).call(
                block_identifier=block_number)

            return {
                'id': record['id'],
                'timestamp': timestamp,
                'transaction_hash': record['transaction_hash'],
                'block_number': record['block_number'],
                'pool_id': pool_id,
                'user_address': user,
                'multiplier': multiplier,
                'error_message': None
            }
        except Exception as e:
            if 'Too Many Requests' in str(e) and attempt < MAX_RETRIES - 1:
                logger.warning(f"Rate limit hit, retrying in {RETRY_DELAY} seconds...")
                await asyncio.sleep(RETRY_DELAY)
                return None
            else:
                logger.error(f"Error processing record {record['id']}: {str(e)}")
                return {
                    'id': record['id'],
                    'timestamp': record['timestamp'] if isinstance(record['timestamp'], str) else record['timestamp'],
                    'transaction_hash': record['transaction_hash'],
                    'block_number': record['block_number'],
                    'pool_id': int(record['poolid']),
                    'user_address': record['user'],
                    'multiplier': None,
                    'error_message': str(e)
                }
    return None


async def process_batch(batch):
    tasks = [get_multiplier(record) for record in batch]
    return await asyncio.gather(*tasks)


def format_multiplier(value):
    if value is None:
        return None
    # Convert to Decimal for precise handling of large numbers
    decimal_value = Decimal(str(value))
    # Return as Decimal with no decimal places
    return decimal_value


def insert_multipliers(records):
    """Insert processed multipliers into the database"""
    try:
        with get_db() as conn:
            with conn.cursor() as cursor:
                # Prepare values for insert
                values = []
                for record in records:
                    multiplier = format_multiplier(record['multiplier'])

                    values.append((
                        record['id'],  # user_claim_locked_id
                        record['timestamp'],
                        record['transaction_hash'],
                        record['block_number'],
                        record['pool_id'],
                        record['user_address'],
                        multiplier,
                        record['error_message']
                    ))

                # Insert data in batch
                insert_query = f"""
                INSERT INTO {OUTPUT_TABLE_NAME} 
                (user_claim_locked_id, timestamp, transaction_hash, block_number, pool_id, user_address, multiplier, error_message)
                VALUES %s
                ON CONFLICT (user_address, pool_id, block_number) DO UPDATE 
                SET multiplier = EXCLUDED.multiplier, error_message = EXCLUDED.error_message
                """
                execute_values(cursor, insert_query, values)

                logger.info(f"Inserted {len(values)} new multiplier records into database")
    except Exception as e:
        logger.error(f"Error inserting multipliers: {str(e)}")
        raise


async def calculate_user_multipliers():
    try:
        # Ensure output table exists
        ensure_table_exists()

        # Get unprocessed records from input table
        records = get_unprocessed_records()

        if not records:
            logger.info("No new records to process")
            return

        # Split into batches
        batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]

        total_processed = 0
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i + 1}/{len(batches)}")

            # Process batch and get multipliers
            processed_records = await process_batch(batch)

            # Insert results into database
            insert_multipliers(processed_records)

            total_processed += len(batch)
            logger.info(f"Completed batch {i + 1}/{len(batches)}")

            # Add delay between batches to prevent rate limiting
            if i < len(batches) - 1:
                await asyncio.sleep(1)

        logger.info(f"Successfully calculated and stored multipliers for {total_processed} users")

    except Exception as e:
        logger.error(f"Error in calculate_user_multipliers: {str(e)}")
        logger.exception("Exception details:")
        raise


if __name__ == "__main__":
    asyncio.run(calculate_user_multipliers())