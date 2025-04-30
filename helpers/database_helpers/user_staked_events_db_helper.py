from psycopg2.extras import execute_values

from app.db.database import get_db
import logging

logger = logging.getLogger(__name__)

def ensure_user_staked_events_table_exists():
    """Create the table if it doesn't exist"""
    table_name = "user_staked_events"
    db = get_db()

    try:
        with db.cursor() as cursor:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
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
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_block ON {table_name} (block_number)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_user ON {table_name} (user_address)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_pool ON {table_name} (pool_id)")

            # Create a unique index on transaction hash and event log index
            # to prevent duplicate event processing
            cursor.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_tx_unique 
            ON {table_name} (transaction_hash, block_number)
            """)

            logger.info(f"Ensured table {table_name} exists with required structure")
    except Exception as e:
        logger.error(f"Error ensuring table exists: {str(e)}")
        raise

def get_last_block_from_user_staked_events_db():
    """Get the last processed block number from the database"""
    table_name = "user_staked_events"
    db = get_db()

    try:
        with db.cursor() as cursor:
            cursor.execute(f"SELECT MAX(block_number) FROM {table_name}")
            result = cursor.fetchone()[0]
            return int(result) if result else None
    except Exception as e:
        logger.warning(f"Error getting last block from database: {str(e)}")
        return None


def insert_events_to_db(events_data):
    """Insert event data into the database"""
    table_name = "user_staked_events"

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
            INSERT INTO {table_name}
            (timestamp, transaction_hash, block_number, pool_id, user_address, amount)
            VALUES %s
            ON CONFLICT (transaction_hash, block_number) DO NOTHING
            """

            execute_values(cursor, insert_query, values)

            inserted_count = cursor.rowcount
            logger.info(f"Inserted {inserted_count} new records into database")
            return inserted_count
    except Exception as e:
        logger.error(f"Error inserting events to database: {str(e)}")
        raise
