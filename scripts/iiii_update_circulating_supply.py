import logging
from datetime import datetime
from decimal import Decimal
from web3.exceptions import BlockNotFound
from psycopg2.extras import execute_values

from app.core.config import distribution_contract
from app.db.database import get_db
from app.web3.web3_wrapper import Web3Provider

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TABLE_NAME = "circulating_supply"

web3 = Web3Provider.get_instance()

def ensure_table_exists():
    """Create the circulating supply table if it doesn't exist"""
    db = get_db()
    try:
        with db.cursor() as cursor:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                circulating_supply_at_that_date NUMERIC(36, 18) NOT NULL,
                block_timestamp_at_that_date BIGINT NOT NULL,
                total_claimed_that_day NUMERIC(36, 18) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)

            # Create indexes for efficient lookups
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_date ON {TABLE_NAME} (date)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_timestamp ON {TABLE_NAME} (block_timestamp_at_that_date)")

            # Add unique constraint on date
            cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_date_unique ON {TABLE_NAME} (date)")

            logger.info(f"Ensured table {TABLE_NAME} exists with required structure")
    except Exception as e:
        logger.error(f"Error ensuring table exists: {str(e)}")
        raise

def get_latest_record():
    """Get the latest circulating supply record from the database"""
    db = get_db()
    try:
        with db.cursor() as cursor:
            query = f"""
            SELECT date, circulating_supply_at_that_date, block_timestamp_at_that_date
            FROM {TABLE_NAME}
            ORDER BY date DESC
            LIMIT 1
            """
            cursor.execute(query)
            row = cursor.fetchone()

            if row:
                return {
                    'date': row[0],
                    'circulating_supply_at_that_date': Decimal(row[1]),
                    'block_timestamp_at_that_date': row[2]
                }
            else:
                logger.error("No records found in the database")
                return None
    except Exception as e:
        logger.error(f"Error getting latest record: {str(e)}")
        raise

def save_new_supply_data(new_data):
    """Save new circulating supply data to the database"""
    if not new_data:
        return 0

    db = get_db()
    try:
        with db.cursor() as cursor:
            # Prepare values for insert
            values = []
            for record in new_data:
                # Convert date string to date object
                date_obj = datetime.strptime(record['date'], '%d/%m/%Y').date()

                values.append((
                    date_obj,
                    record['circulating_supply_at_that_date'],
                    record['block_timestamp_at_that_date'],
                    record['total_claimed_that_day']
                ))

            # Insert data with ON CONFLICT DO UPDATE
            insert_query = f"""
            INSERT INTO {TABLE_NAME} 
            (date, circulating_supply_at_that_date, block_timestamp_at_that_date, total_claimed_that_day)
            VALUES %s
            ON CONFLICT (date)
            DO UPDATE SET
                circulating_supply_at_that_date = EXCLUDED.circulating_supply_at_that_date,
                block_timestamp_at_that_date = EXCLUDED.block_timestamp_at_that_date,
                total_claimed_that_day = EXCLUDED.total_claimed_that_day
            """

            execute_values(cursor, insert_query, values)

            # Get total count of records
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            total_count = cursor.fetchone()[0]

            logger.info(f"Saved {len(values)} new circulating supply records. Total records: {total_count}")
            return total_count
    except Exception as e:
        logger.error(f"Error saving supply data: {str(e)}")
        raise


def get_block_number_by_timestamp(timestamp):
    """Binary search to find the block number closest to the given timestamp."""
    provider = Web3Provider.get_instance()
    left = 1
    right = provider.eth.get_block('latest')['number']

    while left <= right:
        mid = (left + right) // 2
        try:
            mid_block = provider.eth.get_block(mid)
            if mid_block['timestamp'] == timestamp:
                return mid
            elif mid_block['timestamp'] < timestamp:
                left = mid + 1
            else:
                right = mid - 1
        except BlockNotFound:
            right = mid - 1

    return left  # Return the closest block number


def send_slack_notification(message):
    """Placeholder for slack notification function"""
    # This would be implemented elsewhere or imported
    logger.info(f"NOTIFICATION: {message}")


def update_circulating_supply():
    try:
        # Ensure the table exists
        ensure_table_exists()

        # Get the latest record from the database
        latest_record = get_latest_record()
        if not latest_record:
            logger.error("Cannot update: No baseline record found in the database")
            send_slack_notification("Error: Cannot update circulating supply - no baseline record found")
            return None

        # Extract latest values
        latest_block_timestamp = int(latest_record['block_timestamp_at_that_date'])
        latest_circulating_supply = Decimal(latest_record['circulating_supply_at_that_date'])

        # Find the block number for the latest timestamp
        start_block = get_block_number_by_timestamp(latest_block_timestamp)
        start_block += 1  # Start from the next block

        logger.info(f"Fetching events from block {start_block} to latest")

        # Create a filter for UserClaimed events from the latest block timestamp
        event_filter = distribution_contract.events.UserClaimed.create_filter(
            from_block=start_block,
            to_block='latest'
        )

        # Fetch all new events
        events = event_filter.get_all_entries()
        logger.info(f"Found {len(events)} new UserClaimed events")

        # Process new events
        new_data = []
        for event in events:
            try:
                block_number = event['blockNumber']
                block = web3.eth.get_block(block_number)
                timestamp = block['timestamp']
                date_str = datetime.utcfromtimestamp(timestamp).strftime('%d/%m/%Y')

                # Convert Wei to Ether (dividing by 10^18)
                amount = Decimal(event['args']['amount']) / Decimal(10 ** 18)
                latest_circulating_supply += amount

                new_data.append({
                    "date": date_str,
                    "circulating_supply_at_that_date": latest_circulating_supply,
                    "block_timestamp_at_that_date": timestamp,
                    "total_claimed_that_day": amount
                })

            except BlockNotFound:
                logger.warning(f"Block {block_number} not found. Skipping...")
                continue
            except Exception as e:
                logger.error(f"Error processing event: {e}")
                break

        if new_data:
            # Aggregate data by date (in case multiple claims occur on the same day)
            date_totals = {}
            for record in new_data:
                date = record['date']
                if date in date_totals:
                    # Add the claimed amount but keep the latest circulating supply
                    date_totals[date]['total_claimed_that_day'] += record['total_claimed_that_day']
                    date_totals[date]['circulating_supply_at_that_date'] = record['circulating_supply_at_that_date']
                    # Keep the latest timestamp for that date
                    if record['block_timestamp_at_that_date'] > date_totals[date]['block_timestamp_at_that_date']:
                        date_totals[date]['block_timestamp_at_that_date'] = record['block_timestamp_at_that_date']
                else:
                    date_totals[date] = record.copy()

            # Convert back to list
            aggregated_data = list(date_totals.values())

            # Save to database
            total_count = save_new_supply_data(aggregated_data)

            send_slack_notification(f"Circulating Supply updated. Total records: {total_count}")
            return total_count
        else:
            logger.info("No new data to update.")
            send_slack_notification("No new Circulating Supply data to update.")
            return None

    except Exception as e:
        logger.error(f"Error updating circulating supply: {e}")
        logger.exception("Exception details:")
        send_slack_notification(f"Error updating Circulating Supply: {str(e)}")
        return None


# Function to be called by cron job
def cron_update_circulating_supply():
    update_circulating_supply()


if __name__ == "__main__":
    cron_update_circulating_supply()