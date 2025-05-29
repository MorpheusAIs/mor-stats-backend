import asyncio
import logging
from dateutil import parser
from web3 import AsyncWeb3
from decimal import Decimal

from app.core.config import ETH_RPC_URL, distribution_contract
from app.db.database import get_db
from app.models.database_models import UserMultiplier
from app.repository import UserMultiplierRepository
from app.web3.web3_wrapper import get_block_number

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BATCH_SIZE = 50

INPUT_TABLE_NAME = "user_claim_locked"
TABLE_NAME = "user_multiplier"
EVENT_NAME = "UserMultiplier"

RPC_URL = ETH_RPC_URL
w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(ETH_RPC_URL))
contract = w3.eth.contract(address=distribution_contract.address, abi=distribution_contract.abi)



def get_unprocessed_user_multiplier_records():
    """Get records from user_claim_locked that haven't been processed yet"""
    try:
        repository = UserMultiplierRepository()
        records = repository.get_unprocessed_records()
        
        logger.info(f"Found {len(records)} unprocessed records")
        return records
    except Exception as e:
        logger.error(f"Error getting unprocessed records: {str(e)}")
        raise


async def get_multiplier(record):
    for attempt in range(MAX_RETRIES):
        try:
            pool_id = int(record['poolid'])
            user = w3.to_checksum_address(record['user'])

            if isinstance(record['timestamp'], str):
                timestamp = parser.parse(record['timestamp'])
            else:
                timestamp = record['timestamp']

            block_number = await get_block_number()
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
                    'multiplier': None
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


def insert_user_multiplier_events(user_multiplier_events: list[UserMultiplier]):
    """Insert processed multipliers into the database"""
    try:
        repository = UserMultiplierRepository()
        repository.bulk_insert(user_multiplier_events)
        logger.info(f"Inserted {len(user_multiplier_events)} new multiplier records into database")
    except Exception as e:
        logger.error(f"Error inserting multipliers: {str(e)}")
        raise


async def process_user_multiplier_events():
    try:
        # Get unprocessed records from input table
        records = get_unprocessed_user_multiplier_records()

        if not records:
            logger.info(f"No new records to process for {EVENT_NAME}")
            return

        # Split into batches
        batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]

        total_processed = 0
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i + 1}/{len(batches)}")

            # Process batch and get multipliers
            processed_records = await process_batch(batch)

            # Convert to UserMultiplier objects
            user_multiplier_events = []
            for record in processed_records:
                if record:
                    multiplier = format_multiplier(record['multiplier'])
                    
                    user_multiplier = UserMultiplier(
                        id=None,
                        user_claim_locked_start=record['id'],
                        timestamp=record['timestamp'],
                        transaction_hash=record['transaction_hash'],
                        block_number=record['block_number'],
                        pool_id=record['pool_id'],
                        user_address=record['user_address'],
                        multiplier=multiplier
                    )
                    
                    user_multiplier_events.append(user_multiplier)

            # Insert results into database
            if user_multiplier_events:
                insert_user_multiplier_events(user_multiplier_events)

            total_processed += len(batch)
            logger.info(f"Completed batch {i + 1}/{len(batches)}")

            # Add delay between batches to prevent rate limiting
            if i < len(batches) - 1:
                await asyncio.sleep(1)

        logger.info(f"Successfully processed and stored {EVENT_NAME} for {total_processed} users")

    except Exception as e:
        logger.error(f"Error in process_user_multiplier_events: {str(e)}")
        logger.exception("Exception details:")
        raise


if __name__ == "__main__":
    asyncio.run(process_user_multiplier_events())