import asyncio
import logging
from app.repository.user_claim_locked_repository import UserClaimLockedRepository
from web3 import AsyncWeb3
from decimal import Decimal

from app.core.config import ETH_RPC_URL, distribution_contract
from app.db.database import get_db
from app.models.database_models import UserClaimLocked, UserMultiplier
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


async def get_multiplier(record: UserClaimLocked) -> UserMultiplier:
    for attempt in range(MAX_RETRIES):
        try:
            user = w3.to_checksum_address(record.user_address)

            block_number = await get_block_number()
            multiplier = await contract.functions.getCurrentUserMultiplier(record.pool_id, user).call(
                block_identifier=block_number)
            logger.info(f"user {str(user)} multiplier: {str(multiplier)}")

            return UserMultiplier(
                user_claim_locked_start = record.claim_lock_start,
                user_claim_locked_end = record.claim_lock_end,
                timestamp = record.timestamp,
                block_number = block_number,
                pool_id = record.pool_id,
                user_address = record.user_address,
                multiplier = format_multiplier(multiplier)
            )
        except Exception as e:
            if 'Too Many Requests' in str(e) and attempt < MAX_RETRIES - 1:
                logger.warning(f"Rate limit hit, retrying in {RETRY_DELAY} seconds...")
                await asyncio.sleep(RETRY_DELAY)
                return None
            else:
                logger.error(f"Error processing user {user}: poolid {str(record.pool_id,)}, error {str(e)}")
                return None
    return None


async def process_batch(batch : list[UserClaimLocked]):
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
        user_multiplier_repository = UserClaimLockedRepository()
        records = user_multiplier_repository.get_unique_user_pool_combinations()

        user_multiplier_repository = UserMultiplierRepository()
        user_multiplier_repository.clean_table()      

        batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]

        total_processed = 0
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i + 1}/{len(batches)}")

            processed_records = await process_batch(batch)

            if processed_records:
                insert_user_multiplier_events(processed_records)

            total_processed += len(batch)
            logger.info(f"Completed batch {i + 1}/{len(batches)}")

            if i < len(batches) - 1:
                await asyncio.sleep(5)

        logger.info(f"Successfully processed and stored {EVENT_NAME} for {total_processed} users")

    except Exception as e:
        logger.error(f"Error in process_user_multiplier_events: {str(e)}")
        logger.exception("Exception details:")
        raise


if __name__ == "__main__":
    asyncio.run(process_user_multiplier_events())