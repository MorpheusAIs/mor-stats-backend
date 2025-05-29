import logging
from datetime import datetime
from psycopg2.extras import execute_values

from app.core.config import ETH_RPC_URL, distribution_contract
from app.db.database import get_db
from app.models.database_models import UserClaimLocked
from app.repository import UserClaimLockedRepository
from app.web3.web3_wrapper import Web3Provider
from helpers.database_helpers.db_helper import get_last_block_from_db
from helpers.web3_helper import get_events_in_batches, get_event_headers

logger = logging.getLogger(__name__)

START_BLOCK = 20180927
BATCH_SIZE = 1000000
TABLE_NAME = "user_claim_locked"
EVENT_NAME = "UserClaimLocked"

RPC_URL = ETH_RPC_URL
web3 = Web3Provider.get_instance()
contract = distribution_contract


def insert_user_claim_locked_events(user_claim_locked_events: list[UserClaimLocked]):
    try:
        repository = UserClaimLockedRepository()
        repository.bulk_insert(user_claim_locked_events)
    except Exception as e:
        logger.error(f"Error inserting events to database: {str(e)}")
        raise

def process_user_claim_locked_events():
    try:
        latest_block = web3.eth.get_block('latest')['number']
        last_processed_block = get_last_block_from_db(TABLE_NAME)

        if last_processed_block is None:
            start_block = START_BLOCK
        else:
            start_block = last_processed_block + 1

        events = list(get_events_in_batches(start_block, latest_block, EVENT_NAME, BATCH_SIZE))
        logger.info(f"Processing {len(events)} new {EVENT_NAME} events from block {start_block} to {latest_block}")

        if events:
            user_claim_locked_events: list[UserClaimLocked] = []
            for event in events:
                user_claim_locked = UserClaimLocked(
                    id = None,
                    timestamp = datetime.fromtimestamp(web3.eth.get_block(event['blockNumber'])['timestamp']),
                    transaction_hash = event['transactionHash'].hex(),
                    block_number = event['blockNumber'],
                    user = event['user'],
                    pool_id = event['poolId'],
                    claim_lock_start = event['claimLockStart'],
                    claim_lock_end = event['claimLockEnd']
                )

                user_claim_locked_events.append(user_claim_locked)

            insert_user_claim_locked_events(user_claim_locked_events)

            logger.info(f"Successfully processed and stored new events for {EVENT_NAME}")
        else:
            logger.info(f"No new events found for {EVENT_NAME}.")

    except Exception as e:
        logger.error(f"An error occurred in process_events: {str(e)}")
        logger.exception("Exception details:")


if __name__ == "__main__":
    process_user_claim_locked_events()