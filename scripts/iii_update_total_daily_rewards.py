import asyncio
import logging
import time
import datetime
from decimal import Decimal
from web3 import AsyncWeb3

from app.core.config import ETH_RPC_URL, distribution_contract
from app.db.database import get_db
from app.models.database_models import RewardSummary, UserMultiplier
from app.repository import RewardSummaryRepository, UserMultiplierRepository
from helpers.web3_helper import get_block_by_timestamp

logger = logging.getLogger(__name__)

MAX_RETRIES = 10
RETRY_DELAY = 10  # seconds
BATCH_SIZE = 50

INPUT_TABLE_NAME = "user_multiplier"
TABLE_NAME = "reward_summary"
EVENT_NAME = "RewardSummary"

RPC_URL = ETH_RPC_URL
w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_URL))
contract = w3.eth.contract(address=distribution_contract.address, abi=distribution_contract.abi)

def get_user_reward_data() -> list:
    """Get user and pool data from the user_multiplier table using the repository"""
    try:
        # Use the repository to get the data
        repository = UserMultiplierRepository()
        results = repository.get_all_user_multipliers_grouped()

        logger.info(f"Retrieved {len(results)} unique user/pool combinations")
        return results
    except Exception as e:
        logger.error(f"Error getting user reward data: {str(e)}")
        raise


async def get_useful_blocks():
    current_unix_timestamp = int(time.time())
    timestamp_24_hours_ago = current_unix_timestamp - (24 * 60 * 60)
    block_24_hours_ago = await get_block_by_timestamp(timestamp_24_hours_ago)
    return block_24_hours_ago, 'latest'


async def get_user_reward_at_block(pool_id, address, block):
    for attempt in range(MAX_RETRIES):
        try:
            reward = await contract.functions.getCurrentUserReward(pool_id, address).call(block_identifier=block)
            return Decimal(w3.from_wei(reward, 'ether'))
        except Exception as e:
            if 'Too Many Requests' in str(e) and attempt < MAX_RETRIES - 1:
                logger.warning(f"Rate limit hit, retrying in {RETRY_DELAY * (attempt + 1)} seconds...")
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"Error getting reward for {address} in pool {pool_id} at block {block}: {str(e)}")
                return Decimal('0')
    raise


async def process_rewards_batch(batch : list, block_24_hours_ago, block_right_now) -> list:
    tasks = []
    for record in batch:
        address = w3.to_checksum_address(record['user_address'])
        tasks.append(get_user_reward_at_block(record['pool_id'], address, block_right_now))
        tasks.append(get_user_reward_at_block(record['pool_id'], address, block_24_hours_ago))

    results = await asyncio.gather(*tasks)

    rewards_data = []
    for i in range(0, len(results), 2):
        current_reward = results[i]
        past_reward = results[i + 1]
        daily_reward = current_reward - past_reward

        # Calculate the index in the batch
        batch_index = i // 2

        rewards_data.append({
            'user_address': batch[batch_index]['user_address'],
            'pool_id': int(batch[batch_index]['pool_id']),
            'daily_reward': daily_reward,
            'total_reward': current_reward
        })

    return rewards_data

def insert_reward_summary_events(summary_data, block_24_hours_ago, latest_block) -> int:
    """Save the reward summary data to the database using the repository"""
    try:
        repository = RewardSummaryRepository()
        current_time = datetime.datetime.now()

        reward_summary = RewardSummary(
            id=None,
            timestamp=current_time,
            calculation_block_current=latest_block,
            calculation_block_past=block_24_hours_ago,
            daily_pool_reward_0=summary_data['Daily Pool 0'],
            daily_pool_reward_1=summary_data['Daily Pool 1'],
            daily_reward=summary_data['Daily Combined'],
            total_reward_pool_0=summary_data['Total Pool 0'],
            total_reward_pool_1=summary_data['Total Pool 1'],
            total_reward=summary_data['Total Combined'],
        )
        # Insert the events and get the ID of the first one
        result = repository.create(reward_summary)
        
        logger.info(f"Saved reward summary id {result.id}")
        return result.id
    except Exception as e:
        logger.error(f"Error saving reward summary: {str(e)}")
        raise

async def process_reward_events():
    try:
        # Fetch user data from database
        users = get_user_reward_data()

        if not users:
            logger.error(f"No users found in the database for {EVENT_NAME}")
            raise RuntimeError(f"No users found in the database for {EVENT_NAME}")

        # Get blocks for calculation
        block_24_hours_ago, block_right_now = await get_useful_blocks()
        latest_block = await w3.eth.get_block_number() if block_right_now == 'latest' else block_right_now

        logger.info(f"Processing {EVENT_NAME} between blocks {block_24_hours_ago} and {latest_block}")

        # Split users into batches
        batches = [users[i:i + BATCH_SIZE] for i in range(0, len(users), BATCH_SIZE)]

        # Process all users
        daily_pool_0_sum = Decimal('0')
        daily_pool_1_sum = Decimal('0')
        total_pool_0_sum = Decimal('0')
        total_pool_1_sum = Decimal('0')

        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i + 1}/{len(batches)}")
            reward_details = await process_rewards_batch(batch, block_24_hours_ago, block_right_now)

            # Aggregate reward data
            for detail in reward_details:
                if detail['pool_id'] == 0:
                    daily_pool_0_sum += detail['daily_reward']
                    total_pool_0_sum += detail['total_reward']
                elif detail['pool_id'] == 1:
                    daily_pool_1_sum += detail['daily_reward']
                    total_pool_1_sum += detail['total_reward']

            logger.info(f"Completed batch {i + 1}/{len(batches)}")

            # Add delay between batches to prevent rate limiting
            if i < len(batches) - 1:
                await asyncio.sleep(1)

        # Calculate combined totals
        daily_combined_sum = daily_pool_0_sum + daily_pool_1_sum
        total_combined_sum = total_pool_0_sum + total_pool_1_sum

        # Prepare summary data
        summary_data = {
            'Daily Pool 0': daily_pool_0_sum,
            'Daily Pool 1': daily_pool_1_sum,
            'Daily Combined': daily_combined_sum,
            'Total Pool 0': total_pool_0_sum,
            'Total Pool 1': total_pool_1_sum,
            'Total Combined': total_combined_sum
        }

        # Save summary to database
        insert_reward_summary_events(summary_data, block_24_hours_ago, latest_block)

        logger.info(f"Successfully processed and stored {EVENT_NAME}")

    except Exception as e:
        logger.error(f"Error in process_reward_events: {str(e)}")
        logger.exception("Exception details:")
        raise


if __name__ == "__main__":
    asyncio.run(process_reward_events())