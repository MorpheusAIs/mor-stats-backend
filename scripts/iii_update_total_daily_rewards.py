import asyncio
import logging
import pandas as pd
import time
import aiohttp
import os
from web3 import AsyncWeb3
from configuration.config import (
    ETH_RPC_URL, distribution_contract, ETHERSCAN_API_KEY, SPREADSHEET_ID
)
from sheet_config.google_utils import (
    download_sheet, clear_and_upload_new_records, slack_notification
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BATCH_SIZE = 50

INPUT_SHEET_NAME = "UserMultiplier"
OUTPUT_SHEET_NAME = "RewardSum"

RPC_URL = ETH_RPC_URL
w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_URL))
contract = w3.eth.contract(address=distribution_contract.address, abi=distribution_contract.abi)


async def get_block_by_timestamp(timestamp):
    url = (f"https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp="
           f"{timestamp}&closest=before"
           f"&apikey={ETHERSCAN_API_KEY}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            return int(data['result'])


async def get_useful_blocks():
    current_unix_timestamp = int(time.time())
    timestamp_24_hours_ago = current_unix_timestamp - (24 * 60 * 60)
    block_24_hours_ago = await get_block_by_timestamp(timestamp_24_hours_ago)
    return block_24_hours_ago, 'latest'


async def get_user_reward_at_block(pool_id, address, block):
    for attempt in range(MAX_RETRIES):
        try:
            reward = await contract.functions.getCurrentUserReward(pool_id, address).call(block_identifier=block)
            return float(w3.from_wei(reward, 'ether'))
        except Exception as e:
            if 'Too Many Requests' in str(e) and attempt < MAX_RETRIES - 1:
                logger.warning(f"Rate limit hit, retrying in {RETRY_DELAY} seconds...")
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"Error getting reward for {address} in pool {pool_id} at block {block}: {str(e)}")
                return 0


async def process_rewards_batch(batch, block_24_hours_ago, block_right_now):
    tasks = []
    for _, row in batch.iterrows():
        pool_id = int(row['poolId'])
        address = w3.to_checksum_address(row['user'])
        tasks.append(get_user_reward_at_block(pool_id, address, block_right_now))
        tasks.append(get_user_reward_at_block(pool_id, address, block_24_hours_ago))

    results = await asyncio.gather(*tasks)

    daily_rewards = [results[i] - results[i + 1] for i in range(0, len(results), 2)]
    total_rewards = results[::2]

    return daily_rewards, total_rewards


async def calculate_rewards():
    try:
        input_csv = download_sheet(INPUT_SHEET_NAME)
        df = pd.read_csv(input_csv)
        df = df.drop_duplicates(subset=['user', 'poolId'], keep='first')

        block_24_hours_ago, block_right_now = await get_useful_blocks()

        batches = [df[i:i + BATCH_SIZE] for i in range(0, df.shape[0], BATCH_SIZE)]

        daily_pool_0_sum = daily_pool_1_sum = total_pool_0_sum = total_pool_1_sum = 0

        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i + 1}/{len(batches)}")
            daily_rewards, total_rewards = await process_rewards_batch(batch, block_24_hours_ago, block_right_now)

            for j, (daily_reward, total_reward) in enumerate(zip(daily_rewards, total_rewards)):
                pool_id = int(batch.iloc[j]['poolId'])
                if pool_id == 0:
                    daily_pool_0_sum += daily_reward
                    total_pool_0_sum += total_reward
                elif pool_id == 1:
                    daily_pool_1_sum += daily_reward
                    total_pool_1_sum += total_reward

            logger.info(f"Completed batch {i + 1}/{len(batches)}")
            await asyncio.sleep(1)

        daily_combined_sum = daily_pool_0_sum + daily_pool_1_sum
        total_combined_sum = total_pool_0_sum + total_pool_1_sum

        result_df = pd.DataFrame({
            'Category': ['Daily Pool 0', 'Daily Pool 1', 'Daily Combined', 'Total Pool 0', 'Total Pool 1',
                         'Total Combined'],
            'Value': [daily_pool_0_sum, daily_pool_1_sum, daily_combined_sum, total_pool_0_sum, total_pool_1_sum,
                      total_combined_sum]
        })

        output_csv = f'updated_{OUTPUT_SHEET_NAME}.csv'
        result_df.to_csv(output_csv, index=False)

        clear_and_upload_new_records(OUTPUT_SHEET_NAME, output_csv)

        logger.info(f"Successfully calculated and uploaded reward sums")
        slack_notification(f"Successfully calculated and uploaded reward sums to {OUTPUT_SHEET_NAME}!"
                           f" Link to file: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/")

    except Exception as e:
        logger.error(f"Error in calculate_rewards: {str(e)}")
        slack_notification(f"Error in calculating rewards: {str(e)}")
        raise
    finally:
        if 'input_csv' in locals() and os.path.exists(input_csv):
            os.remove(input_csv)
        if 'output_csv' in locals() and os.path.exists(output_csv):
            os.remove(output_csv)



if __name__ == "__main__":
    asyncio.run(calculate_rewards())
