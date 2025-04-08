import asyncio
import logging
import os
import pandas as pd
from dateutil import parser
from web3 import AsyncWeb3
from decimal import Decimal
from configuration.config import ETH_RPC_URL, distribution_contract, SPREADSHEET_ID
from sheet_config.google_utils import (
    download_sheet, clear_and_upload_new_records, slack_notification
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

INPUT_SHEET_NAME = "UserClaimLocked"
OUTPUT_SHEET_NAME = "UserMultiplier"

RPC_URL = ETH_RPC_URL
w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(ETH_RPC_URL))
contract = w3.eth.contract(address=distribution_contract.address, abi=distribution_contract.abi)


async def get_block_number(timestamp):
    latest_block = await w3.eth.get_block('latest', full_transactions=False)
    return latest_block['number']


async def get_multiplier(row):
    for attempt in range(MAX_RETRIES):
        try:
            pool_id = int(row['poolId'])
            user = w3.to_checksum_address(row['user'])
            timestamp = parser.parse(row['Timestamp'])
            block_number = await get_block_number(timestamp)
            multiplier = await contract.functions.getCurrentUserMultiplier(pool_id, user).call(
                block_identifier=block_number)
            return multiplier
        except Exception as e:
            if 'Too Many Requests' in str(e) and attempt < MAX_RETRIES - 1:
                logger.warning(f"Rate limit hit, retrying in {RETRY_DELAY} seconds...")
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"Error processing row {row}: {str(e)}")
                return None


async def process_batch(batch):
    tasks = [get_multiplier(row) for _, row in batch.iterrows()]
    return await asyncio.gather(*tasks)


def format_multiplier(value):
    if value is None:
        return "Error"
    # Convert to Decimal for precise handling of large numbers
    decimal_value = Decimal(str(value))
    # Format as a string with no decimal places
    return f"{decimal_value:.0f}"


async def calculate_user_multipliers():
    input_csv = None
    output_csv = None
    try:
        input_csv = download_sheet(INPUT_SHEET_NAME)
        df = pd.read_csv(input_csv)

        batch_size = 50
        batches = [df[i:i + batch_size] for i in range(0, df.shape[0], batch_size)]

        all_multipliers = []
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i + 1}/{len(batches)}")
            multipliers = await process_batch(batch)
            all_multipliers.extend(multipliers)
            logger.info(f"Completed batch {i + 1}/{len(batches)}")
            await asyncio.sleep(1)

        df['multiplier'] = [format_multiplier(m) for m in all_multipliers]

        output_csv = f'updated_{OUTPUT_SHEET_NAME}.csv'
        df.to_csv(output_csv, index=False)

        clear_and_upload_new_records(OUTPUT_SHEET_NAME, output_csv)

        logger.info(f"Successfully calculated and uploaded user multipliers")
        slack_notification(f"Successfully calculated and uploaded user multipliers to {OUTPUT_SHEET_NAME}!"
                           f" Link to file: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/")

    except Exception as e:
        logger.error(f"Error in calculate_user_multipliers: {str(e)}")
        slack_notification(f"Error in calculating user multipliers: {str(e)}")
        raise
    finally:
        for file in [input_csv, output_csv]:
            if file and os.path.exists(file):
                os.remove(file)
                logger.info(f"Removed temporary file: {file}")


# if __name__ == "__main__":
#     asyncio.run(calculate_user_multipliers())
