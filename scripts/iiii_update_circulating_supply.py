import os
from datetime import datetime
from web3.exceptions import BlockNotFound
from configuration.config import web3, distribution_contract
from sheet_config.google_utils import (
    download_sheet, append_new_data, clear_and_upload_new_records, slack_notification)
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CIRC_SUPPLY_SHEET_NAME = "CircSupply"


def get_block_number_by_timestamp(timestamp):
    """Binary search to find the block number closest to the given timestamp."""
    left = 1
    right = web3.eth.get_block('latest')['number']

    while left <= right:
        mid = (left + right) // 2
        try:
            mid_block = web3.eth.get_block(mid)
            if mid_block['timestamp'] == timestamp:
                return mid
            elif mid_block['timestamp'] < timestamp:
                left = mid + 1
            else:
                right = mid - 1
        except BlockNotFound:
            right = mid - 1

    return left  # Return the closest block number


def update_circulating_supply_sheet():
    try:
        # Download the existing sheet data
        existing_csv = download_sheet(CIRC_SUPPLY_SHEET_NAME)

        df = pd.read_csv(existing_csv)

        if df.empty:
            logger.error("Error: Sheet is empty")
            return

        # Convert date strings to datetime objects
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

        # Sort by date and get the latest record
        df = df.sort_values('date', ascending=False)
        latest_record = df.iloc[0]
        latest_block_timestamp = int(latest_record['block_timestamp_at_that_date'])
        latest_circulating_supply = float(latest_record['circulating_supply_at_that_date'])

        # Find the block number for the latest timestamp
        start_block = get_block_number_by_timestamp(latest_block_timestamp)
        start_block += 1

        # Create a filter for UserClaimed events from the latest block timestamp
        event_filter = distribution_contract.events.UserClaimed.create_filter(
            from_block=start_block,
            to_block='latest'
        )

        # Fetch all new events
        events = event_filter.get_all_entries()

        # Process new events
        new_data = []
        for event in events:
            try:
                block_number = event['blockNumber']
                block = web3.eth.get_block(block_number)
                timestamp = block['timestamp']
                date_str = datetime.utcfromtimestamp(timestamp).strftime('%d/%m/%Y')

                amount = float(event['args']['amount']) / 10 ** 18
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
            # Append new data to the existing CSV
            output_csv = 'updated_circ_supply.csv'
            updated_csv = append_new_data(existing_csv, new_data, output_csv)

            # Read the updated CSV, sort, and remove duplicates
            updated_df = pd.read_csv(updated_csv)
            updated_df['date'] = pd.to_datetime(updated_df['date'], format='%d/%m/%Y')
            updated_df = updated_df.sort_values('date', ascending=False)
            updated_df = updated_df.drop_duplicates(subset=['date'], keep='first')
            updated_df['date'] = updated_df['date'].dt.strftime('%d/%m/%Y')

            # Write the final data back to CSV
            updated_df.to_csv(output_csv, index=False)

            # Upload the updated data to Google Sheets
            clear_and_upload_new_records(CIRC_SUPPLY_SHEET_NAME, output_csv)

            logger.info(f"Updated {CIRC_SUPPLY_SHEET_NAME}. Total records: {len(updated_df)}")
            slack_notification(f"Circulating Supply updated. Total records: {len(updated_df)}")

            # Clean up temporary files
            os.remove(existing_csv)
            os.remove(output_csv)

            return len(updated_df)  # Return number of records as a success indicator
        else:
            logger.info("No new data to update.")
            slack_notification("No new Circulating Supply data to update.")
            os.remove(existing_csv)
            return len(df)

    except Exception as e:
        logger.error(f"Error updating circulating supply sheet: {e}")
        slack_notification(f"Error updating Circulating Supply: {str(e)}")
        return None


# Function to be called by cron job
# def cron_update_circulating_supply():
#     update_circulating_supply_sheet()
#
#
# if __name__ == "__main__":
#     cron_update_circulating_supply()