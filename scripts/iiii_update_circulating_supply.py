import logging
from datetime import datetime
from decimal import Decimal
from web3.exceptions import BlockNotFound

from app.core.config import distribution_contract
from app.db.database import get_db
from app.models.database_models import CirculatingSupply
from app.repository import CirculatingSupplyRepository
from app.web3.web3_wrapper import Web3Provider

logger = logging.getLogger(__name__)

TABLE_NAME = "circulating_supply"
EVENT_NAME = "CirculatingSupply"

web3 = Web3Provider.get_instance()

def get_latest_circulating_supply_record():
    """Get the latest circulating supply record from the database using the repository"""
    try:
        repository = CirculatingSupplyRepository()
        latest_record = repository.get_latest()
        
        if latest_record:
            return {
                'date': latest_record.date,
                'circulating_supply_at_that_date': latest_record.circulating_supply_at_that_date,
                'block_timestamp_at_that_date': latest_record.block_timestamp_at_that_date
            }
        else:
            logger.error("No records found in the database")
            return None
    except Exception as e:
        logger.error(f"Error getting latest record: {str(e)}")
        raise

def insert_circulating_supply_events(new_data):
    """Save new circulating supply data to the database using the repository"""
    if not new_data:
        return 0

    try:
        repository = CirculatingSupplyRepository()
        total_count = repository.save_new_supply_data(new_data)
        
        logger.info(f"Saved {len(new_data)} new {EVENT_NAME} records. Total records: {total_count}")
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


def process_circulating_supply_events():
    try:
        # Get the latest record from the database
        latest_record = get_latest_circulating_supply_record()
        if not latest_record:
            logger.error(f"Cannot update: No baseline record found in the database for {EVENT_NAME}")
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
            total_count = insert_circulating_supply_events(aggregated_data)

            return total_count
        else:
            logger.info("No new data to update.")
            return None

    except Exception as e:
        logger.error(f"Error processing {EVENT_NAME} events: {str(e)}")
        return None

if __name__ == "__main__":
    process_circulating_supply_events()