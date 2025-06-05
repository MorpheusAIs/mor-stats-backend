import asyncio
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Tuple, List, Dict
import httpx
import pandas as pd
import requests
from dune_client.client import DuneClient
from dune_client.models import DuneError

from app.core.config import logger
from app.core.config import (supply_contract, distribution_contract,
                             MAINNET_BLOCK_1ST_JAN_2024, DEXSCREENER_URL, COINGECKO_HISTORICAL_PRICES,
                             DUNE_API_KEY, DUNE_QUERY_ID, IMPLIED_PRICES_JSON)
from app.repository.circulating_supply_repository import CirculatingSupplyRepository
from helpers.supply_helpers.get_burnt_and_locked_arbitrum import get_locked_amounts, get_burned_amounts
from helpers.supply_helpers.get_historical_total_supply import get_total_supply_from_emissions_df
from helpers.staking_helpers.get_emission_schedule_for_today import get_historical_emissions

async def get_combined_supply_data():
    try:
        # Step 1: Retrieve the updated total supply data
        total_supply_data = get_total_supply_from_emissions_df()

        # Step 2: Get the earliest date from the total supply data
        earliest_total_supply_date = min(
            total_supply_data.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y')
        )

        # Step 3: Read circulating supply data from the CSV file
        circulating_supply_data = get_historical_circulating_supply(earliest_total_supply_date)
        total_emissions_data = get_historical_emissions()

        # Step 4: Combine total supply, circulating supply, and emissions data into the desired format
        combined_data = []
        last_known_circulating_supply = 0.0

        # Sort dates to ensure we process them in chronological order
        sorted_dates = sorted(total_supply_data.keys(),
                              key=lambda x: datetime.strptime(x, '%d/%m/%Y'))

        for date in sorted_dates:
            total_supply = total_supply_data[date]

            # If we have new circulating supply data, update the last known value
            if date in circulating_supply_data:
                last_known_circulating_supply = circulating_supply_data[date]['circulating_supply']
                claimed_that_day = circulating_supply_data[date]['total_claimed_that_day']
            else:
                claimed_that_day = 0.0  # No new claims that day

            data_point = {
                "date": date,
                "total_supply": total_supply,
                "circulating_supply": last_known_circulating_supply,
                "total_claimed_that_day": claimed_that_day
            }

            if date in total_emissions_data:
                data_point["total_emission"] = total_emissions_data[date]

            combined_data.append(data_point)

        # Sort the combined data by date in descending order for display
        combined_data.sort(key=lambda x: datetime.strptime(x['date'], '%d/%m/%Y'), reverse=True)

        return combined_data

    except Exception as e:
        logger.info(f"Error occurred while fetching data: {str(e)}")
        return [{}]


def get_historical_circulating_supply(earliest_date: str) -> dict:
    try:
        # Convert earliest_date string to datetime object
        earliest_date_obj = datetime.strptime(earliest_date, '%d/%m/%Y').date()
        
        # Use the repository to get data from the database
        repo = CirculatingSupplyRepository()
        records = repo.get_by_date_range(earliest_date_obj, datetime.now().date())
        
        # Convert to the required format
        circulating_supply_data = {}
        for record in records:
            date_str = record.date.strftime('%d/%m/%Y')
            circulating_supply_data[date_str] = {
                "circulating_supply": float(record.circulating_supply_at_that_date),
                "total_claimed_that_day": float(record.total_claimed_that_day)
            }
        
        return circulating_supply_data

    except Exception as e:
        logger.error(f"An error occurred while fetching circulating supply data: {str(e)}")
        return {}


async def get_historical_prices_and_trading_volume():
    with open(IMPLIED_PRICES_JSON, 'r') as file:
        json_data = json.load(file)

    json_prices = {datetime.strptime(item[0], '%d/%m/%Y'): item[1] for item in json_data['prices']}
    json_volumes = {datetime.strptime(item[0], '%d/%m/%Y'): item[1] for item in json_data['total_volumes']}

    # Fetch data from API
    async with httpx.AsyncClient() as client:
        response = await client.get(COINGECKO_HISTORICAL_PRICES)
        api_data = response.json()

    def process_data(api_points: List[Tuple[int, float]], json_points: Dict[datetime, float]) -> List[List]:
        aggregated_data = defaultdict(list)

        # Process API data
        for timestamp, value in api_points:
            date = datetime.utcfromtimestamp(timestamp / 1000)
            aggregated_data[date].append(value)

        # Process JSON data
        for date, value in json_points.items():
            if date not in aggregated_data:
                aggregated_data[date] = [value]

        # Average the values for each day
        averaged_data = []
        for date, values in aggregated_data.items():
            averaged_value = round((sum(values) / len(values)), 4)
            averaged_data.append([date.strftime('%d/%m/%Y'), averaged_value])

        # Sort the data with the latest date at the top
        averaged_data.sort(key=lambda x: datetime.strptime(x[0], '%d/%m/%Y'), reverse=True)

        return averaged_data

    # Process prices and total_volumes
    sorted_prices = process_data(api_data['prices'], json_prices)
    sorted_volumes = process_data(api_data['total_volumes'], json_volumes)

    # Create JSON structures
    prices_data = {"prices": sorted_prices}
    volume_data = {"total_volumes": sorted_volumes}

    combined_prices_volume_data = {
        "prices": prices_data["prices"],
        "total_volumes": volume_data["total_volumes"]
    }

    return combined_prices_volume_data


async def get_current_total_supply() -> float:
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        total_supply = await loop.run_in_executor(pool,
                                                  supply_contract.functions.getTotalRewards().call)

        return round((total_supply / 10 ** 18), 4)


async def get_current_circulating_supply() -> float:
    circulating_supply = 0

    event_filter = distribution_contract.events.UserClaimed.create_filter(
        from_block=MAINNET_BLOCK_1ST_JAN_2024,
        to_block='latest',
    )

    events = event_filter.get_all_entries()

    for event in events:
        amount = float(event['args']['amount']) / pow(10, 18)
        circulating_supply += amount

    return round(circulating_supply, 4)


async def get_current_mor_price() -> float:
    response = requests.get(DEXSCREENER_URL)

    if response.status_code == 200:
        data = response.json()
        mor_price = float(data['pairs'][0]['priceUsd'])
    else:
        mor_price = 0.0
    return mor_price


async def get_historical_locked_and_burnt_mor():
    burnt_mor = await get_burned_amounts()
    locked_mor = await get_locked_amounts()

    burnt_mor_data = json.loads(burnt_mor)
    locked_mor_data = json.loads(locked_mor)

    locked_and_burnt_mor = {
        "burnt_mor": {
            "cumulative_mor_burnt": burnt_mor_data["cumulative_mor_burnt"],
            "total_burnt_till_now": burnt_mor_data["total_burnt_till_now"]
        },
        "locked_mor": {
            "cumulative_mor_locked": locked_mor_data["cumulative_mor_locked"],
            "total_locked_till_now": locked_mor_data["total_locked_till_now"]
        }
    }

    return locked_and_burnt_mor


async def get_market_cap():
    try:
        locked_and_burnt_mor = await get_historical_locked_and_burnt_mor()

        # Ensure we're working with dictionaries
        if not isinstance(locked_and_burnt_mor, dict):
            logger.error(f"Unexpected type for locked_and_burnt_mor: {type(locked_and_burnt_mor)}")
            return {"error": "Unexpected data format"}

        burnt_json = locked_and_burnt_mor.get('burnt_mor', {})
        locked_json = locked_and_burnt_mor.get('locked_mor', {})

        # Safely get the values, providing default values if keys don't exist
        total_burnt_amount = float(burnt_json.get('total_burnt_till_now', 0))
        total_locked_amount = float(locked_json.get('total_locked_till_now', 0))

        total_burnt_and_locked = total_burnt_amount + total_locked_amount

        current_mor_price = await get_current_mor_price()
        current_circulating_supply = await get_current_circulating_supply()
        current_total_supply = await get_current_total_supply()

        total_supply_market_cap = (current_total_supply - total_burnt_and_locked) * current_mor_price
        circulating_supply_market_cap = (current_circulating_supply - total_burnt_and_locked) * current_mor_price

        total_supply_market_cap = round(total_supply_market_cap, 4)
        circulating_supply_market_cap = round(circulating_supply_market_cap, 4)

        market_caps = {
            "total_supply_market_cap": total_supply_market_cap,
            "circulating_supply_market_cap": circulating_supply_market_cap
        }

        return market_caps

    except Exception as e:
        logger.exception(f"An error occurred in get_market_cap: {str(e)}")
        return {"error": str(e)}


async def get_mor_holders():
    try:
        dune = DuneClient(
            api_key=DUNE_API_KEY,
            base_url="https://api.dune.com",
            request_timeout=300
        )
        token_holders = dune.get_latest_result(DUNE_QUERY_ID)
        holders_data = token_holders.result.rows

        ranges = [
            {"range": "0-10", "min": 0, "max": 10},
            {"range": "10-25", "min": 10, "max": 25},
            {"range": "25-50", "min": 25, "max": 50},
            {"range": "50-100", "min": 50, "max": 100},
            {"range": "100-200", "min": 100, "max": 200},
            {"range": "200-500", "min": 200, "max": 500},
            {"range": "500-1000", "min": 500, "max": 1000},
            {"range": "1000-10000", "min": 1000, "max": 10000},
            {"range": "10000-500000", "min": 10000, "max": 500000}
        ]

        def get_range_counts():
            return {r['range']: 0 for r in ranges}

        result = {
            "total": get_range_counts(),
            "Arbitrum": get_range_counts(),
            "Base": get_range_counts(),
            "Ethereum": get_range_counts()
        }

        for holder in holders_data:
            if holder['address'] == "0x0000000000000000000000000000000000000000":
                continue

            amount = float(holder['amount'])
            if amount <= 0.001:
                continue

            chain = holder['chain']

            for r in ranges:
                if r['min'] <= amount < r['max']:
                    result["total"][r['range']] += 1
                    result[chain][r['range']] += 1
                    break

        return result

    except DuneError as e:
        error_message = f"API Limit hit for Dune. Please rotate environment variables. Error: {str(e)}"
        logger.exception(f"An error occurred in get_mor_holders: {str(e)}")
        slack_notification(error_message)
        return None
    except Exception as e:
        logger.exception(f"An error occurred in get_mor_holders: {str(e)}")
        return None
