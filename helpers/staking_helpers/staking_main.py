import logging
from collections import defaultdict
from datetime import datetime, date, timedelta
from decimal import Decimal
import numpy as np
import pandas as pd
import requests
from app.core.config import (distribution_contract, USER_MULTIPLIER_SHEET_NAME, REWARD_SUM_SHEET_NAME)
from app.repository import UserMultiplierRepository, RewardSummaryRepository
from helpers.staking_helpers.get_emission_schedule_for_today import read_emission_schedule

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_repository_data_as_dataframe(repository_class, table_name):
    """
    Get data from a repository and convert it to a DataFrame.
    
    Args:
        repository_class: The repository class to use
        table_name: The table name (for logging purposes)
        
    Returns:
        DataFrame with the repository data
    """
    try:
        # Create repository instance
        repo = repository_class()
        
        # Get all records (with a high limit to ensure we get everything)
        records = repo.get_all(limit=100000)
        
        # Convert to DataFrame
        if records:
            # Convert Pydantic models to dictionaries
            data = [record.model_dump() for record in records]
            df = pd.DataFrame(data)
            logger.info(f"Successfully loaded {len(df)} records from {table_name}")
            return df
        else:
            logger.warning(f"No records found in {table_name}")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading from {table_name}: {str(e)}")
        return pd.DataFrame()


def get_dataframe_from_sheet_name(sheet_name):
    """
    Get a DataFrame from a repository based on the sheet name.
    
    Args:
        sheet_name: The name of the sheet (used to determine which repository to use)
        
    Returns:
        DataFrame with the repository data
    """
    if sheet_name == USER_MULTIPLIER_SHEET_NAME:
        df = get_repository_data_as_dataframe(UserMultiplierRepository, "user_multiplier")
        
        # Rename columns to match the expected format
        if not df.empty:
            df = df.rename(columns={
                'timestamp': 'Timestamp',
                'transaction_hash': 'TransactionHash',
                'block_number': 'BlockNumber',
                'pool_id': 'poolId',
                'user_address': 'user',
                'multiplier': 'multiplier',
                'error_message': 'errorMessage'
            })
            
            # Add missing columns that might be expected
            if 'claimLockStart' not in df.columns:
                df['claimLockStart'] = 0
            if 'claimLockEnd' not in df.columns:
                df['claimLockEnd'] = 0
                
            # Try to get claim lock data from the blockchain for each user
            try:
                for i, row in df.iterrows():
                    user = row['user']
                    pool_id = row['poolId']
                    try:
                        user_data = distribution_contract.functions.usersData(user, pool_id).call()
                        df.at[i, 'claimLockStart'] = user_data[2]  # Assuming index 2 is claimLockStart
                        df.at[i, 'claimLockEnd'] = user_data[3]    # Assuming index 3 is claimLockEnd
                    except Exception as e:
                        logger.warning(f"Could not get claim lock data for user {user} in pool {pool_id}: {str(e)}")
            except Exception as e:
                logger.error(f"Error getting claim lock data: {str(e)}")
                
        return df
    elif sheet_name == REWARD_SUM_SHEET_NAME:
        df = get_repository_data_as_dataframe(RewardSummaryRepository, "reward_summary")
        
        # Rename columns to match the expected format
        if not df.empty:
            df = df.rename(columns={
                'category': 'Category',
                'value': 'Value'
            })
            
        return df
    else:
        # For other sheets, we might still need to use the original function
        # This is a placeholder for EMISSIONS_SHEET_NAME which might still be read from a sheet
        logger.warning(f"No repository mapping for sheet {sheet_name}, using original function")
        return None  # Return None to indicate that we should use the original function


def get_todays_capital_emission():
    mor_daily_emission = 0

    today = datetime.today()
    emissions_data = read_emission_schedule(today)

    mor_daily_emission = float(emissions_data['new_emissions']['Capital Emission'])

    return mor_daily_emission


def get_crypto_price(crypto_id):
    base_url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": crypto_id,
        "vs_currencies": "usd"
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()

        if crypto_id in data and "usd" in data[crypto_id]:
            return data[crypto_id]["usd"]
        else:
            return None
    except requests.RequestException as e:
        logger.info(f"An error occurred: {e}")
        return None


def is_valid_stake(row):
    current_time: int = int(datetime.now().timestamp())
    claim_lock_start = int(row['claimLockStart'])
    claim_lock_end = int(row['claimLockEnd'])
    twenty_years_from_now = current_time + (25 * 365 * 24 * 60 * 60)  # 25 years in seconds

    return claim_lock_start != 0 and claim_lock_end != 0 and current_time < claim_lock_end <= twenty_years_from_now


def analyze_mor_stakers():
    df = get_dataframe_from_sheet_name(USER_MULTIPLIER_SHEET_NAME)

    stakers_by_pool = {0: set(), 1: set()}
    stakers_by_pool_and_date = defaultdict(lambda: defaultdict(set))
    total_stake_time = {0: timedelta(), 1: timedelta()}
    stake_count = {0: 0, 1: 0}

    mor_price = get_crypto_price("morpheusai")
    eth_price = get_crypto_price("staked-ether")
    prices = {"MOR": mor_price, "stETH": eth_price}

    try:
        for _, row in df.iterrows():
            if not is_valid_stake(row):
                continue

            timestamp = pd.to_datetime(row['Timestamp']).date()
            pool_id = int(row['poolId'])
            user = row['user']

            # Add to overall pool stakers
            stakers_by_pool[pool_id].add(user)

            # Add to date-specific pool stakers
            stakers_by_pool_and_date[timestamp][pool_id].add(user)

            # Calculate stake time
            claim_lock_start = int(row['claimLockStart'])
            claim_lock_end = int(row['claimLockEnd'])
            stake_time = timedelta(seconds=claim_lock_end - claim_lock_start)

            # Add to total stake time
            total_stake_time[pool_id] += stake_time
            stake_count[pool_id] += 1

        logger.info("Successfully analyzed MOR stakers from DataFrame")

        # Calculate average stake time
        avg_stake_time = {
            pool_id: (total_time / count if count > 0 else timedelta())
            for pool_id, total_time in total_stake_time.items()
            for count in [stake_count[pool_id]]
        }

        # Calculate combined average stake time
        total_combined_stake_time = sum(total_stake_time.values(), timedelta())
        total_combined_stakes = sum(stake_count.values())
        combined_avg_stake_time = total_combined_stake_time / total_combined_stakes \
            if total_combined_stakes > 0 else timedelta()

        # Prepare results
        results = {
            'total_unique_stakers': {
                'pool_0': len(stakers_by_pool[0]),
                'pool_1': len(stakers_by_pool[1]),
                'combined': len(stakers_by_pool[0] | stakers_by_pool[1])
            },
            'daily_unique_stakers': defaultdict(lambda: {'pool_0': 0, 'pool_1': 0, 'combined': 0}),
            'average_stake_time': avg_stake_time,
            'combined_average_stake_time': combined_avg_stake_time,
            'total_stakes': stake_count,
            'prices': prices,
            'emissionToday': get_todays_capital_emission()
        }

        # Process daily data
        for date, pools in stakers_by_pool_and_date.items():
            results['daily_unique_stakers'][date] = {
                'pool_0': len(pools[0]),
                'pool_1': len(pools[1]),
                'combined': len(pools[0] | pools[1])
            }

    except Exception as e:
        logger.error(f"Unexpected error when analyzing MOR stakers from DataFrame: {str(e)}")
        raise

    return results


def calculate_average_multipliers():
    df = get_dataframe_from_sheet_name(USER_MULTIPLIER_SHEET_NAME)

    try:
        # Filter valid stakes
        valid_df = df[df.apply(is_valid_stake, axis=1)]

        # Convert multiplier from wei to whole units
        valid_df['multiplier'] = valid_df['multiplier'].apply(lambda x: Decimal(x) / Decimal('1e18'))

        # Calculate overall average
        total_multiplier = valid_df['multiplier'].sum()
        total_count = len(valid_df)
        average_multiplier = total_multiplier / total_count if total_count > 0 else Decimal('0')

        # Calculate average for capital pool (poolId = 0)
        capital_df = valid_df[valid_df['poolId'] == 0]
        capital_multiplier = capital_df['multiplier'].sum()
        capital_count = len(capital_df)
        average_capital_multiplier = capital_multiplier / capital_count if capital_count > 0 else Decimal('0')

        # Calculate average for code pool (poolId = 1)
        code_df = valid_df[valid_df['poolId'] == 1]
        code_multiplier = code_df['multiplier'].sum()
        code_count = len(code_df)
        average_code_multiplier = code_multiplier / code_count if code_count > 0 else Decimal('0')

        logger.info("Successfully calculated average multipliers from DataFrame")

        return {
            'overall_average': average_multiplier,
            'capital_average': average_capital_multiplier,
            'code_average': average_code_multiplier
        }

    except Exception as e:
        logger.error(f"Unexpected error when calculating average multipliers: {str(e)}")
        raise


def calculate_pool_rewards_summary():
    try:
        # Fetch data from reward_summary table
        df = get_dataframe_from_sheet_name(REWARD_SUM_SHEET_NAME)

        # Initialize the pool_rewards dictionary
        pool_rewards = defaultdict(lambda: {'daily_reward_sum': 0, 'total_current_user_reward_sum': 0})

        # Process the data
        for _, row in df.iterrows():
            category = row['Category']
            value = float(row['Value'])  # Assuming 'Value' is a string, convert to float

            if category == 'Daily Pool 0':
                pool_rewards[0]['daily_reward_sum'] = abs(value)
            elif category == 'Daily Pool 1':
                pool_rewards[1]['daily_reward_sum'] = abs(value)
            elif category == 'Total Pool 0':
                pool_rewards[0]['total_current_user_reward_sum'] = abs(value)
            elif category == 'Total Pool 1':
                pool_rewards[1]['total_current_user_reward_sum'] = abs(value)

        logger.info("Successfully calculated pool rewards summary from reward_summary table")
        return dict(pool_rewards)  # Convert defaultdict to regular dict

    except Exception as e:
        logger.error(f"Error calculating pool rewards summary: {str(e)}")
        raise


def get_wallet_stake_info():
    df = get_dataframe_from_sheet_name(USER_MULTIPLIER_SHEET_NAME)

    wallet_info = {
        'combined': {},
        'capital': {},
        'code': {}
    }

    for _, row in df.iterrows():
        if not is_valid_stake(row):
            continue

        wallet = row['user']
        pool_id = int(row['poolId'])

        # Calculate stake time
        claim_lock_start = int(row['claimLockStart'])
        claim_lock_end = int(row['claimLockEnd'])
        stake_time = timedelta(seconds=claim_lock_end - claim_lock_start)

        # Get power multiplier, handling scientific notation
        power_multiplier = int(float(row['multiplier']))

        # Update wallet info for combined and specific pool
        for key in ['combined', 'capital' if pool_id == 0 else 'code']:
            if wallet not in wallet_info[key] or stake_time > wallet_info[key][wallet]['stake_time']:
                wallet_info[key][wallet] = {
                    'stake_time': stake_time,
                    'power_multiplier': power_multiplier
                }

    def process_pool_data(pool_data):
        stake_times = np.array([v["stake_time"].total_seconds() for v in pool_data.values()])
        power_multipliers = np.array([v["power_multiplier"] / 1e25 for v in pool_data.values()])

        year_in_seconds = 365.25 * 24 * 60 * 60
        stake_times_in_years = stake_times / year_in_seconds

        def bin_data_custom_ranges(data, bins, right=True):
            bin_indices = np.digitize(data, bins, right=right)
            frequencies = np.bincount(bin_indices, minlength=len(bins))[1:]
            ranges = [[float(bins[i]), float(bins[i + 1]) if i < len(bins) - 2 else None] for i in range(len(bins) - 1)]
            return ranges, frequencies.tolist()

        stake_time_bins_years = [0, 1, 2, 3, 4, 5, 6, 1000]  # Using 1000 years as an effective "infinity"
        stake_time_ranges, stake_time_frequencies = bin_data_custom_ranges(stake_times_in_years, stake_time_bins_years)

        # Define the specific power multiplier ranges we want
        power_multiplier_bins = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, float('inf')]
        power_multiplier_ranges, power_multiplier_frequencies = bin_data_custom_ranges(
            power_multipliers,
            power_multiplier_bins,
            right=False  # Changed to False to make ranges inclusive on the left
        )

        return {
            "stake_time": {
                "ranges": stake_time_ranges,
                "frequencies": stake_time_frequencies
            },
            "power_multiplier": {
                "ranges": power_multiplier_ranges,
                "frequencies": power_multiplier_frequencies
            }
        }

    output = {
        "combined": process_pool_data(wallet_info['combined']),
        "capital": process_pool_data(wallet_info['capital']),
        "code": process_pool_data(wallet_info['code'])
    }

    return output


##################################################### APY REWARD CALCULATIONS ##########################################
def get_virtual_steth_pool(pool_id):
    pools_data = distribution_contract.functions.poolsData(pool_id).call()
    return pools_data[2] / 1e18


def calculate_power_factor(staking_period_days):
    # Convert days to years
    years = staking_period_days // 365

    # Define static multipliers for years 0-6
    multipliers = [1, 2.12, 4.17, 6.08, 7.82, 9.35, 10.67]

    # If years is greater than 6, use the last multiplier
    if years >= 6:
        return multipliers[-1]
    else:
        return multipliers[years]


def calculate_mor_rewards(mor_daily_emission, staking_period_days, mor_price, eth_price):
    total_virtual_steth = get_virtual_steth_pool(0)
    # Calculate power factor
    power_factor = calculate_power_factor(staking_period_days)
    # Calculate APR
    apr = (mor_daily_emission * 365 * mor_price * power_factor) / (total_virtual_steth * eth_price)
    # Calculate APY assuming compounding once per year
    apy = (1 + apr) ** 1 - 1
    # Calculate daily MOR rewards per 1 deposited stETH
    daily_mor_rewards = (mor_daily_emission * power_factor) / total_virtual_steth
    return apy, daily_mor_rewards


def give_more_reward_response():
    mor_daily_emission = get_todays_capital_emission()

    staking_periods = [0, 365, 730, 1095, 1460, 1825, 2190]  # 0, 1 year, 2 years, 3 years, 4 years, 5 years, 6 years

    mor_price = get_crypto_price("morpheusai")
    eth_price = get_crypto_price("staked-ether")  # stETH address

    rewards_data = {
        "apy_per_steth": [],
        "daily_mor_rewards_per_steth": []
    }

    for period in staking_periods:
        apy, daily_mor_rewards = calculate_mor_rewards(mor_daily_emission, period, mor_price, eth_price)
        rewards_data["apy_per_steth"].append({
            "staking_period": period,
            "apy": f"{apy:.2%}"
        })
        rewards_data["daily_mor_rewards_per_steth"].append({
            "staking_period": period,
            "daily_mor_rewards": f"{daily_mor_rewards:.6f}"
        })
    return rewards_data


async def get_analyze_mor_master_dict():
    staker_analysis = analyze_mor_stakers()
    multiplier_analysis = calculate_average_multipliers()
    stakereward_analysis = calculate_pool_rewards_summary()
    today = datetime.today()
    emissionreward_analysis = read_emission_schedule(today)

    # Convert date objects to strings
    staker_analysis['daily_unique_stakers'] = {
        k.isoformat() if isinstance(k, date) else k: v
        for k, v in staker_analysis['daily_unique_stakers'].items()
    }

    # Convert timedelta objects to string representations
    for pool_id, time_delta in staker_analysis['average_stake_time'].items():
        staker_analysis['average_stake_time'][pool_id] = str(time_delta)
    staker_analysis['combined_average_stake_time'] = str(staker_analysis['combined_average_stake_time'])

    # Convert numpy types to Python native types
    def convert_np(obj):
        if isinstance(obj, np.generic):
            return obj.item()
        elif isinstance(obj, dict):
            return {k: convert_np(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_np(i) for i in obj]
        return obj

    emissionreward_analysis = convert_np(emissionreward_analysis)

    # Cache the staking analysis results
    staking_metrics = {
        "staker_analysis": staker_analysis,
        "multiplier_analysis": {
            "overall_average": float(multiplier_analysis['overall_average']),
            "capital_average": float(multiplier_analysis['capital_average']),
            "code_average": float(multiplier_analysis['code_average'])
        },
        "stakereward_analysis": {str(k): v for k, v in stakereward_analysis.items()},
        "emissionreward_analysis": emissionreward_analysis
    }

    # print(json.dumps(staking_metrics, indent=4))

    return staking_metrics
