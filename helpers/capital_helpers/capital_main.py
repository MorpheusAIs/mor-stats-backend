import json
from collections import OrderedDict, defaultdict
from datetime import datetime
import pandas as pd
from app.core.config import (logger, USER_STAKED_SHEET_NAME,
                             USER_WITHDRAWN_SHEET_NAME,
                             OVERPLUS_BRIDGED_SHEET_NAME, distribution_contract,
                             MAINNET_BLOCK_1ST_JAN_2024, EMISSIONS_SHEET_NAME)
from app.repository import (
    UserStakedEventsRepository,
    UserWithdrawnEventsRepository,
    OverplusBridgedEventsRepository
)
from helpers.staking_helpers.get_emission_schedule_for_today import read_emission_schedule
from helpers.staking_helpers.staking_main import calculate_pool_rewards_summary


def process_transactions(df):
    balances = defaultdict(float)
    for _, row in df.iterrows():
        balances[row['User']] += row['Amount']
    return balances


def safe_divide(df, column, divisor):
    try:
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column] / divisor
    except Exception as e:
        logger.error(f"Error converting and dividing {column}: {str(e)}")
    return df


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


def get_total_supply_and_staker_info():
    try:
        # Get data from repositories instead of sheets
        user_staked_df = get_repository_data_as_dataframe(
            UserStakedEventsRepository, "user_staked_events")
        user_withdrawn_df = get_repository_data_as_dataframe(
            UserWithdrawnEventsRepository, "user_withdrawn_events")
        
        if user_staked_df.empty or user_withdrawn_df.empty:
            logger.error("One or both DataFrames are empty")
            return OrderedDict(), {}, OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict(), pd.DataFrame()
        
        # Rename columns to match the expected format
        user_staked_df = user_staked_df.rename(columns={
            'timestamp': 'Timestamp',
            'transaction_hash': 'TransactionHash',
            'block_number': 'BlockNumber',
            'pool_id': 'PoolId',
            'user_address': 'User',
            'amount': 'Amount'
        })
        
        user_withdrawn_df = user_withdrawn_df.rename(columns={
            'timestamp': 'Timestamp',
            'transaction_hash': 'TransactionHash',
            'block_number': 'BlockNumber',
            'pool_id': 'PoolId',
            'user_address': 'User',
            'amount': 'Amount'
        })
        
        # Convert amount from raw blockchain format to decimal
        user_staked_df = safe_divide(user_staked_df, 'Amount', 1e18)
        user_withdrawn_df = safe_divide(user_withdrawn_df, 'Amount', 1e18)

    except Exception as e:
        logger.error(f"Error reading from repositories: {str(e)}")
        return OrderedDict(), {}, OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict(), pd.DataFrame()

    # Ensure the DataFrames have the required columns
    required_columns = ["Timestamp", "TransactionHash", "BlockNumber", "PoolId", "User", "Amount"]

    if not all(col in user_staked_df.columns
               and
               col in user_withdrawn_df.columns for col in required_columns):
        logger.error(f"DataFrames are missing required columns. Available columns: "
                     f"Staked: {user_staked_df.columns}, Withdrawn: {user_withdrawn_df.columns}")
        return OrderedDict(), {}, OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict(), pd.DataFrame()

    try:
        # Convert Timestamp to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(user_staked_df['Timestamp']):
            user_staked_df['Timestamp'] = pd.to_datetime(user_staked_df['Timestamp'], errors='coerce')
        
        if not pd.api.types.is_datetime64_any_dtype(user_withdrawn_df['Timestamp']):
            user_withdrawn_df['Timestamp'] = pd.to_datetime(user_withdrawn_df['Timestamp'], errors='coerce')

        # Remove rows with NaT timestamps
        user_staked_df = user_staked_df.dropna(subset=['Timestamp'])
        user_withdrawn_df = user_withdrawn_df.dropna(subset=['Timestamp'])

        # Process transactions
        staked_balances = process_transactions(user_staked_df)
        withdrawn_balances = process_transactions(user_withdrawn_df)

        # Calculate final balances
        final_balances = defaultdict(float)
        for address, amount in staked_balances.items():
            final_balances[address] = amount - withdrawn_balances.get(address, 0)

        # Aggregate data by day
        user_staked_df['Date'] = user_staked_df['Timestamp'].dt.date
        user_withdrawn_df['Date'] = user_withdrawn_df['Timestamp'].dt.date

        daily_staked = user_staked_df.groupby('Date')['Amount'].sum().reset_index()
        daily_withdrawn = user_withdrawn_df.groupby('Date')['Amount'].sum().reset_index()

        # Merge daily staked and withdrawn data
        daily_net = pd.merge(daily_staked, daily_withdrawn, on='Date', how='outer', suffixes=('_staked', '_withdrawn'))
        daily_net = daily_net.fillna(0)
        daily_net['Net_Staked'] = daily_net['Amount_staked'] - daily_net['Amount_withdrawn']
        daily_net['Cumulative_Net_Staked'] = daily_net['Net_Staked'].cumsum()

        # Sort by date in ascending order
        daily_net = daily_net.sort_values('Date')

        # Create JSON outputs
        json_output = OrderedDict()
        total_stakers_by_date = OrderedDict()
        active_stakers_by_date = OrderedDict()
        currently_staked_by_date = OrderedDict()
        total_staked_by_date = OrderedDict()

        cumulative_stakers = set()
        cumulative_staked = 0

        # Create a copy of user_staked_df with unique Users only
        unique_users_staked_df = user_staked_df.drop_duplicates(subset=['User'])

        for _, row in daily_net.iterrows():
            date_str = row['Date'].strftime('%d/%m/%Y')
            json_output[date_str] = {
                'Staked': round(row['Amount_staked'], 4),
                'Withdrawn': round(row['Amount_withdrawn'], 4),
                'Net_Staked': round(row['Net_Staked'], 4),
                'Cumulative_Net_Staked': round(row['Cumulative_Net_Staked'], 4)
            }

            # Calculate cumulative stakers and total staked for each date using unique users
            daily_stakers = set(unique_users_staked_df[unique_users_staked_df['Date'] == row['Date']]['User'])
            cumulative_stakers.update(daily_stakers)
            cumulative_staked += row['Amount_staked']

            # For total stakers, we use the cumulative unique stakers
            total_stakers_by_date[date_str] = len(cumulative_stakers)

            # For active stakers, we only count unique addresses with positive balances
            unique_active_stakers = len(set(addr for addr in cumulative_stakers if final_balances[addr] > 0))
            active_stakers_by_date[date_str] = unique_active_stakers

            currently_staked_by_date[date_str] = round(row['Cumulative_Net_Staked'], 4)
            total_staked_by_date[date_str] = round(cumulative_staked, 4)

        return (json_output, dict(
            final_balances), total_stakers_by_date, active_stakers_by_date, currently_staked_by_date,
                total_staked_by_date, daily_net)

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return OrderedDict(), {}, OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict(), pd.DataFrame()


def get_bridged_overplus_amounts_by_date():
    try:
        # Get data from repository instead of sheet
        bridged_df = get_repository_data_as_dataframe(
            OverplusBridgedEventsRepository, "overplus_bridged_events")
        
        if bridged_df.empty:
            logger.error("Bridged events DataFrame is empty")
            return OrderedDict()
        
        # Rename columns to match the expected format
        bridged_df = bridged_df.rename(columns={
            'timestamp': 'Timestamp',
            'transaction_hash': 'TransactionHash',
            'block_number': 'BlockNumber',
            'unique_id': 'uniqueId'
        })
        
    except Exception as e:
        logger.error(f"Error reading from repository: {str(e)}")
        return OrderedDict()

    # Ensure the DataFrame has the required columns
    required_columns = ["Timestamp", "TransactionHash", "BlockNumber", "amount", "uniqueId"]

    if not all(col in bridged_df.columns for col in required_columns):
        logger.error(f"DataFrame is missing required columns. Available columns: {bridged_df.columns}")
        return OrderedDict()

    try:
        # Convert Timestamp to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(bridged_df['Timestamp']):
            bridged_df['Timestamp'] = pd.to_datetime(bridged_df['Timestamp'], errors='coerce')

        # Remove rows with NaT timestamps
        bridged_df = bridged_df.dropna(subset=['Timestamp'])

        # Convert amount to string first to preserve precision
        bridged_df['amount'] = bridged_df['amount'].astype(str)

        # Convert to float after dividing by 1e18 to maintain precision
        bridged_df['amount'] = bridged_df['amount'].apply(lambda x: float(x) / 1e18)

        # Aggregate data by day
        bridged_df['Date'] = bridged_df['Timestamp'].dt.date

        # Group by date to calculate the daily bridged amount
        daily_bridged = bridged_df.groupby('Date')['amount'].sum().reset_index()

        # Calculate the cumulative bridged amount
        daily_bridged['Cumulative_Bridged'] = daily_bridged['amount'].cumsum()

        # Sort the data by date in ascending order
        daily_bridged = daily_bridged.sort_values('Date')

        # Create JSON output
        json_output = OrderedDict()

        for _, row in daily_bridged.iterrows():
            date_str = row['Date'].strftime('%d/%m/%Y')
            json_output[date_str] = {
                'Daily_Bridged': round(row['amount'], 4),
                'Cumulative_Bridged': round(row['Cumulative_Bridged'], 4)
            }

        return json_output

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return OrderedDict()


def get_all_claim_metrics():

    today = datetime.today()
    emissions_data = {}
    claimed_capital_rewards = 0
    claimed_code_rewards = 0

    try:
        emissions_data = read_emission_schedule(today, EMISSIONS_SHEET_NAME)
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    total_code_emissions = emissions_data['total_emissions']['Code Emission']
    total_capital_emissions = emissions_data['total_emissions']['Capital Emission']

    claimed_filter = distribution_contract.events.UserClaimed.create_filter(from_block=MAINNET_BLOCK_1ST_JAN_2024,
                                                                            to_block='latest')

    events = claimed_filter.get_all_entries()

    for event in events:
        amount = (event['args']['amount'] / 1e18)
        pool_id = int(event['args']['poolId'])

        if pool_id == 0:
            claimed_capital_rewards += amount
        elif pool_id == 1:
            claimed_code_rewards += amount
        else:
            continue

    unclaimed_capital_emissions = total_capital_emissions - claimed_capital_rewards
    unclaimed_code_emissions = total_code_emissions - claimed_code_rewards

    total_emissions = total_capital_emissions + total_code_emissions
    total_claimed_rewards = claimed_capital_rewards + claimed_code_rewards
    total_unclaimed_rewards = total_emissions - total_claimed_rewards

    stakereward_analysis = calculate_pool_rewards_summary()
    stakereward_analysis = {str(key): value for key, value in stakereward_analysis.items()}

    total_capital_staked_reward_sum = stakereward_analysis["0"]["total_current_user_reward_sum"]
    total_code_staked_reward_sum = stakereward_analysis["1"]["total_current_user_reward_sum"]

    claim_metrics = {
        "capital": {
            "claimed_capital_rewards": claimed_capital_rewards,
            "unclaimed_capital_emissions": unclaimed_capital_emissions,
            "total_capital_staked_reward_sum": total_capital_staked_reward_sum,
            "total_capital_emissions": total_capital_emissions,
        },
        "code": {
            "claimed_code_rewards": claimed_code_rewards,
            "unclaimed_code_emissions": unclaimed_code_emissions,
            "total_code_staked_reward_sum": total_code_staked_reward_sum,
            "total_code_emissions": total_code_emissions,
        },
        "total": {
            "total_claimed_rewards": total_claimed_rewards,
            "total_unclaimed_rewards": total_unclaimed_rewards,
            "total_staked_reward_sum": total_capital_staked_reward_sum + total_code_staked_reward_sum,
            "total_emissions": total_emissions
        }
    }

    return claim_metrics


def get_capital_metrics():
    result = get_total_supply_and_staker_info()
    if len(result) == 7:
        json_output, final_balances, total_stakers, active_stakers, currently_staked, total_staked, daily_net = result

        capital_master_dict = {
            "detailed_daily_staking_data": json_output,
            "number_of_total_capital_providers_by_date": total_stakers,
            "number_of_active_capital_providers_by_date": active_stakers,
            "total_staked_steth_amount_by_date": total_staked,
            "currently_staked_steth_amount_by_date": currently_staked,
            "bridged_overplus_amount_by_date": get_bridged_overplus_amounts_by_date(),
            "claim_metrics": get_all_claim_metrics()
        }

        return capital_master_dict

    else:
        print("An error occurred while processing the data.")
        return {}
