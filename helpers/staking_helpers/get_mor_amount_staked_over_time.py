import asyncio
import json
from collections import defaultdict
from datetime import datetime

import pandas as pd
from web3 import AsyncWeb3

from app.core.config import (ETH_RPC_URL, DISTRIBUTION_PROXY_ADDRESS, DISTRIBUTION_ABI, logger)
from app.repository import UserMultiplierRepository

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BATCH_SIZE = 50

w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(ETH_RPC_URL))

distribution_contract = w3.eth.contract(address=w3.to_checksum_address(DISTRIBUTION_PROXY_ADDRESS),
                                        abi=DISTRIBUTION_ABI)


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


def get_user_multiplier_dataframe():
    """
    Get a DataFrame from the repository.
        
    Returns:
        DataFrame with the repository data
    """

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
            'claim_lock_start': 'claimLockStart',
            'claim_lock_end': 'claimLockEnd'
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


def is_valid_stake(row):
    """Validate if a stake is currently active and within reasonable timeframe"""
    current_time = int(datetime.now().timestamp())
    claim_lock_start = int(row['claimLockStart'])
    claim_lock_end = int(row['claimLockEnd'])
    twenty_five_years_from_now = current_time + (25 * 365 * 24 * 60 * 60)

    return (claim_lock_start != 0 and
            claim_lock_end != 0 and
            current_time < claim_lock_end <= twenty_five_years_from_now)


async def get_user_reward(pool_id, address):
    """Get current user reward with retry mechanism"""
    for attempt in range(MAX_RETRIES):
        try:
            reward = await distribution_contract.functions.getCurrentUserReward(pool_id, address).call()
            return float(w3.from_wei(reward, 'ether'))
        except Exception as e:
            if 'Too Many Requests' in str(e) and attempt < MAX_RETRIES - 1:
                logger.warning(f"Rate limit hit, retrying in {RETRY_DELAY} seconds...")
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"Error getting reward for {address} in pool {pool_id}: {str(e)}")
                return 0


async def process_batch(batch_df):
    """Process a batch of users and get their current rewards"""
    tasks = []
    for _, row in batch_df.iterrows():
        pool_id = int(row['poolId'])
        address = w3.to_checksum_address(row['user'])
        tasks.append(get_user_reward(pool_id, address))

    return await asyncio.gather(*tasks)


async def get_mor_staked_over_time():
    try:
        df = get_user_multiplier_dataframe()
        
        if df.empty:
            logger.error("No data found in user_multiplier repository")
            return {}
            
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])

        # Initialize tracking dictionary with defaultdict
        daily_rewards = defaultdict(lambda: {
            'daily_current_rewards_locked': 0.0,
            'total': 0.0,  # Total cumulative
            'capital': 0.0,  # Pool 0 cumulative
            'code': 0.0,  # Pool 1 cumulative
            'pool_0_daily': 0.0,
            'pool_1_daily': 0.0
        })

        # Filter valid stakes
        valid_stakes = df[df.apply(is_valid_stake, axis=1)].copy()

        # Process in batches
        batches = [valid_stakes[i:i + BATCH_SIZE] for i in range(0, len(valid_stakes), BATCH_SIZE)]

        # Track cumulative values
        cumulative_total = 0.0
        cumulative_pool_0 = 0.0
        cumulative_pool_1 = 0.0

        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i + 1}/{len(batches)}")

            rewards = await process_batch(batch)

            for (_, row), reward in zip(batch.iterrows(), rewards):
                date_key = row['Timestamp'].strftime('%d/%m/%Y')
                pool_id = int(row['poolId'])

                # Update daily rewards
                if pool_id == 0:
                    daily_rewards[date_key]['pool_0_daily'] += round(reward, 4)
                    cumulative_pool_0 += reward
                else:
                    daily_rewards[date_key]['pool_1_daily'] += round(reward, 4)
                    cumulative_pool_1 += reward

                cumulative_total = cumulative_pool_0 + cumulative_pool_1

                # Update cumulative values for this date
                daily_rewards[date_key]['total'] = round(cumulative_total, 4)
                daily_rewards[date_key]['capital'] = round(cumulative_pool_0, 4)
                daily_rewards[date_key]['code'] = round(cumulative_pool_1, 4)

            await asyncio.sleep(1)  # Rate limiting

        return dict(daily_rewards)

    except Exception as e:
        logger.error(f"Error in get_mor_staked_over_time: {str(e)}")
        raise