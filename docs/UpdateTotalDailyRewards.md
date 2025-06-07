# `iii_update_total_daily_rewards.py` Requirements Summary

## Overview
This script calculates and tracks daily and total user rewards from an Ethereum distribution contract. It processes user data from a Google Sheet, calculates reward values for different pools, and uploads summary statistics back to Google Sheets.

## Core Functionality

1. **Blockchain Interaction**
   - Connects to Ethereum using AsyncWeb3 via the configured RPC URL
   - Interacts with a distribution contract to fetch reward data
   - Uses Etherscan API to convert timestamps to block numbers

2. **Reward Calculation**
   - Calculates daily rewards by comparing current rewards with rewards from 24 hours ago
   - Processes users across multiple pools (Pool 0 and Pool 1)
   - Aggregates statistics for both individual pools and combined totals

3. **Batch Processing**
   - Fetches user data from "UserMultiplier" Google Sheet
   - Processes users in batches of 50 to manage API load
   - Implements asynchronous processing for improved performance
   - Uses retry logic for rate-limited operations

4. **Summary Generation**
   - Removes duplicate user/pool combinations
   - Calculates six key metrics:
     - Daily Pool 0 rewards
     - Daily Pool 1 rewards
     - Combined daily rewards
     - Total Pool 0 rewards
     - Total Pool 1 rewards
     - Combined total rewards

5. **Google Sheets Integration**
   - Downloads input data from "UserMultiplier" sheet
   - Uploads summary results to "RewardSum" sheet
   - Clears existing data before uploading new records

## Technical Requirements

1. **Dependencies**
   - `asyncio`: For asynchronous processing
   - `pandas`: For data manipulation
   - `time`: For timestamp handling
   - `aiohttp`: For async API calls to Etherscan
   - `web3`: For Ethereum blockchain interaction (AsyncWeb3 variant)
   - Custom modules:
     - `configuration.config`: For RPC URL, contract information, API keys, and spreadsheet ID

2. **API Requirements**
   - Ethereum RPC endpoint for blockchain interaction
   - Etherscan API (with API key) for block number lookups
   - Google Sheets API for reading/writing spreadsheet data

3. **Error Handling**
   - Retry mechanism for rate-limited API calls (maximum 3 attempts with 5-second delay)
   - Comprehensive logging
   - Temporary file cleanup even if errors occur

## Implementation Details

1. **Rate Limiting Protection**
   - 5-second delay between retry attempts
   - 1-second delay between batch processing
   - Batch size of 50 records to manage API load

2. **Block Calculation**
   - Uses Etherscan API to find the block closest to 24 hours ago
   - Compares rewards at current block vs. block from 24 hours ago

3. **Data Processing**
   - Handles duplicate removal for user/pool combinations
   - Converts wei values to ether for human-readable results
   - Aggregates values across multiple users and pools

4. **Temporary File Management**
   - Creates and manages temporary CSV files during processing
   - Ensures cleanup of temporary files in finally block

## Execution Flow
1. Download "UserMultiplier" sheet data
2. Remove duplicate user/pool combinations
3. Determine relevant blocks (24 hours ago and current)
4. Process users in batches:
   - Fetch current reward value for each user/pool
   - Fetch reward value from 24 hours ago for each user/pool
   - Calculate daily reward as the difference
5. Aggregate results by pool and calculate combined totals
6. Generate a summary dataframe with the six key metrics
7. Upload results to "RewardSum" Google Sheet
8. Send notification about operation status
9. Clean up temporary files

This script is the third step in what appears to be a data pipeline for tracking and analyzing blockchain rewards, building upon the data collected and processed by the previous scripts (`i_update_user_claim_locked_events.py` and `ii_update_user_multipliers.py`).