# `iiii_update_circulating_supply.py` Requirements Summary

## Overview
This script tracks and updates the circulating supply of a cryptocurrency token by monitoring `UserClaimed` events from an Ethereum blockchain distribution contract. It maintains a historical record of circulating supply changes over time in a database.

## Core Functionality

1. **Blockchain Event Monitoring**
   - Connects to an Ethereum node using a preconfigured Web3 instance
   - Retrieves `UserClaimed` events from a distribution contract
   - Processes events starting from the block after the latest recorded timestamp
   - Calculates circulating supply changes based on claim amounts

2. **Historical Data Management**
   - Retrieves existing circulating supply data from the database
   - Determines the latest recorded data point to continue tracking from that point
   - Processes new blockchain events to update the circulating supply figures
   - Maintains chronological records with daily granularity

3. **Block Timestamp Correlation**
   - Uses binary search algorithm to find blockchain blocks by timestamp
   - Correlates event data with specific dates for time-series analysis
   - Records block timestamps alongside circulating supply values for reference

4. **Data Processing**
   - Converts raw blockchain token amounts to human-readable format (dividing by 10^18)
   - Accumulates claimed amounts to update the circulating supply
   - Sorts data chronologically and removes duplicate date entries
   - Maintains daily records of total claimed tokens

## Technical Requirements

1. **Dependencies**
   - `web3`: For Ethereum blockchain interaction
   - `datetime`: For timestamp formatting and conversion
   - `decimal`: For precise numeric calculations
   - `logging`: For application logging
   - Custom modules:
     - `app.core.config`: For Web3 and contract configuration
     - `app.db.database`: For database operations
     - `app.repository.circulating_supply_repository`: For circulating supply data operations

2. **Error Handling**
   - Catches and logs blockchain-related exceptions like `BlockNotFound`
   - Implements comprehensive error logging throughout the process
   - Provides operational notifications via Slack
   - Ensures database transaction integrity

## Data Structure

The script maintains a database table with the following columns:
- `id`: Primary key
- `date`: Date in YYYY-MM-DD format
- `circulating_supply_at_that_date`: Cumulative token supply in circulation as of the date
- `block_timestamp_at_that_date`: Unix timestamp of the corresponding blockchain block
- `total_claimed_that_day`: Amount of tokens newly claimed on that specific date
- `created_at`: Timestamp when the record was created

## Implementation Details

1. **Block Discovery**
   - Implements binary search algorithm to efficiently find blocks by timestamp
   - Handles edge cases where blocks might be missing or not found

2. **Data Integrity**
   - Uses database transactions to ensure data consistency
   - Implements ON CONFLICT handling for upsert operations
   - Ensures chronological ordering of records
   - Converts between different date formats for consistency

3. **Database Operations**
   - Creates the table if it doesn't exist
   - Uses prepared statements to prevent SQL injection
   - Implements bulk insert for efficient data loading
   - Uses indexes for optimized query performance

4. **Execution Pattern**
   - Designed to be run as a scheduled task or cron job
   - Contains a main function for direct execution

## Operational Flow
1. Ensure the database table exists with proper structure
2. Retrieve the latest recorded timestamp and corresponding supply figure from the database
3. Retrieve all `UserClaimed` events that occurred after the latest timestamp
4. For each event:
   - Extract the block timestamp and convert to a date
   - Calculate the claimed amount in tokens
   - Add the claimed amount to the running circulating supply total
   - Record the date, updated supply, block timestamp, and daily claim amount
5. Aggregate data by date to handle multiple claims on the same day
6. Save the updated data to the database using ON CONFLICT handling
7. Send notification about the operation status

## Initialization
A separate script `initialize_circulating_supply.py` is provided to:
1. Create the circulating supply table if it doesn't exist
2. Import historical data from the CSV file (`data/MASTER MOR EXPLORER - CircSupply.csv`)
3. Set up necessary indexes for efficient querying

To initialize the database with historical data, run:
```
python scripts/initialize_circulating_supply.py
```

This script provides a crucial tracking mechanism for monitoring the token's circulating supply over time, which is important financial data for cryptocurrency projects. The historical record maintained enables trend analysis and provides transparency for token holders.