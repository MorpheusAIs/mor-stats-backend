# `iiii_update_circulating_supply.py` Requirements Summary

## Overview
This script tracks and updates the circulating supply of a cryptocurrency token by monitoring `UserClaimed` events from an Ethereum blockchain distribution contract. It maintains a historical record of circulating supply changes over time in Google Sheets.

## Core Functionality

1. **Blockchain Event Monitoring**
   - Connects to an Ethereum node using a preconfigured Web3 instance
   - Retrieves `UserClaimed` events from a distribution contract
   - Processes events starting from the block after the latest recorded timestamp
   - Calculates circulating supply changes based on claim amounts

2. **Historical Data Management**
   - Downloads existing circulating supply data from Google Sheets
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
   - `pandas`: For data manipulation and date handling
   - `datetime`: For timestamp formatting and conversion
   - `logging`: For application logging
   - Custom modules:
     - `configuration.config`: For Web3 and contract configuration
     - `sheet_config.google_utils`: For Google Sheets operations

2. **Error Handling**
   - Catches and logs blockchain-related exceptions like `BlockNotFound`
   - Implements comprehensive error logging throughout the process
   - Provides operational notifications via Slack
   - Ensures temporary file cleanup even if errors occur

## Data Structure

The script maintains a Google Sheet with the following columns:
- `date`: Date in DD/MM/YYYY format
- `circulating_supply_at_that_date`: Cumulative token supply in circulation as of the date
- `block_timestamp_at_that_date`: Unix timestamp of the corresponding blockchain block
- `total_claimed_that_day`: Amount of tokens newly claimed on that specific date

## Implementation Details

1. **Block Discovery**
   - Implements binary search algorithm to efficiently find blocks by timestamp
   - Handles edge cases where blocks might be missing or not found

2. **Data Integrity**
   - Sorts by date and removes duplicates, keeping only the most recent record per date
   - Ensures chronological ordering of records
   - Converts between different date formats for consistency

3. **File Management**
   - Creates and manages temporary CSV files during processing
   - Cleans up temporary files after successful operation

4. **Execution Pattern**
   - Designed to be run as a scheduled task or cron job
   - Contains commented-out execution functions for deployment flexibility

## Operational Flow
1. Download existing circulating supply data from Google Sheets
2. Identify the latest recorded timestamp and corresponding supply figure
3. Retrieve all `UserClaimed` events that occurred after the latest timestamp
4. For each event:
   - Extract the block timestamp and convert to a date
   - Calculate the claimed amount in tokens
   - Add the claimed amount to the running circulating supply total
   - Record the date, updated supply, block timestamp, and daily claim amount
5. Sort the updated data chronologically and remove duplicate dates
6. Upload the complete dataset back to Google Sheets
7. Send notification about the operation status
8. Clean up temporary files

This script provides a crucial tracking mechanism for monitoring the token's circulating supply over time, which is important financial data for cryptocurrency projects. The historical record maintained enables trend analysis and provides transparency for token holders.