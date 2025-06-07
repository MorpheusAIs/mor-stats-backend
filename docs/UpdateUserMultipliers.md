# `ii_update_user_multipliers.py` Requirements Summary

## Overview
This script processes blockchain data to calculate and record user multipliers from a distribution contract. It reads user claim events from a Google Sheet, fetches corresponding multipliers from the Ethereum blockchain, and uploads the results back to Google Sheets.

## Core Functionality

1. **Blockchain Interaction**
   - Uses `AsyncWeb3` to connect to an Ethereum node via the configured RPC URL
   - Queries a distribution contract to get user multiplier values
   - Performs lookups based on pool ID, user address, and timestamp

2. **Batch Processing**
   - Downloads data from "UserClaimLocked" Google Sheet
   - Processes records in batches of 50 to manage API load
   - Uses asynchronous calls to improve performance
   - Implements retry logic for rate-limited operations

3. **Data Processing**
   - Parses timestamps from input data
   - Queries blockchain for multiplier values at specific blocks
   - Formats multipliers as integers with no decimal places
   - Adds multiplier data as a new column to the input data

4. **Google Sheets Integration**
   - Downloads input data from "UserClaimLocked" sheet
   - Uploads results to "UserMultiplier" sheet
   - Clears existing data before uploading new records

## Technical Requirements

1. **Dependencies**
   - `asyncio`: For asynchronous processing
   - `pandas`: For data manipulation
   - `dateutil`: For timestamp parsing
   - `web3`: For Ethereum blockchain interaction (AsyncWeb3 variant)
   - `decimal`: For precise handling of large numbers
   - Custom modules:
     - `configuration.config`: For RPC URL, contract information, and spreadsheet ID

2. **Configuration Requirements**
   - Ethereum RPC URL (`ETH_RPC_URL`)
   - Distribution contract instance
   - Google Spreadsheet ID
   - Sheet names for input/output

3. **Error Handling**
   - Retry mechanism for rate-limited API calls (maximum 3 attempts)
   - Comprehensive error logging
   - Temporary file cleanup in the finally block

## Implementation Details

1. **Rate Limiting Protection**
   - 5-second delay between retry attempts
   - 1-second delay between batch processing
   - Batch size of 50 records to avoid overwhelming the API

2. **Data Formatting**
   - Multipliers are converted to exact Decimal values and formatted as integers
   - Error values are marked as "Error" string in the output

3. **Temporary File Management**
   - Creates temporary files during processing
   - Ensures cleanup of temporary files even if errors occur

4. **Execution Flow**
   - Currently has commented-out main execution code
   - Designed to be run as a standalone script via `asyncio.run()`

## Data Flow
1. Download "UserClaimLocked" sheet data
2. Split data into manageable batches
3. For each record, query the blockchain for user multiplier data
4. Format multiplier values and add to the dataset
5. Save results to a temporary CSV file
6. Upload results to "UserMultiplier" Google Sheet
7. Send notification about operation status
8. Clean up temporary files

This script appears to be the second step in a data pipeline that first collects UserClaimLocked events (potentially from th