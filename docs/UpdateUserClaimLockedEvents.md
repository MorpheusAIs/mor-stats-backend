# `i_update_user_claim_locked_events.py` Requirements Summary

## Overview
This script is designed to track and process `UserClaimLocked` events from an Ethereum blockchain contract and prepare this data for storage in Google Sheets.

## Core Functionality

1. **Blockchain Event Monitoring**
   - Connects to an Ethereum node using the configured `ETH_RPC_URL`
   - Fetches `UserClaimLocked` events from a distribution contract
   - Processes events starting from a specified block (20180927) or continues from the last processed block
   - Handles large ranges by breaking requests into batches of 1 million blocks

2. **Data Processing**
   - Extracts event data including:
     - Timestamp (converted from block timestamp)
     - Transaction hash
     - Block number
     - Event-specific arguments
   - Organizes data into structured format for storage

3. **Google Sheets Integration**
   - Downloads existing data from a Google Sheet named "UserClaimLocked"
   - Determines the last processed block to avoid duplicate processing
   - Prepares new data to be appended to existing records
   - Contains commented-out code for uploading the processed data back to Google Sheets

## Technical Requirements

1. **Dependencies**
   - `web3`: For Ethereum blockchain interaction
   - `pandas`: For data manipulation
   - `datetime`: For timestamp handling
   - `logging`: For application logging
   - Custom modules:
     - `app.core.config`: For Ethereum RPC and contract configuration
     - `sheets_config.google_utils`: For Google Sheets operations

2. **Configuration Requirements**
   - Ethereum RPC URL (`ETH_RPC_URL`)
   - Distribution contract instance (`distribution_contract`)
   - Starting block number (20180927)
   - Batch size for processing (1,000,000 blocks)

3. **Error Handling**
   - Comprehensive error catching and logging
   - Batch-level error isolation to prevent complete process failure
   - Logging of exceptions with detailed information

## Implementation Notes

- The main execution code is commented out (`if __name__ == "__main__"`)
- Functions for uploading data to Google Sheets are commented out
- There's a commented-out reference to Slack notifications for operation status reporting

## Data Flow
1. Download existing data from Google Sheets
2. Determine last processed block
3. Fetch new events from blockchain
4. Process and format the new event data
5. [Currently disabled] Append new data to existing data
6. [Currently disabled] Upload updated data back to Google Sheets

This script appears to be part of a data pipeline for tracking blockchain events related to user claim locking functionality, with the purpose of maintaining an up-to-date record in Google Sheets for analysis or reporting.