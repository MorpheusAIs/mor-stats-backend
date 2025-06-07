# `vi_update_overplus_bridged_events.py` Requirements Summary

## Overview
This script tracks and processes `OverplusBridged` events from an Ethereum blockchain distribution contract and uploads this data to Google Sheets. It maintains an ongoing record of bridge operations related to overplus (excess) tokens in the system.

## Core Functionality

1. **Blockchain Event Monitoring**
   - Connects to an Ethereum node using the configured `ETH_RPC_URL`
   - Fetches `OverplusBridged` events from a distribution contract
   - Processes events from a specified starting block (20180927) or continues from the last processed block
   - Handles large block ranges by breaking requests into 1,000,000 block batches

2. **Data Processing**
   - Extracts event data including:
     - Timestamp (converted from block timestamp)
     - Transaction hash
     - Block number
     - Amount bridged
     - Unique ID (in hexadecimal format)
   - Organizes the data into a structured format for storage

3. **Google Sheets Integration**
   - Downloads existing data from a Google Sheet named "OverplusBridged"
   - Determines the last processed block to avoid duplicate processing
   - Appends new event data to existing records
   - Uploads the updated dataset back to the Google Sheet

## Technical Requirements

1. **Dependencies**
   - `web3`: For Ethereum blockchain interaction
   - `pandas`: For data manipulation
   - `datetime`: For timestamp handling
   - `logging`: For application logging
   - Custom modules:
     - `configuration.config`: For Ethereum RPC and contract configuration
     - `sheet_config.google_utils`: For Google Sheets operations

2. **Configuration Requirements**
   - Ethereum RPC URL (`ETH_RPC_URL`)
   - Distribution contract instance
   - Starting block number (20180927)
   - Batch size for processing (1,000,000 blocks)
   - Target Google Sheet name ("OverplusBridged")

3. **Error Handling**
   - Comprehensive error catching and logging
   - Batch-level error isolation to prevent complete process failure
   - Detailed exception logging for debugging purposes

## Implementation Details

1. **Event Retrieval**
   - Uses Web3's event filtering to efficiently retrieve blockchain events
   - Dynamically extracts event parameters from the contract's ABI
   - Implements batched retrieval to handle large block ranges

2. **Sheet Management**
   - Determines the last processed block from existing sheet data
   - Appends only new events to maintain a complete historical record
   - Performs cleanup of temporary files after processing

3. **Data Flow**
   - Downloads existing "OverplusBridged" sheet data
   - Determines last processed block
   - Fetches new events from blockchain
   - Formats event data for storage
   - Appends new data to existing data
   - Uploads combined dataset back to Google Sheets
   - Cleans up temporary files

4. **Notification**
   - Logs key actions and errors for monitoring

## Specific Features

- Unlike other scripts, event name is taken from `SHEET_NAME` ("OverplusBridged")
- Uniquely handles the `uniqueId` field by converting it to hexadecimal format
- Uses a slightly different data structure (amount and uniqueId) compared to other event tracking scripts
- Follows the same overall pattern as other event tracking scripts in the series

## Operational Notes

- The main execution function is commented out, suggesting this may be imported and called from elsewhere
- Creates and manages temporary files during operation
- Maintains incremental processing by storing and retrieving the last processed block

This script continues the pattern seen in the previous event monitoring scripts but focuses specifically on `OverplusBridged` events. It follows the same architectural approach but adapts to the specific data structure of this event type, particularly in how it handles the `uniqueId` field (converting it to hexadecimal) and the different event parameters it tracks.