"""
Script to update emissions data in the database.

This script can be used to:
1. Import initial emissions data from a CSV file
2. Update emissions data from a CSV file
"""
import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from decimal import Decimal

# Add the parent directory to the path so we can import from the app package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.database import get_db
from app.models.database_models import Emission
from app.repository import EmissionRepository

# Constants
TABLE_NAME = "emissions"
EVENT_NAME = "Emission"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)



def parse_emission_data(file_path):
    """
    Parse emission data from a CSV or TSV file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        List of emission data dictionaries
    """
    emissions = []
    
    try:
        # Determine the delimiter based on file extension
        delimiter = ',' if file_path.lower().endswith('.csv') else '\t'
        
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file, delimiter=delimiter)
            for row in reader:
                # Convert date string to date object
                date_str = row.get('Date')
                if date_str:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                else:
                    logger.warning(f"Skipping row with missing date: {row}")
                    continue
                
                # Parse emission data
                try:
                    emission = {
                        'day': int(row.get('Day', 0)),
                        'date': date_obj,
                        'capital_emission': Decimal(row.get('Capital Emission', 0)),
                        'code_emission': Decimal(row.get('Code Emission', 0)),
                        'compute_emission': Decimal(row.get('Compute Emission', 0)),
                        'community_emission': Decimal(row.get('Community Emission', 0)),
                        'protection_emission': Decimal(row.get('Protection Emission', 0)),
                        'total_emission': Decimal(row.get('Total Emission', 0)),
                        'total_supply': Decimal(row.get('Total Supply', 0))
                    }
                    emissions.append(emission)
                except (ValueError, TypeError) as e:
                    logger.error(f"Error parsing row {row}: {str(e)}")
                    continue
                    
        logger.info(f"Parsed {len(emissions)} emission records from {file_path}")
        return emissions
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        return []


def parse_emission_data_from_string(data_string, delimiter='\t'):
    """
    Parse emission data from a string.
    
    Args:
        data_string: String containing emission data
        delimiter: Delimiter used in the data string (default: tab)
        
    Returns:
        List of emission data dictionaries
    """
    emissions = []
    
    try:
        lines = data_string.strip().split('\n')
        header = lines[0].split(delimiter)
        
        for i in range(1, len(lines)):
            row = dict(zip(header, lines[i].split(delimiter)))
            
            # Convert date string to date object
            date_str = row.get('Date')
            if date_str:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            else:
                logger.warning(f"Skipping row with missing date: {row}")
                continue
            
            # Parse emission data
            try:
                emission = {
                    'day': int(row.get('Day', 0)),
                    'date': date_obj,
                    'capital_emission': Decimal(row.get('Capital Emission', 0)),
                    'code_emission': Decimal(row.get('Code Emission', 0)),
                    'compute_emission': Decimal(row.get('Compute Emission', 0)),
                    'community_emission': Decimal(row.get('Community Emission', 0)),
                    'protection_emission': Decimal(row.get('Protection Emission', 0)),
                    'total_emission': Decimal(row.get('Total Emission', 0)),
                    'total_supply': Decimal(row.get('Total Supply', 0))
                }
                emissions.append(emission)
            except (ValueError, TypeError) as e:
                logger.error(f"Error parsing row {row}: {str(e)}")
                continue
                
        logger.info(f"Parsed {len(emissions)} emission records from input string")
        return emissions
    except Exception as e:
        logger.error(f"Error parsing input string: {str(e)}")
        return []


def process_emission_events(emissions_data=None):
    """
    Process emission events and update emissions data in the database.
    
    Args:
        emissions_data: List of emission data dictionaries. If None or empty,
                       will attempt to fetch data from the configured source.
        
    Returns:
        Number of records inserted/updated
    """

    try:
        # If no data provided, try to fetch from the configured source
        if not emissions_data:
            logger.info("No emissions data provided, attempting to fetch from configured source")
            try:
                # Check if the CSV file exists
                csv_file_path = 'data/MASTER MOR EXPLORER - Emissions.csv'
                if os.path.exists(csv_file_path):
                    logger.info(f"Reading emissions data from {csv_file_path}")
                    emissions_data = parse_emission_data(csv_file_path)
                else:
                    # Import here to avoid circular imports
                    from helpers.staking_helpers.get_emission_schedule_for_today import get_emissions_data
                    
                    # Get emissions data from the configured source
                    emissions_df = get_emissions_data()
                    
                    if emissions_df is not None and not emissions_df.empty:
                        # Convert DataFrame to list of dictionaries
                        emissions_data = []
                        for _, row in emissions_df.iterrows():
                            try:
                                emission = {
                                    'day': int(row.get('Day', 0)),
                                    'date': row['Date'].date() if hasattr(row['Date'], 'date') else row['Date'],
                                    'capital_emission': Decimal(str(row['Capital Emission'])),
                                    'code_emission': Decimal(str(row['Code Emission'])),
                                    'compute_emission': Decimal(str(row['Compute Emission'])),
                                    'community_emission': Decimal(str(row['Community Emission'])),
                                    'protection_emission': Decimal(str(row['Protection Emission'])),
                                    'total_emission': Decimal(str(row['Total Emission'])),
                                    'total_supply': Decimal(str(row['Total Supply']))
                                }
                                emissions_data.append(emission)
                            except (ValueError, TypeError, KeyError) as e:
                                logger.error(f"Error parsing row {row}: {str(e)}")
                                continue
                        
                        logger.info(f"Fetched {len(emissions_data)} emission records from configured source")
                    else:
                        logger.warning("No emission data found in configured source")
                        return 0
            except Exception as e:
                logger.error(f"Error fetching emissions data from configured source: {str(e)}")
                return 0
        
        if not emissions_data:
            logger.warning("No emission data to update")
            return 0
        
        repository = EmissionRepository()
        count = repository.save_emission_data(emissions_data)
        logger.info(f"Successfully inserted/updated {count} {EVENT_NAME} records")
        return count
    except Exception as e:
        logger.error(f"Error updating emissions data: {str(e)}")
        return 0


def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Update emissions data in the database')
    parser.add_argument('--file', type=str, help='Path to the CSV or TSV file containing emissions data')
    parser.add_argument('--data', type=str, help='Emissions data as a string')
    parser.add_argument('--delimiter', type=str, default='\t', help='Delimiter for the data string (default: tab)')
    
    args = parser.parse_args()
    
    if args.file:
        emissions_data = parse_emission_data(args.file)
    elif args.data:
        emissions_data = parse_emission_data_from_string(args.data, args.delimiter)
    else:
        # No specific data source provided, use the default source
        emissions_data = []
    
    count = process_emission_events(emissions_data)
    
    if count > 0:
        logger.info(f"Successfully processed and stored {count} {EVENT_NAME} records")
        return 0
    else:
        logger.error(f"Failed to process {EVENT_NAME} records")
        return 1


if __name__ == "__main__":
    sys.exit(main())