"""
Script to import initial emissions data from the provided CSV file.

This script reads the emission data from 'data/MASTER MOR EXPLORER - Emissions.csv'
and imports it into the emissions table.
"""
import os
import sys
import logging
import csv
from datetime import datetime
from decimal import Decimal

# Add the parent directory to the path so we can import from the app package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.repository import EmissionRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Path to the CSV file
CSV_FILE_PATH = 'data/MASTER MOR EXPLORER - Emissions.csv'


def import_emissions_from_csv(csv_file_path):
    """
    Import emissions data from the CSV file.
    
    Args:
        csv_file_path: Path to the CSV file
        
    Returns:
        Number of records imported
    """
    try:
        emissions_data = []
        
        with open(csv_file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    # Parse the date
                    date_str = row.get('Date')
                    if not date_str:
                        logger.warning(f"Skipping row with missing date: {row}")
                        continue
                        
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                    
                    # Create emission record
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
                    emissions_data.append(emission)
                except (ValueError, TypeError, KeyError) as e:
                    logger.error(f"Error parsing row {row}: {str(e)}")
                    continue
        
        if not emissions_data:
            logger.warning("No emission data found in the CSV file")
            return 0
            
        # Save to the database
        repository = EmissionRepository()
        result = repository.save_emission_data(emissions_data)
        
        logger.info(f"Successfully imported {result} emission records")
        return result
        
    except Exception as e:
        logger.error(f"Error importing emissions data: {str(e)}")
        return 0


def main():
    """Import the initial emissions data."""
    try:
        # Check if the CSV file exists
        if not os.path.exists(CSV_FILE_PATH):
            logger.error(f"CSV file not found: {CSV_FILE_PATH}")
            return 1
            
        # Import emissions data from the CSV file
        count = import_emissions_from_csv(CSV_FILE_PATH)
        
        if count > 0:
            logger.info(f"Successfully imported {count} emission records")
            return 0
        else:
            logger.error("Failed to import emission records")
            return 1
            
    except Exception as e:
        logger.error(f"Error importing emission data: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())