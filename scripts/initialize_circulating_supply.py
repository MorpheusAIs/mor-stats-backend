import logging
import csv
import os
from datetime import datetime
from decimal import Decimal

from app.db.database import get_db
from app.models.database_models import CirculatingSupply
from app.repository.circulating_supply_repository import CirculatingSupplyRepository

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Path to the CSV file
CSV_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                            'data', 'MASTER MOR EXPLORER - CircSupply.csv')

def ensure_table_exists():
    """Create the circulating supply table if it doesn't exist"""
    db = get_db()
    try:
        with db.cursor() as cursor:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS circulating_supply (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                circulating_supply_at_that_date NUMERIC(36, 18) NOT NULL,
                block_timestamp_at_that_date BIGINT NOT NULL,
                total_claimed_that_day NUMERIC(36, 18) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)

            # Create indexes for efficient lookups
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_circulating_supply_date ON circulating_supply (date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_circulating_supply_timestamp ON circulating_supply (block_timestamp_at_that_date)")

            # Add unique constraint on date
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_circulating_supply_date_unique ON circulating_supply (date)")

            logger.info("Ensured table circulating_supply exists with required structure")
    except Exception as e:
        logger.error(f"Error ensuring table exists: {str(e)}")
        raise

def import_data_from_csv():
    """Import data from the CSV file into the circulating supply table"""
    try:
        if not os.path.exists(CSV_FILE_PATH):
            logger.error(f"CSV file not found at: {CSV_FILE_PATH}")
            return 0
        
        logger.info(f"Importing data from CSV file: {CSV_FILE_PATH}")
        
        # Read data from CSV file
        records = []
        with open(CSV_FILE_PATH, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                try:
                    # Convert date string to date object
                    date_obj = datetime.strptime(row["date"], "%d/%m/%Y").date()
                    
                    # Create CirculatingSupply object
                    record = CirculatingSupply(
                        date=date_obj,
                        circulating_supply_at_that_date=Decimal(str(row["circulating_supply_at_that_date"])),
                        block_timestamp_at_that_date=int(row["block_timestamp_at_that_date"]),
                        total_claimed_that_day=Decimal(str(row["total_claimed_that_day"]))
                    )
                    records.append(record)
                except Exception as e:
                    logger.warning(f"Error processing row: {row}. Error: {str(e)}")
                    continue
        
        # Insert records into database
        repo = CirculatingSupplyRepository()
        count = repo.bulk_insert(records)
        logger.info(f"Imported {count} records from CSV file")
        return count
    
    except Exception as e:
        logger.error(f"Error importing data from CSV: {str(e)}")
        raise

def main():
    """Main function to initialize the circulating supply table"""
    try:
        # Ensure the table exists
        ensure_table_exists()
        
        # Import data from CSV
        count = import_data_from_csv()
        
        if count > 0:
            logger.info(f"Successfully imported {count} records into circulating_supply table")
        else:
            logger.warning("No records were imported. Check the CSV file and logs for errors.")
            
    except Exception as e:
        logger.error(f"Error initializing circulating supply table: {str(e)}")
        raise

if __name__ == "__main__":
    main()