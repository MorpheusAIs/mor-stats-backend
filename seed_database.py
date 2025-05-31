"""
Consolidated database seeding script.

This script handles seeding all tables from CSV files in the data directory.
It provides a unified approach to database initialization.
"""
import csv
import logging
import os
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Type, Callable

from app.core.settings import settings
from app.core.exceptions import DatabaseError
from app.db.database import DBConfig, init_db
from app.models.database_models import (
    CirculatingSupply,
    Emission,
    UserClaimLocked,
    UserMultiplier,
    RewardSummary,
    UserStakedEvent,
    UserWithdrawnEvent,
    OverplusBridgedEvent
)
from app.repository.circulating_supply_repository import CirculatingSupplyRepository
from app.repository.emission_repository import EmissionRepository
from app.repository.user_claim_locked_repository import UserClaimLockedRepository
from app.repository.user_multiplier_repository import UserMultiplierRepository
from app.repository.reward_repository import RewardSummaryRepository
from app.repository.user_staked_events_repository import UserStakedEventsRepository
from app.repository.user_withdrawn_events_repository import UserWithdrawnEventsRepository
from app.repository.overplus_bridged_events_repository import OverplusBridgedEventsRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Base directory for data files
DATA_DIR = './data'

# Define table creation SQL statements
# Using a list to ensure tables are created in the correct order (important for foreign key constraints)
TABLE_DEFINITIONS = [
    # Basic tables with no dependencies
    ("circulating_supply", """
        CREATE TABLE IF NOT EXISTS circulating_supply (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            circulating_supply_at_that_date NUMERIC(36, 18) NOT NULL,
            block_timestamp_at_that_date BIGINT NOT NULL,
            total_claimed_that_day NUMERIC(36, 18) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_circulating_supply_date ON circulating_supply (date);
        CREATE INDEX IF NOT EXISTS idx_circulating_supply_timestamp ON circulating_supply (block_timestamp_at_that_date);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_circulating_supply_date_unique ON circulating_supply (date);
    """),
    ("emissions", """
        CREATE TABLE IF NOT EXISTS emissions (
            id SERIAL PRIMARY KEY,
            day INTEGER NOT NULL,
            date DATE NOT NULL,
            capital_emission NUMERIC(36, 18) NOT NULL,
            code_emission NUMERIC(36, 18) NOT NULL,
            compute_emission NUMERIC(36, 18) NOT NULL,
            community_emission NUMERIC(36, 18) NOT NULL,
            protection_emission NUMERIC(36, 18) NOT NULL,
            total_emission NUMERIC(36, 18) NOT NULL,
            total_supply NUMERIC(36, 18) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_emissions_date ON emissions (date);
        CREATE INDEX IF NOT EXISTS idx_emissions_day ON emissions (day);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_emissions_date_unique ON emissions (date);
    """),
    ("user_claim_locked", """
        CREATE TABLE IF NOT EXISTS user_claim_locked (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            transaction_hash varchar(255) NOT NULL,
            block_number BIGINT NOT NULL,
            pool_id INTEGER NOT NULL,
            user_address varchar(255) NOT NULL,
            claim_lock_start bigint NOT NULL,
            claim_lock_end bigint NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_user_claim_locked_block_number ON user_claim_locked (block_number);
        CREATE INDEX IF NOT EXISTS idx_user_claim_locked_user ON user_claim_locked (user_address);
    """),
    # Tables with dependencies - user_multiplier depends on user_claim_locked
    ("user_multiplier", """
        CREATE TABLE IF NOT EXISTS user_multiplier (
            id SERIAL PRIMARY KEY,
            user_claim_locked_start bigint NOT NULL,
            user_claim_locked_end bigint NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            transaction_hash TEXT NOT NULL,
            block_number BIGINT NOT NULL,
            pool_id INTEGER NOT NULL,
            user_address varchar(255) NOT NULL,
            multiplier NUMERIC(78, 38),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_user_multiplier_user ON user_multiplier (user_address);
    """),
    ("reward_summary", """
        CREATE TABLE IF NOT EXISTS reward_summary (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            calculation_block_current BIGINT NOT NULL,
            calculation_block_past BIGINT NOT NULL,
            daily_pool_reward_0 NUMERIC(36, 18) NOT NULL,
            daily_pool_reward_1 NUMERIC(36, 18) NOT NULL,
            daily_reward NUMERIC(36, 18) NOT NULL,
            total_reward_pool_0 NUMERIC(36, 18) NOT NULL,
            total_reward_pool_1 NUMERIC(36, 18) NOT NULL,
            total_reward NUMERIC(36, 18) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """),
    ("user_staked_events", """
        CREATE TABLE IF NOT EXISTS user_staked_events (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            transaction_hash varchar(255) NOT NULL,
            block_number BIGINT NOT NULL,
            pool_id INTEGER NOT NULL,
            user_address varchar(255) NOT NULL,
            amount NUMERIC(78, 38) NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_user_staked_events_block_number ON user_staked_events (block_number);
        CREATE INDEX IF NOT EXISTS idx_user_staked_events_user ON user_staked_events (user_address);
        CREATE INDEX IF NOT EXISTS idx_user_staked_events_pool ON user_staked_events (pool_id);
    """),
    ("user_withdrawn_events", """
        CREATE TABLE IF NOT EXISTS user_withdrawn_events (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            transaction_hash varchar(255) NOT NULL,
            block_number BIGINT NOT NULL,
            pool_id INTEGER NOT NULL,
            user_address varchar(255) NOT NULL,
            amount NUMERIC(78, 38) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_user_withdrawn_events_block_number ON user_withdrawn_events (block_number);
        CREATE INDEX IF NOT EXISTS idx_user_withdrawn_events_user ON user_withdrawn_events (user_address);
        CREATE INDEX IF NOT EXISTS idx_user_withdrawn_events_pool ON user_withdrawn_events (pool_id);
    """),
    ("overplus_bridged_events", """
        CREATE TABLE IF NOT EXISTS overplus_bridged_events (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            transaction_hash varchar(255) NOT NULL,
            block_number BIGINT NOT NULL,
            amount NUMERIC(78, 38) NOT NULL,
            unique_id varchar(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_overplus_bridged_events_block_number ON overplus_bridged_events (block_number);
        CREATE INDEX IF NOT EXISTS idx_overplus_bridged_events_unique_id ON overplus_bridged_events (unique_id);
    """)
]

# Define CSV file parsers for each data type
def parse_circulating_supply(row: Dict[str, str]) -> CirculatingSupply:
    """Parse a CSV row into a CirculatingSupply object."""
    date_obj = datetime.strptime(row["date"], "%d/%m/%Y").date()
    return CirculatingSupply(
        date=date_obj,
        circulating_supply_at_that_date=Decimal(str(row["circulating_supply_at_that_date"])),
        block_timestamp_at_that_date=int(row["block_timestamp_at_that_date"]),
        total_claimed_that_day=Decimal(str(row["total_claimed_that_day"]))
    )

def parse_emission(row: Dict[str, str]) -> Emission:
    """Parse a CSV row into an Emission object."""
    date_obj = datetime.strptime(row["Date"], "%Y-%m-%d").date()
    return Emission(
        day=int(row["Day"]),
        date=date_obj,
        capital_emission=Decimal(row["Capital Emission"]),
        code_emission=Decimal(row["Code Emission"]),
        compute_emission=Decimal(row["Compute Emission"]),
        community_emission=Decimal(row["Community Emission"]),
        protection_emission=Decimal(row["Protection Emission"]),
        total_emission=Decimal(row["Total Emission"]),
        total_supply=Decimal(row["Total Supply"])
    )

def parse_user_claim_locked(row: Dict[str, str]) -> UserClaimLocked:
    """Parse a CSV row into a UserClaimLocked object."""
    timestamp = datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S")
    return UserClaimLocked(
        timestamp=timestamp,
        transaction_hash=row["TransactionHash"],
        block_number=int(row["BlockNumber"]),
        pool_id=int(row["poolId"]),
        user_address=row["user"],
        claim_lock_start=row["claimLockStart"],
        claim_lock_end=row["claimLockEnd"]
    )

def parse_user_multiplier(row: Dict[str, str]) -> UserMultiplier:
    """Parse a CSV row into a UserMultiplier object."""
    timestamp = datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S")
    
    # Handle multiplier with error checking
    multiplier = None
    if row.get("multiplier"):
        try:
            # Handle both regular numbers and scientific notation
            # Keep full precision
            multiplier = Decimal(row["multiplier"])
        except (ValueError, InvalidOperation):
            # If there's any issue, log it and try a different approach
            logger.warning(f"Error converting multiplier '{row['multiplier']}' to Decimal. Attempting fallback conversion.")
            # Try to clean the string and convert again
            try:
                cleaned_multiplier = row["multiplier"].strip().replace(',', '')
                multiplier = Decimal(cleaned_multiplier)
            except (ValueError, InvalidOperation) as e:
                logger.error(f"Failed to parse multiplier value '{row['multiplier']}' after cleaning: {str(e)}")
                multiplier = None
    
    return UserMultiplier(
        user_claim_locked_start=int(row["claimLockStart"]),
        user_claim_locked_end=int(row["claimLockEnd"]),
        timestamp=timestamp,
        transaction_hash=row["TransactionHash"],
        block_number=int(row["BlockNumber"]),
        pool_id=int(row["poolId"]),
        user_address=row["user"],
        multiplier=multiplier,
    )

def parse_reward_summary(row: Dict[str, str]) -> RewardSummary:
    """Parse a CSV row into a RewardSummary object."""
    timestamp = datetime.fromtimestamp(int(row["block_timestamp_at_that_date"]))
    return RewardSummary(
        timestamp=timestamp,
        calculation_block_current=int(row["calculation_block_current"]),
        calculation_block_past=int(row["calculation_block_past"]),
        category=row["category"],
        value=Decimal(row["value"])
    )

def parse_user_staked_event(row: Dict[str, str]) -> UserStakedEvent:
    """Parse a CSV row into a UserStakedEvent object."""
    timestamp = datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S")
    
    # Convert amount to Decimal to maintain precision
    try:
        # Handle both regular numbers and scientific notation
        amount = Decimal(row["Amount"])
    except (ValueError, InvalidOperation):
        # If there's any issue, log it and try a different approach
        logger.warning(f"Error converting amount '{row['Amount']}' to Decimal. Attempting fallback conversion.")
        # Try to clean the string and convert again
        cleaned_amount = row["Amount"].strip().replace(',', '')
        amount = Decimal(cleaned_amount)
    
    return UserStakedEvent(
        timestamp=timestamp,
        transaction_hash=row["TransactionHash"],
        block_number=int(row["BlockNumber"]),
        pool_id=int(row["PoolId"]),
        user_address=row["User"],
        amount=amount
    )

def parse_user_withdrawn_event(row: Dict[str, str]) -> UserWithdrawnEvent:
    """Parse a CSV row into a UserWithdrawnEvent object."""
    timestamp = datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S")
    return UserWithdrawnEvent(
        timestamp=timestamp,
        transaction_hash=row["TransactionHash"],
        block_number=int(row["BlockNumber"]),
        pool_id=int(row["PoolId"]),
        user_address=row["User"],
        amount=Decimal(row["Amount"])
    )

def parse_overplus_bridged_event(row: Dict[str, str]) -> OverplusBridgedEvent:
    """Parse a CSV row into an OverplusBridgedEvent object."""
    timestamp = datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S")
    return OverplusBridgedEvent(
        timestamp=timestamp,
        transaction_hash=row["TransactionHash"],
        block_number=int(row["BlockNumber"]),
        amount=Decimal(row["amount"]),
        unique_id=row["uniqueId"]
    )

# Define mapping between CSV files, models, repositories, and parsers
DATA_MAPPING = [
    {
        "csv_file": "MASTER MOR EXPLORER - CircSupply.csv",
        "table_name": "circulating_supply",
        "repository_class": CirculatingSupplyRepository,
        "parser": parse_circulating_supply
    },
    {
        "csv_file": "MASTER MOR EXPLORER - Emissions.csv",
        "table_name": "emissions",
        "repository_class": EmissionRepository,
        "parser": parse_emission
    },
    {
        "csv_file": "MASTER MOR EXPLORER - UserClaimLocked.csv",
        "table_name": "user_claim_locked",
        "repository_class": UserClaimLockedRepository,
        "parser": parse_user_claim_locked
    },
    {
        "csv_file": "MASTER MOR EXPLORER - UserMultiplier.csv",
        "table_name": "user_multiplier",
        "repository_class": UserMultiplierRepository,
        "parser": parse_user_multiplier
    },
    {
        "table_name": "reward_summary",
        "repository_class": RewardSummaryRepository,
    },
    {
        "csv_file": "MASTER MOR EXPLORER - UserStaked.csv",
        "table_name": "user_staked_events",
        "repository_class": UserStakedEventsRepository,
        "parser": parse_user_staked_event
    },
    {
        "csv_file": "MASTER MOR EXPLORER - UserWithdrawn.csv",
        "table_name": "user_withdrawn_events",
        "repository_class": UserWithdrawnEventsRepository,
        "parser": parse_user_withdrawn_event
    },
    {
        "csv_file": "MASTER MOR EXPLORER - OverplusBridged.csv",
        "table_name": "overplus_bridged_events",
        "repository_class": OverplusBridgedEventsRepository,
        "parser": parse_overplus_bridged_event
    }
]

def ensure_tables_exist():
    """Create all tables if they don't exist."""
    config = DBConfig(
        host=settings.database.host,
        port=settings.database.port,
        database=settings.database.database,
        user=settings.database.user,
        password=settings.database.password,
        minconn=settings.database.minconn,
        maxconn=settings.database.maxconn,
        autocommit=settings.database.autocommit,
    )

    db = init_db(config)
    try:
        if not db.health_check():
            raise DatabaseError("Database health check failed")
        logger.info("Database connection check successful")
    except Exception as e:
        raise DatabaseError("Database connection error", details={"error": str(e)})
    try:
        with db.cursor() as cursor:
            for table_name, create_sql in TABLE_DEFINITIONS:
                cursor.execute(create_sql)
                logger.info(f"Ensured table {table_name} exists with required structure")
    except Exception as e:
        logger.error(f"Error ensuring tables exist: {str(e)}")
        raise

def import_data_from_csv(csv_file: str, parser: Callable, repository_class: Type) -> int:
    """
    Import data from a CSV file into the database.
    
    Args:
        csv_file: Path to the CSV file
        parser: Function to parse CSV rows into model objects
        repository_class: Repository class to use for database operations
        
    Returns:
        Number of records imported
    """
    try:
        csv_path = os.path.join(DATA_DIR, csv_file)
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return 0
        
        logger.info(f"Importing data from CSV file: {csv_path}")
        
        # Read data from CSV file
        records = []
        with open(csv_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                try:
                    record = parser(row)
                    records.append(record)
                except Exception as e:
                    logger.warning(f"{file.name} - Error processing row: {row}. Error: {str(e)}")
                    continue
        
        if not records:
            logger.warning(f"No valid records found in {csv_file}")
            return 0
        
        # Insert records into database
        repository = repository_class()
        count = repository.bulk_insert(records)
        logger.info(f"Imported {count} records from {csv_file}")
        return count
    
    except Exception as e:
        logger.error(f"Error importing data from {csv_file}: {str(e)}")
        raise

def seed_database():
    """Seed the database with data from all CSV files."""
    try:
        # Ensure all tables exist
        ensure_tables_exist()
        
        # Import data from each CSV file
        total_count = 0
        for mapping in DATA_MAPPING:
            try:
                count = import_data_from_csv(
                    mapping["csv_file"],
                    mapping["parser"],
                    mapping["repository_class"]
                )
                total_count += count
                logger.info(f"Successfully imported {count} records into {mapping['table_name']} table")
            except Exception as e:
                logger.error(f"Error importing data for {mapping['table_name']}: {str(e)}")
                continue
        
        return total_count
    
    except Exception as e:
        logger.error(f"Error seeding database: {str(e)}")
        raise

def main():
    """Main function to seed the database."""
    try:
        count = seed_database()
        if count > 0:
            logger.info(f"Successfully imported {count} total records into the database")
        else:
            logger.warning("No records were imported. Check the CSV files and logs for errors.")
        return 0
    except Exception as e:
        logger.error(f"Error seeding database: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())