import logging
from app.repository import (
    UserClaimLockedRepository,
    UserMultiplierRepository,
    RewardSummaryRepository,
    CirculatingSupplyRepository,
    UserStakedEventsRepository,
    UserWithdrawnEventsRepository,
    OverplusBridgedEventsRepository,
    EmissionRepository
)

logger = logging.getLogger(__name__)

def ensure_user_claim_locked_table_exists():
    """Check if the table exists - table creation is handled by the seed script"""
    try:
        repository = UserClaimLockedRepository()
        TABLE_NAME = "user_claim_locked"
        # Check if the table exists
        if repository.count() >= 0:  # This will fail if the table doesn't exist
            logger.info(f"Table {TABLE_NAME} exists")
            return True
    except Exception as e:
        logger.error(f"Table {TABLE_NAME} does not exist. Run 'make seed' first to create all tables.")
        logger.error(f"Error checking if table exists: {str(e)}")
        raise Exception(f"Table {TABLE_NAME} does not exist")

def ensure_user_multiplier_table_exists():
    """Check if the table exists - table creation is handled by the seed script"""
    try:
        repository = UserMultiplierRepository()
        TABLE_NAME = "user_multiplier"
        # Check if the table exists
        if repository.count() >= 0:  # This will fail if the table doesn't exist
            logger.info(f"Table {TABLE_NAME} exists")
            return True
    except Exception as e:
        logger.error(f"Table {TABLE_NAME} does not exist. Run 'make seed' first to create all tables.")
        logger.error(f"Error checking if table exists: {str(e)}")
        raise Exception(f"Table {TABLE_NAME} does not exist")

def ensure_reward_summary_table_exists():
    """Check if the table exists - table creation is handled by the seed script"""
    try:
        repository = RewardSummaryRepository()
        TABLE_NAME = "reward_summary"
        # Check if the table exists
        if repository.count() >= 0:  # This will fail if the table doesn't exist
            logger.info(f"Table {TABLE_NAME} exists")
            return True
    except Exception as e:
        logger.error(f"Table {TABLE_NAME} does not exist. Run 'make seed' first to create all tables.")
        logger.error(f"Error checking if table exists: {str(e)}")
        raise Exception(f"Table {TABLE_NAME} does not exist")

def ensure_circulating_supply_table_exists():
    """Check if the table exists - table creation is handled by the seed script"""
    try:
        repository = CirculatingSupplyRepository()
        TABLE_NAME = "circulating_supply"
        # Check if the table exists
        if repository.count() >= 0:  # This will fail if the table doesn't exist
            logger.info(f"Table {TABLE_NAME} exists")
            return True
    except Exception as e:
        logger.error(f"Table {TABLE_NAME} does not exist. Run 'make seed' first to create all tables.")
        logger.error(f"Error checking if table exists: {str(e)}")
        raise Exception(f"Table {TABLE_NAME} does not exist")

def ensure_user_staked_events_table_exists():
    """Check if the table exists - table creation is handled by the seed script"""
    try:
        repository = UserStakedEventsRepository()
        TABLE_NAME = "user_staked_events"
        # Check if the table exists
        if repository.count() >= 0:  # This will fail if the table doesn't exist
            logger.info(f"Table {TABLE_NAME} exists")
            return True
    except Exception as e:
        logger.error(f"Table {TABLE_NAME} does not exist. Run 'make seed' first to create all tables.")
        logger.error(f"Error checking if table exists: {str(e)}")
        raise Exception(f"Table {TABLE_NAME} does not exist")

def ensure_user_withdrawn_events_table_exists():
    """Check if the table exists - table creation is handled by the seed script"""
    try:
        repository = UserWithdrawnEventsRepository()
        TABLE_NAME = "user_withdrawn_events"
        # Check if the table exists
        if repository.count() >= 0:  # This will fail if the table doesn't exist
            logger.info(f"Table {TABLE_NAME} exists")
            return True
    except Exception as e:
        logger.error(f"Table {TABLE_NAME} does not exist. Run 'make seed' first to create all tables.")
        logger.error(f"Error checking if table exists: {str(e)}")
        raise Exception(f"Table {TABLE_NAME} does not exist")

def ensure_overplus_bridged_events_table_exists():
    """Check if the table exists - table creation is handled by the seed script"""
    try:
        repository = OverplusBridgedEventsRepository()
        TABLE_NAME = "overplus_bridged_events"
        # Check if the table exists
        if repository.count() >= 0:  # This will fail if the table doesn't exist
            logger.info(f"Table {TABLE_NAME} exists")
            return True
    except Exception as e:
        logger.error(f"Table {TABLE_NAME} does not exist. Run 'make seed' first to create all tables.")
        logger.error(f"Error checking if table exists: {str(e)}")
        raise Exception(f"Table {TABLE_NAME} does not exist")

def ensure_emission_table_exists():
    """Check if the table exists - table creation is handled by the seed script"""
    try:
        repository = EmissionRepository()
        TABLE_NAME = "emissions"
        # Check if the table exists
        if repository.count() >= 0:  # This will fail if the table doesn't exist
            logger.info(f"Table {TABLE_NAME} exists")
            return True
    except Exception as e:
        logger.error(f"Table {TABLE_NAME} does not exist. Run 'make seed' first to create all tables.")
        logger.error(f"Error checking if table exists: {str(e)}")
        raise Exception(f"Table {TABLE_NAME} does not exist")