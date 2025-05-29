import logging

from app.db.database import get_db
from app.repository import (
    UserClaimLockedRepository,
    UserMultiplierRepository,
    RewardSummaryRepository,
    CirculatingSupplyRepository,
    UserStakedEventsRepository,
    UserWithdrawnEventsRepository,
    OverplusBridgedEventsRepository
)

logger = logging.getLogger(__name__)

def get_last_block_from_db(table_name : str):
    """
    Get the last processed block number from a table.
    
    This function can be used in two ways:
    1. Using the legacy direct SQL approach
    2. Using the new repository approach
    
    Args:
        table_name: The name of the table to query
        
    Returns:
        The last processed block number, or None if no records exist
    """
    try:
        # Try to use the repository approach first
        if table_name == "user_claim_locked":
            repo = UserClaimLockedRepository()
            return repo.get_last_processed_block()
        # elif table_name == "user_multiplier":
        #     repo = UserMultiplierRepository()
        #     return repo.get_last_processed_block()
        elif table_name == "user_staked_events":
            repo = UserStakedEventsRepository()
            return repo.get_last_processed_block()
        elif table_name == "user_withdrawn_events":
            repo = UserWithdrawnEventsRepository()
            return repo.get_last_processed_block()
        elif table_name == "overplus_bridged_events":
            repo = OverplusBridgedEventsRepository()
            return repo.get_last_processed_block()
        else:
            # Fall back to the legacy approach for other tables
            db = get_db()
            with db.cursor() as cursor:
                cursor.execute(f"SELECT MAX(block_number) as last_block FROM {table_name}")
                result = cursor.fetchone()
                logger.info(f"sql result: {str(result)}")
                return int(result[0]) if result and result[0] is not None else None
    except Exception as e:
        logger.warning(f"Error getting last block from database: {str(e)}")
        return None