from app.repository import UserStakedEventsRepository
import logging

logger = logging.getLogger(__name__)

def ensure_user_staked_events_table_exists():
    """
    Create the user_staked_events table if it doesn't exist.
    
    This function now uses the repository approach, which handles table creation.
    """
    try:
        # Creating an instance of the repository will ensure the table exists
        # because the repository's methods check for and create the table if needed
        repo = UserStakedEventsRepository()
        logger.info("Ensured user_staked_events table exists with required structure")
        return True
    except Exception as e:
        logger.error(f"Error ensuring table exists: {str(e)}")
        raise


def insert_events_to_db(events_data):
    """
    Insert event data into the database using the repository approach.
    
    Args:
        events_data: List of event data dictionaries
        
    Returns:
        Number of records inserted
    """
    if not events_data:
        return 0

    try:
        # Convert the event data to model instances
        from app.models.database_models import UserStakedEvent
        
        models = []
        for event in events_data:
            model = UserStakedEvent(
                timestamp=event['timestamp'],
                transaction_hash=event['transaction_hash'],
                block_number=event['block_number'],
                pool_id=event['pool_id'],
                user_address=event['user_address'],
                amount=event['amount']
            )
            models.append(model)
        
        # Use the repository to bulk insert the models
        repo = UserStakedEventsRepository()
        inserted_count = repo.bulk_insert(models)
        
        logger.info(f"Inserted {inserted_count} new records into database")
        return inserted_count
    except Exception as e:
        logger.error(f"Error inserting events to database: {str(e)}")
        raise
