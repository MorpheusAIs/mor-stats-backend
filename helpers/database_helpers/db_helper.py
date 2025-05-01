import logging

from app.db.database import get_db

logger = logging.getLogger(__name__)

def get_last_block_from_db(table_name : str):
    try:
        db = get_db()

        with db.cursor() as cursor:
            cursor.execute(f"SELECT MAX(block_number) FROM {table_name}")
            result = cursor.fetchone()[0]
            return int(result) if result else None
    except Exception as e:
        logger.warning(f"Error getting last block from database: {str(e)}")
        return None