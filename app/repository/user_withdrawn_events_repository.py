"""
Repository for user_withdrawn_events table.
"""
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from app.models.database_models import UserWithdrawnEvent
from app.repository.base_repository import BaseRepository


class UserWithdrawnEventsRepository(BaseRepository[UserWithdrawnEvent]):
    """Repository for user_withdrawn_events table."""

    def __init__(self):
        """Initialize the repository."""
        super().__init__(UserWithdrawnEvent, "user_withdrawn_events")

    def get_by_transaction_hash(self, transaction_hash: str) -> Optional[UserWithdrawnEvent]:
        """
        Get a record by transaction hash.
        
        Args:
            transaction_hash: The transaction hash
            
        Returns:
            The record, or None if not found
        """
        sql = f"SELECT * FROM {self.table_name} WHERE transaction_hash = %s"
        result = self.db.fetchone(sql, [transaction_hash])
        return UserWithdrawnEvent(**result) if result else None

    def get_by_user_address(self, user_address: str, limit: int = 100, offset: int = 0) -> List[UserWithdrawnEvent]:
        """
        Get records by user address.
        
        Args:
            user_address: The user address
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of records
        """
        sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE user_address = %s 
        ORDER BY block_number DESC 
        LIMIT %s OFFSET %s
        """
        results = self.db.fetchall(sql, [user_address, limit, offset])
        return [UserWithdrawnEvent(**result) for result in results]

    def get_by_pool_id(self, pool_id: int, limit: int = 100, offset: int = 0) -> List[UserWithdrawnEvent]:
        """
        Get records by pool ID.
        
        Args:
            pool_id: The pool ID
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of records
        """
        sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE pool_id = %s 
        ORDER BY block_number DESC 
        LIMIT %s OFFSET %s
        """
        results = self.db.fetchall(sql, [pool_id, limit, offset])
        return [UserWithdrawnEvent(**result) for result in results]

    def get_by_user_and_pool(self, user_address: str, pool_id: int, limit: int = 100, offset: int = 0) -> List[UserWithdrawnEvent]:
        """
        Get records by user address and pool ID.
        
        Args:
            user_address: The user address
            pool_id: The pool ID
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of records
        """
        sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE user_address = %s AND pool_id = %s 
        ORDER BY block_number DESC 
        LIMIT %s OFFSET %s
        """
        results = self.db.fetchall(sql, [user_address, pool_id, limit, offset])
        return [UserWithdrawnEvent(**result) for result in results]

    def get_by_block_range(self, start_block: int, end_block: int) -> List[UserWithdrawnEvent]:
        """
        Get records by block range.
        
        Args:
            start_block: The start block number
            end_block: The end block number
            
        Returns:
            List of records
        """
        sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE block_number BETWEEN %s AND %s 
        ORDER BY block_number
        """
        results = self.db.fetchall(sql, [start_block, end_block])
        return [UserWithdrawnEvent(**result) for result in results]

    def get_by_date_range(self, start_date: datetime, end_date: datetime) -> List[UserWithdrawnEvent]:
        """
        Get records by date range.
        
        Args:
            start_date: The start date
            end_date: The end date
            
        Returns:
            List of records
        """
        sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE timestamp BETWEEN %s AND %s 
        ORDER BY timestamp
        """
        results = self.db.fetchall(sql, [start_date, end_date])
        return [UserWithdrawnEvent(**result) for result in results]

    def get_last_processed_block(self) -> Optional[int]:
        """
        Get the last processed block number.
        
        Returns:
            The last processed block number, or None if no records exist
        """
        sql = f"SELECT MAX(block_number) as last_block FROM {self.table_name}"
        result = self.db.fetchone(sql)
        return int(result[0]) if result and result[0] is not None else None

    def get_total_withdrawn_by_user(self, user_address: str) -> Dict[int, float]:
        """
        Get the total amount withdrawn by a user for each pool.
        
        Args:
            user_address: The user address
            
        Returns:
            Dictionary mapping pool_id to total withdrawn amount
        """
        sql = f"""
        SELECT 
            pool_id,
            SUM(amount) as total_withdrawn
        FROM {self.table_name} 
        WHERE user_address = %s 
        GROUP BY pool_id
        """
        results = self.db.fetchall(sql, [user_address])
        return {row['pool_id']: float(row['total_withdrawn']) for row in results}

    def get_total_withdrawn_by_pool(self) -> Dict[int, float]:
        """
        Get the total amount withdrawn for each pool.
        
        Returns:
            Dictionary mapping pool_id to total withdrawn amount
        """
        sql = f"""
        SELECT 
            pool_id,
            SUM(amount) as total_withdrawn
        FROM {self.table_name} 
        GROUP BY pool_id
        """
        results = self.db.fetchall(sql)
        return {row['pool_id']: float(row['total_withdrawn']) for row in results}

    def get_withdrawal_history(self, user_address: str, days: int = 30) -> List[Dict[str, any]]:
        """
        Get withdrawal history for a user over time.
        
        Args:
            user_address: The user address
            days: Number of days to retrieve
            
        Returns:
            List of daily withdrawal amounts
        """
        end_date = datetime.now()
        start_date = end_date - datetime.timedelta(days=days)
        
        sql = f"""
        SELECT 
            DATE(timestamp) as date,
            pool_id,
            SUM(amount) as daily_withdrawn
        FROM {self.table_name} 
        WHERE 
            user_address = %s AND
            timestamp BETWEEN %s AND %s
        GROUP BY date, pool_id
        ORDER BY date, pool_id
        """
        results = self.db.fetchall(sql, [user_address, start_date, end_date])
        
        # Group by date
        withdrawal_history = []
        for row in results:
            withdrawal_history.append({
                'date': row['date'].isoformat(),
                'pool_id': row['pool_id'],
                'amount': float(row['daily_withdrawn'])
            })
        
        return withdrawal_history

    def get_net_position(self, user_address: str) -> Dict[int, float]:
        """
        Calculate net position (staked - withdrawn) for a user by pool.
        This requires joining with the user_staked_events table.
        
        Args:
            user_address: The user address
            
        Returns:
            Dictionary mapping pool_id to net position
        """
        sql = """
        WITH staked AS (
            SELECT 
                pool_id,
                SUM(amount) as total_staked
            FROM user_staked_events
            WHERE user_address = %s
            GROUP BY pool_id
        ),
        withdrawn AS (
            SELECT 
                pool_id,
                SUM(amount) as total_withdrawn
            FROM user_withdrawn_events
            WHERE user_address = %s
            GROUP BY pool_id
        )
        SELECT 
            COALESCE(s.pool_id, w.pool_id) as pool_id,
            COALESCE(s.total_staked, 0) - COALESCE(w.total_withdrawn, 0) as net_position
        FROM staked s
        FULL OUTER JOIN withdrawn w ON s.pool_id = w.pool_id
        """
        results = self.db.fetchall(sql, [user_address, user_address])
        return {row['pool_id']: float(row['net_position']) for row in results}

    def bulk_insert(self, records: List[UserWithdrawnEvent]) -> int:
        """
        Insert multiple records at once.
        
        Args:
            records: List of records to insert
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        # Extract fields from the first record to determine columns
        sample = records[0].model_dump(exclude_none=True)
        if 'id' in sample:
            del sample['id']  # Remove id field for insertion

        columns = list(sample.keys())
        placeholders = ', '.join(['%s'] * len(columns))
        column_str = ', '.join(columns)

        # Prepare values for all records
        values_list = []
        for record in records:
            record_dict = record.model_dump(exclude_none=True)
            if 'id' in record_dict:
                del record_dict['id']
            values_list.append(tuple(record_dict[col] for col in columns))

        # Build and execute the query without ON CONFLICT handling
        sql = f"""
        INSERT INTO {self.table_name} ({column_str})
        VALUES ({placeholders})
        """

        with self.db.transaction() as cursor:
            cursor.executemany(sql, values_list)
            return len(values_list)