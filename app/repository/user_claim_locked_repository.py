"""
Repository for user_claim_locked table.
"""
from datetime import datetime
from typing import List, Optional

from app.models.database_models import UserClaimLocked
from app.repository.base_repository import BaseRepository


class UserClaimLockedRepository(BaseRepository[UserClaimLocked]):
    """Repository for user_claim_locked table."""

    def __init__(self):
        """Initialize the repository."""
        super().__init__(UserClaimLocked, "user_claim_locked")

    def get_by_transaction_hash(self, transaction_hash: str) -> Optional[UserClaimLocked]:
        """
        Get a record by transaction hash.
        
        Args:
            transaction_hash: The transaction hash
            
        Returns:
            The record, or None if not found
        """
        sql = f"SELECT * FROM {self.table_name} WHERE transaction_hash = %s"
        result = self.db.fetchone(sql, [transaction_hash])
        return UserClaimLocked(**result) if result else None
    
    def get_unique_user_pool_combinations(self) -> List[UserClaimLocked]:
        """
        Get all unique combinations of pool ID and user address as UserClaimLocked objects.
        If a user is in multiple pools, they will appear once for each pool they're in.
        
        Returns:
            List of UserClaimLocked objects representing unique user-pool combinations
        """
        sql = f"""
        SELECT DISTINCT pool_id, *
        FROM {self.table_name}
        ORDER BY pool_id, user_address
        """
    
        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql)
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch all results
            results = cur.fetchall()
            
            # Convert tuples to dictionaries using column names
            dict_results = []
            for result in results:
                dict_result = dict(zip(columns, result))
                dict_results.append(dict_result)
        
        # Create model instances from dictionaries
        return [UserClaimLocked(**dict_result) for dict_result in dict_results]

    def get_by_user(self, user_address: str, limit: int = 100, offset: int = 0) -> List[UserClaimLocked]:
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
        WHERE user = %s 
        ORDER BY block_number DESC 
        LIMIT %s OFFSET %s
        """
        results = self.db.fetchall(sql, [user_address, limit, offset])
        return [UserClaimLocked(**result) for result in results]

    def get_by_pool_id(self, pool_id: int, limit: int = 100, offset: int = 0) -> List[UserClaimLocked]:
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
        WHERE poolid = %s 
        ORDER BY block_number DESC 
        LIMIT %s OFFSET %s
        """
        results = self.db.fetchall(sql, [pool_id, limit, offset])
        return [UserClaimLocked(**result) for result in results]

    def get_by_block_range(self, start_block: int, end_block: int) -> List[UserClaimLocked]:
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
        return [UserClaimLocked(**result) for result in results]

    def get_by_date_range(self, start_date: datetime, end_date: datetime) -> List[UserClaimLocked]:
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
        return [UserClaimLocked(**result) for result in results]

    def get_last_processed_block(self) -> Optional[int]:
        """
        Get the last processed block number.
        
        Returns:
            The last processed block number, or None if no records exist
        """
        sql = f"SELECT MAX(block_number) as last_block FROM {self.table_name}"
        result = self.db.fetchone(sql)
        return int(result[0]) if result and result[0] is not None else None

    def bulk_insert(self, records: List[UserClaimLocked]) -> int:
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

        # Build and execute the query
        sql = f"""
        INSERT INTO {self.table_name} ({column_str})
        VALUES ({placeholders})
        """

        with self.db.transaction() as cursor:
            cursor.executemany(sql, values_list)
            return len(values_list)