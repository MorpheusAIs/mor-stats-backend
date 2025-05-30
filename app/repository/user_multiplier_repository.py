"""
Repository for user_multiplier table.
"""
from datetime import datetime
from typing import List, Optional

from app.models.database_models import UserMultiplier
from app.repository.base_repository import BaseRepository


class UserMultiplierRepository(BaseRepository[UserMultiplier]):
    """Repository for user_multiplier table."""

    def __init__(self):
        """Initialize the repository."""
        super().__init__(UserMultiplier, "user_multiplier")

    def get_by_user_claim_locked_id(self, user_claim_locked_id: int) -> Optional[UserMultiplier]:
        """
        Get a record by user_claim_locked_id.
        
        Args:
            user_claim_locked_id: The user_claim_locked_id
            
        Returns:
            The record, or None if not found
        """
        sql = f"SELECT * FROM {self.table_name} WHERE user_claim_locked_id = %s"
        
        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, [user_claim_locked_id])
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch the result
            result = cur.fetchone()
            
            if result:
                # Convert tuple to dictionary using column names
                dict_result = dict(zip(columns, result))
                return UserMultiplier(**dict_result)
            else:
                return None

    def get_by_user_address(self, user_address: str, limit: int = 100, offset: int = 0) -> List[UserMultiplier]:
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
        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, [user_address, limit, offset])
            
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
            return [UserMultiplier(**dict_result) for dict_result in dict_results]

    def get_by_pool_id(self, pool_id: int, limit: int = 100, offset: int = 0) -> List[UserMultiplier]:
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
        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, [pool_id, limit, offset])
            
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
            return [UserMultiplier(**dict_result) for dict_result in dict_results]

    def get_by_user_and_pool(self, user_address: str, pool_id: int) -> List[UserMultiplier]:
        """
        Get records by user address and pool ID.
        
        Args:
            user_address: The user address
            pool_id: The pool ID
            
        Returns:
            List of records
        """
        sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE user_address = %s AND pool_id = %s 
        ORDER BY block_number DESC
        """
        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, [user_address, pool_id])
            
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
            return [UserMultiplier(**dict_result) for dict_result in dict_results]

    def get_latest_by_user_and_pool(self, user_address: str, pool_id: int) -> Optional[UserMultiplier]:
        """
        Get the latest record by user address and pool ID.
        
        Args:
            user_address: The user address
            pool_id: The pool ID
            
        Returns:
            The latest record, or None if not found
        """
        sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE user_address = %s AND pool_id = %s 
        ORDER BY block_number DESC 
        LIMIT 1
        """
        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, [user_address, pool_id])
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch the result
            result = cur.fetchone()
            
            if result:
                # Convert tuple to dictionary using column names
                dict_result = dict(zip(columns, result))
                return UserMultiplier(**dict_result)
            else:
                return None

    def get_by_date_range(self, start_date: datetime, end_date: datetime) -> List[UserMultiplier]:
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
        # Execute the query directly with a cursor to get column names
        with self.db.cursor() as cur:
            cur.execute(sql, [start_date, end_date])
            
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
            return [UserMultiplier(**dict_result) for dict_result in dict_results]

    def get_unprocessed_records(self) -> List[dict]:
        """
        Get records from user_claim_locked that haven't been processed yet.
        
        Returns:
            List of unprocessed records
        """
        sql = """
        SELECT ucl.id, ucl.timestamp, ucl.transaction_hash, ucl.block_number, ucl.pool_id as pool_id, ucl.user_address as user_address
        FROM user_claim_locked ucl
        LEFT JOIN user_multiplier um
        ON ucl.id = um.user_claim_locked_id
        WHERE um.id IS NULL
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
            
            return dict_results

    def bulk_insert(self, records: List[UserMultiplier]) -> int:
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

        columns = list(sample.keys())
        placeholders = ', '.join(['%s'] * len(columns))
        column_str = ', '.join(columns)

        # Prepare values for all records
        values_list = []
        for record in records:
            record_dict = record.model_dump(exclude_none=True)
            values_list.append(tuple(record_dict[col] for col in columns))

        # Build and execute the query without ON CONFLICT handling
        sql = f"""
        INSERT INTO {self.table_name} ({column_str})
        VALUES ({placeholders})
        """

        with self.db.transaction() as cursor:
            cursor.executemany(sql, values_list)
            return len(values_list)
    
    def clean_table(self) -> bool:
        sql = f"""
        DELETE FROM {self.table_name}
        """

        with self.db.transaction() as cursor:
            cursor.execute(sql)
        
        return True
