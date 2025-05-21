"""
Repository for overplus_bridged_events table.
"""
from datetime import datetime
from typing import Dict, List, Optional

from app.models.database_models import OverplusBridgedEvent
from app.repository.base_repository import BaseRepository


class OverplusBridgedEventsRepository(BaseRepository[OverplusBridgedEvent]):
    """Repository for overplus_bridged_events table."""

    def __init__(self):
        """Initialize the repository."""
        super().__init__(OverplusBridgedEvent, "overplus_bridged_events")

    def get_by_transaction_hash(self, transaction_hash: str) -> Optional[OverplusBridgedEvent]:
        """
        Get a record by transaction hash.
        
        Args:
            transaction_hash: The transaction hash
            
        Returns:
            The record, or None if not found
        """
        sql = f"SELECT * FROM {self.table_name} WHERE transaction_hash = %s"
        result = self.db.fetchone(sql, [transaction_hash])
        return OverplusBridgedEvent(**result) if result else None

    def get_by_unique_id(self, unique_id: str) -> Optional[OverplusBridgedEvent]:
        """
        Get a record by unique ID.
        
        Args:
            unique_id: The unique ID
            
        Returns:
            The record, or None if not found
        """
        sql = f"SELECT * FROM {self.table_name} WHERE unique_id = %s"
        result = self.db.fetchone(sql, [unique_id])
        return OverplusBridgedEvent(**result) if result else None

    def get_by_block_range(self, start_block: int, end_block: int) -> List[OverplusBridgedEvent]:
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
        return [OverplusBridgedEvent(**result) for result in results]

    def get_by_date_range(self, start_date: datetime, end_date: datetime) -> List[OverplusBridgedEvent]:
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
        return [OverplusBridgedEvent(**result) for result in results]

    def get_last_processed_block(self) -> Optional[int]:
        """
        Get the last processed block number.
        
        Returns:
            The last processed block number, or None if no records exist
        """
        sql = f"SELECT MAX(block_number) as last_block FROM {self.table_name}"
        result = self.db.fetchone(sql)
        return result['last_block'] if result and result['last_block'] is not None else None

    def get_total_bridged(self) -> float:
        """
        Get the total amount bridged.
        
        Returns:
            Total amount bridged
        """
        sql = f"SELECT SUM(amount) as total_bridged FROM {self.table_name}"
        result = self.db.fetchone(sql)
        return float(result['total_bridged']) if result and result['total_bridged'] is not None else 0

    def get_bridged_by_day(self, days: int = 30) -> List[Dict[str, any]]:
        """
        Get bridged amounts by day for the last N days.
        
        Args:
            days: Number of days to retrieve
            
        Returns:
            List of daily bridged amounts
        """
        sql = f"""
        SELECT 
            DATE(timestamp) as date,
            SUM(amount) as daily_bridged
        FROM {self.table_name} 
        WHERE timestamp >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY date
        ORDER BY date
        """
        results = self.db.fetchall(sql, [days])
        
        return [
            {
                'date': row['date'].isoformat(),
                'amount': float(row['daily_bridged'])
            }
            for row in results
        ]

    def get_bridged_by_month(self) -> List[Dict[str, any]]:
        """
        Get bridged amounts by month.
        
        Returns:
            List of monthly bridged amounts
        """
        sql = f"""
        SELECT 
            DATE_TRUNC('month', timestamp) as month,
            SUM(amount) as monthly_bridged
        FROM {self.table_name} 
        GROUP BY month
        ORDER BY month
        """
        results = self.db.fetchall(sql)
        
        return [
            {
                'month': row['month'].strftime('%Y-%m'),
                'amount': float(row['monthly_bridged'])
            }
            for row in results
        ]

    def bulk_insert(self, records: List[OverplusBridgedEvent]) -> int:
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