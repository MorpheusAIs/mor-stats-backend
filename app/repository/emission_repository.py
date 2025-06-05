"""
Repository for emissions table.
"""
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from app.models.database_models import Emission
from app.repository.base_repository import BaseRepository


class EmissionRepository(BaseRepository[Emission]):
    """Repository for emissions table."""

    def __init__(self):
        """Initialize the repository."""
        super().__init__(Emission, "emissions")

    def get_by_date(self, date_value: date) -> Optional[Emission]:
        """
        Get a record by date.
        
        Args:
            date_value: The date to query
            
        Returns:
            The record, or None if not found
        """
        # Use cursor directly to get column names
        with self.db.cursor() as cur:
            sql = f"SELECT * FROM {self.table_name} WHERE date = %s"
            cur.execute(sql, [date_value])
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch the result
            result = cur.fetchone()
            
            if not result:
                return None
            
            # Convert tuple to dictionary using column names
            dict_result = dict(zip(columns, result))
            return Emission(**dict_result)

    def get_latest(self) -> Optional[Emission]:
        """
        Get the latest emission record.
        
        Returns:
            The latest record, or None if no records exist
        """
        # Use cursor directly to get column names
        with self.db.cursor() as cur:
            sql = f"SELECT * FROM {self.table_name} ORDER BY date DESC LIMIT 1"
            cur.execute(sql)
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch the result
            result = cur.fetchone()
            
            if not result:
                return None
            
            # Convert tuple to dictionary using column names
            dict_result = dict(zip(columns, result))
            return Emission(**dict_result)

    def get_by_date_range(self, start_date: date, end_date: date) -> List[Emission]:
        """
        Get records by date range.
        
        Args:
            start_date: The start date
            end_date: The end date
            
        Returns:
            List of records
        """
        # Use cursor directly to get column names
        with self.db.cursor() as cur:
            sql = f"""
            SELECT * FROM {self.table_name}
            WHERE date BETWEEN %s AND %s
            ORDER BY date
            """
            cur.execute(sql, [start_date, end_date])
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Fetch all results
            results = cur.fetchall()
            
            # Convert tuples to dictionaries using column names
            return [Emission(**dict(zip(columns, result))) for result in results]

    def bulk_insert(self, records: List[Emission]) -> int:
        """
        Insert multiple records at once with conflict handling.
        
        Args:
            records: List of records to insert
            
        Returns:
            Number of records inserted/updated
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

        # Build and execute the query with ON CONFLICT handling
        sql = f"""
        INSERT INTO {self.table_name} ({column_str})
        VALUES ({placeholders})
        ON CONFLICT (date)
        DO UPDATE SET
            day = EXCLUDED.day,
            capital_emission = EXCLUDED.capital_emission,
            code_emission = EXCLUDED.code_emission,
            compute_emission = EXCLUDED.compute_emission,
            community_emission = EXCLUDED.community_emission,
            protection_emission = EXCLUDED.protection_emission,
            total_emission = EXCLUDED.total_emission,
            total_supply = EXCLUDED.total_supply
        """

        with self.db.transaction() as cursor:
            cursor.executemany(sql, values_list)
            return len(values_list)