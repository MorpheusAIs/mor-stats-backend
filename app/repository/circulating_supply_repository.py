"""
Repository for circulating_supply table.
"""
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from app.models.database_models import CirculatingSupply
from app.repository.base_repository import BaseRepository


class CirculatingSupplyRepository(BaseRepository[CirculatingSupply]):
    """Repository for circulating_supply table."""

    def __init__(self):
        """Initialize the repository."""
        super().__init__(CirculatingSupply, "circulating_supply")

    def get_latest(self) -> CirculatingSupply:
        """
        Get the latest circulating supply record.
        
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
                raise
            
            # Convert tuple to dictionary using column names
            dict_result = dict(zip(columns, result))
            return CirculatingSupply(**dict_result)

    def get_by_date_range(self, start_date: date, end_date: date) -> List[CirculatingSupply]:
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
            return [CirculatingSupply(**dict(zip(columns, result))) for result in results]


    def save_new_supply_data(self, new_data: List[Dict]) -> int:
        """
        Save new circulating supply data to the database.
        
        Args:
            new_data: List of new supply data records
            
        Returns:
            Number of records inserted/updated
        """
        if not new_data:
            return 0
        
        records = []
        for record in new_data:
            # Convert date string to date object if needed
            date_obj = record['date']
            if isinstance(date_obj, str):
                date_obj = datetime.strptime(date_obj, '%d/%m/%Y').date()
            
            records.append(CirculatingSupply(
                date=date_obj,
                circulating_supply_at_that_date=record['circulating_supply_at_that_date'],
                block_timestamp_at_that_date=record['block_timestamp_at_that_date'],
                total_claimed_that_day=record['total_claimed_that_day']
            ))
        
        # Use bulk insert with ON CONFLICT handling
        return self.bulk_insert(records)

    def bulk_insert(self, records: List[CirculatingSupply]) -> int:
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
            circulating_supply_at_that_date = EXCLUDED.circulating_supply_at_that_date,
            block_timestamp_at_that_date = EXCLUDED.block_timestamp_at_that_date,
            total_claimed_that_day = EXCLUDED.total_claimed_that_day
        """

        with self.db.transaction() as cursor:
            cursor.executemany(sql, values_list)
            return len(values_list)