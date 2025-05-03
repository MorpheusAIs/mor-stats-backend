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

    def get_by_date(self, date_value: date) -> Optional[CirculatingSupply]:
        """
        Get a record by date.
        
        Args:
            date_value: The date to query
            
        Returns:
            The record, or None if not found
        """
        sql = f"SELECT * FROM {self.table_name} WHERE date = %s"
        result = self.db.fetchone(sql, [date_value])
        return CirculatingSupply(**result) if result else None

    def get_latest(self) -> Optional[CirculatingSupply]:
        """
        Get the latest circulating supply record.
        
        Returns:
            The latest record, or None if no records exist
        """
        sql = f"SELECT * FROM {self.table_name} ORDER BY date DESC LIMIT 1"
        result = self.db.fetchone(sql)
        return CirculatingSupply(**result) if result else None

    def get_by_date_range(self, start_date: date, end_date: date) -> List[CirculatingSupply]:
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
        WHERE date BETWEEN %s AND %s 
        ORDER BY date
        """
        results = self.db.fetchall(sql, [start_date, end_date])
        return [CirculatingSupply(**result) for result in results]

    def get_daily_change(self, days: int = 30) -> List[Dict[str, any]]:
        """
        Get daily change in circulating supply for the last N days.
        
        Args:
            days: Number of days to retrieve
            
        Returns:
            List of daily changes
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        sql = f"""
        SELECT 
            date,
            circulating_supply_at_that_date,
            total_claimed_that_day,
            LAG(circulating_supply_at_that_date) OVER (ORDER BY date) as prev_supply
        FROM {self.table_name} 
        WHERE date BETWEEN %s AND %s 
        ORDER BY date
        """
        results = self.db.fetchall(sql, [start_date, end_date])
        
        daily_changes = []
        for row in results:
            prev_supply = row['prev_supply'] if row['prev_supply'] is not None else row['circulating_supply_at_that_date'] - row['total_claimed_that_day']
            daily_change = float(row['circulating_supply_at_that_date']) - float(prev_supply)
            
            daily_changes.append({
                'date': row['date'].isoformat(),
                'circulating_supply': float(row['circulating_supply_at_that_date']),
                'daily_change': daily_change,
                'daily_change_percent': (daily_change / float(prev_supply)) * 100 if prev_supply != 0 else 0
            })
        
        return daily_changes

    def get_supply_growth_rate(self, period_days: int = 30) -> Tuple[float, float]:
        """
        Calculate the growth rate of circulating supply over a period.
        
        Args:
            period_days: Number of days to analyze
            
        Returns:
            Tuple of (absolute_growth, percentage_growth)
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=period_days)
        
        sql = f"""
        SELECT 
            (SELECT circulating_supply_at_that_date FROM {self.table_name} 
             WHERE date <= %s ORDER BY date DESC LIMIT 1) as start_supply,
            (SELECT circulating_supply_at_that_date FROM {self.table_name} 
             WHERE date <= %s ORDER BY date DESC LIMIT 1) as end_supply
        """
        result = self.db.fetchone(sql, [start_date, end_date])
        
        if not result or result['start_supply'] is None or result['end_supply'] is None:
            return 0, 0
        
        start_supply = Decimal(result['start_supply'])
        end_supply = Decimal(result['end_supply'])
        
        absolute_growth = float(end_supply - start_supply)
        percentage_growth = float((end_supply - start_supply) / start_supply * 100) if start_supply != 0 else 0
        
        return absolute_growth, percentage_growth

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