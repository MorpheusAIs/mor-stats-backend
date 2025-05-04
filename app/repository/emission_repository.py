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
        sql = f"SELECT * FROM {self.table_name} WHERE date = %s"
        result = self.db.fetchone(sql, [date_value])
        return Emission(**result) if result else None

    def get_by_day(self, day: int) -> Optional[Emission]:
        """
        Get a record by day number.
        
        Args:
            day: The day number to query
            
        Returns:
            The record, or None if not found
        """
        sql = f"SELECT * FROM {self.table_name} WHERE day = %s"
        result = self.db.fetchone(sql, [day])
        return Emission(**result) if result else None

    def get_latest(self) -> Optional[Emission]:
        """
        Get the latest emission record.
        
        Returns:
            The latest record, or None if no records exist
        """
        sql = f"SELECT * FROM {self.table_name} ORDER BY date DESC LIMIT 1"
        result = self.db.fetchone(sql)
        return Emission(**result) if result else None

    def get_by_date_range(self, start_date: date, end_date: date) -> List[Emission]:
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
        return [Emission(**result) for result in results]

    def get_emission_categories(self, date_value: date = None) -> Dict[str, Decimal]:
        """
        Get emission amounts by category for a specific date.
        If no date is provided, returns the latest emission data.
        
        Args:
            date_value: The date to query (optional)
            
        Returns:
            Dictionary with emission categories and amounts
        """
        if date_value:
            emission = self.get_by_date(date_value)
        else:
            emission = self.get_latest()
            
        if not emission:
            return {}
            
        return {
            'capital_emission': emission.capital_emission,
            'code_emission': emission.code_emission,
            'compute_emission': emission.compute_emission,
            'community_emission': emission.community_emission,
            'protection_emission': emission.protection_emission,
            'total_emission': emission.total_emission
        }

    def get_historical_emissions(self, days: int = 30) -> List[Dict[str, any]]:
        """
        Get historical emission data for the last N days.
        
        Args:
            days: Number of days to retrieve
            
        Returns:
            List of emission data dictionaries
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        emissions = self.get_by_date_range(start_date, end_date)
        
        return [{
            'day': emission.day,
            'date': emission.date.isoformat(),
            'capital_emission': float(emission.capital_emission),
            'code_emission': float(emission.code_emission),
            'compute_emission': float(emission.compute_emission),
            'community_emission': float(emission.community_emission),
            'protection_emission': float(emission.protection_emission),
            'total_emission': float(emission.total_emission),
            'total_supply': float(emission.total_supply)
        } for emission in emissions]

    def get_emission_growth_rate(self, period_days: int = 30) -> Dict[str, Tuple[float, float]]:
        """
        Calculate the growth rate of emissions over a period for each category.
        
        Args:
            period_days: Number of days to analyze
            
        Returns:
            Dictionary with categories and their (absolute_growth, percentage_growth) tuples
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=period_days)
        
        start_emission = self.get_by_date(start_date)
        end_emission = self.get_by_date(end_date)
        
        if not start_emission or not end_emission:
            return {}
        
        categories = [
            'capital_emission', 
            'code_emission', 
            'compute_emission', 
            'community_emission', 
            'protection_emission',
            'total_emission'
        ]
        
        growth_rates = {}
        
        for category in categories:
            start_value = getattr(start_emission, category)
            end_value = getattr(end_emission, category)
            
            absolute_growth = float(end_value - start_value)
            percentage_growth = float((end_value - start_value) / start_value * 100) if start_value != 0 else 0
            
            growth_rates[category] = (absolute_growth, percentage_growth)
            
        return growth_rates

    def save_emission_data(self, emission_data: List[Dict]) -> int:
        """
        Save emission data to the database.
        
        Args:
            emission_data: List of emission data records
            
        Returns:
            Number of records inserted/updated
        """
        if not emission_data:
            return 0
        
        records = []
        for record in emission_data:
            # Convert date string to date object if needed
            date_obj = record.get('date')
            if isinstance(date_obj, str):
                date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
            
            records.append(Emission(
                day=record.get('day'),
                date=date_obj,
                capital_emission=record.get('capital_emission'),
                code_emission=record.get('code_emission'),
                compute_emission=record.get('compute_emission'),
                community_emission=record.get('community_emission'),
                protection_emission=record.get('protection_emission'),
                total_emission=record.get('total_emission'),
                total_supply=record.get('total_supply')
            ))
        
        # Use bulk insert with ON CONFLICT handling
        return self.bulk_insert(records)

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