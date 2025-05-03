"""
Repository for reward_summary and reward_detail tables.
"""
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from app.models.database_models import RewardDetail, RewardSummary
from app.repository.base_repository import BaseRepository


class RewardSummaryRepository(BaseRepository[RewardSummary]):
    """Repository for reward_summary table."""

    def __init__(self):
        """Initialize the repository."""
        super().__init__(RewardSummary, "reward_summary")

    def get_by_category(self, category: str, limit: int = 100, offset: int = 0) -> List[RewardSummary]:
        """
        Get records by category.
        
        Args:
            category: The reward category
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of records
        """
        sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE category = %s 
        ORDER BY timestamp DESC 
        LIMIT %s OFFSET %s
        """
        results = self.db.fetchall(sql, [category, limit, offset])
        return [RewardSummary(**result) for result in results]

    def get_by_date_range(self, start_date: datetime, end_date: datetime) -> List[RewardSummary]:
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
        return [RewardSummary(**result) for result in results]

    def get_latest_by_category(self, category: str) -> Optional[RewardSummary]:
        """
        Get the latest record by category.
        
        Args:
            category: The reward category
            
        Returns:
            The latest record, or None if not found
        """
        sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE category = %s 
        ORDER BY timestamp DESC 
        LIMIT 1
        """
        result = self.db.fetchone(sql, [category])
        return RewardSummary(**result) if result else None

    def get_daily_rewards(self, days: int = 30) -> List[Dict[str, Dict[str, float]]]:
        """
        Get daily rewards for the last N days.
        
        Args:
            days: Number of days to retrieve
            
        Returns:
            List of daily rewards by category
        """
        sql = f"""
        SELECT 
            DATE(timestamp) as date,
            category,
            value
        FROM {self.table_name} 
        WHERE 
            timestamp >= CURRENT_DATE - INTERVAL '%s days' AND
            category LIKE 'Daily%%'
        ORDER BY date, category
        """
        results = self.db.fetchall(sql, [days])
        
        # Group by date
        daily_rewards = {}
        for row in results:
            date_str = row['date'].isoformat()
            if date_str not in daily_rewards:
                daily_rewards[date_str] = {}
            
            daily_rewards[date_str][row['category']] = float(row['value'])
        
        # Convert to list sorted by date
        return [
            {'date': date, 'rewards': rewards}
            for date, rewards in sorted(daily_rewards.items())
        ]

    def get_total_rewards_by_pool(self) -> Dict[str, float]:
        """
        Get the latest total rewards by pool.
        
        Returns:
            Dictionary of pool categories and their total rewards
        """
        sql = f"""
        SELECT 
            category,
            value
        FROM {self.table_name} 
        WHERE 
            category LIKE 'Total%%' AND
            timestamp = (
                SELECT MAX(timestamp) 
                FROM {self.table_name} 
                WHERE category LIKE 'Total%%'
            )
        """
        results = self.db.fetchall(sql)
        
        return {row['category']: float(row['value']) for row in results}


class RewardDetailRepository(BaseRepository[RewardDetail]):
    """Repository for reward_detail table."""

    def __init__(self):
        """Initialize the repository."""
        super().__init__(RewardDetail, "reward_detail")

    def get_by_summary_id(self, summary_id: int) -> List[RewardDetail]:
        """
        Get records by summary ID.
        
        Args:
            summary_id: The summary ID
            
        Returns:
            List of records
        """
        sql = f"SELECT * FROM {self.table_name} WHERE summary_id = %s"
        results = self.db.fetchall(sql, [summary_id])
        return [RewardDetail(**result) for result in results]

    def get_by_user_address(self, user_address: str, limit: int = 100, offset: int = 0) -> List[RewardDetail]:
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
        SELECT rd.* FROM {self.table_name} rd
        JOIN reward_summary rs ON rd.summary_id = rs.id
        WHERE rd.user_address = %s 
        ORDER BY rs.timestamp DESC 
        LIMIT %s OFFSET %s
        """
        results = self.db.fetchall(sql, [user_address, limit, offset])
        return [RewardDetail(**result) for result in results]

    def get_by_pool_id(self, pool_id: int, limit: int = 100, offset: int = 0) -> List[RewardDetail]:
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
        SELECT rd.* FROM {self.table_name} rd
        JOIN reward_summary rs ON rd.summary_id = rs.id
        WHERE rd.pool_id = %s 
        ORDER BY rs.timestamp DESC 
        LIMIT %s OFFSET %s
        """
        results = self.db.fetchall(sql, [pool_id, limit, offset])
        return [RewardDetail(**result) for result in results]

    def get_by_user_and_pool(self, user_address: str, pool_id: int, limit: int = 100, offset: int = 0) -> List[RewardDetail]:
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
        SELECT rd.* FROM {self.table_name} rd
        JOIN reward_summary rs ON rd.summary_id = rs.id
        WHERE rd.user_address = %s AND rd.pool_id = %s 
        ORDER BY rs.timestamp DESC 
        LIMIT %s OFFSET %s
        """
        results = self.db.fetchall(sql, [user_address, pool_id, limit, offset])
        return [RewardDetail(**result) for result in results]

    def get_latest_rewards_by_user(self, user_address: str) -> Dict[int, Tuple[float, float]]:
        """
        Get the latest daily and total rewards for a user by pool.
        
        Args:
            user_address: The user address
            
        Returns:
            Dictionary mapping pool_id to (daily_reward, total_reward)
        """
        sql = f"""
        WITH latest_summary AS (
            SELECT id, timestamp
            FROM reward_summary
            ORDER BY timestamp DESC
            LIMIT 1
        )
        SELECT 
            rd.pool_id,
            rd.daily_reward,
            rd.total_reward
        FROM {self.table_name} rd
        JOIN latest_summary ls ON rd.summary_id = ls.id
        WHERE rd.user_address = %s
        """
        results = self.db.fetchall(sql, [user_address])
        
        return {
            row['pool_id']: (float(row['daily_reward']), float(row['total_reward']))
            for row in results
        }

    def bulk_insert(self, records: List[RewardDetail]) -> int:
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