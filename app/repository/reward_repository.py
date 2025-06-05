"""
Repository for reward_summary and reward_detail tables.
"""
from datetime import datetime
from typing import Dict, List, Optional

from app.models.database_models import RewardSummary
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
