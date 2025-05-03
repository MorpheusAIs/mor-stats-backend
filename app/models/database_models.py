"""
Database model definitions for all tables used in the application.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class UserClaimLocked(BaseModel):
    """Model for user_claim_locked table."""
    id: Optional[int] = Field(None, description="Primary key")
    timestamp: datetime = Field(..., description="Event timestamp")
    transaction_hash: str = Field(..., description="Transaction hash")
    block_number: int = Field(..., description="Block number")
    poolid: int = Field(..., description="Pool ID")
    user: str = Field(..., description="User address")
    amount: str = Field(..., description="Amount")


class UserMultiplier(BaseModel):
    """Model for user_multiplier table."""
    id: Optional[int] = Field(None, description="Primary key")
    user_claim_locked_id: int = Field(..., description="Reference to user_claim_locked table")
    timestamp: datetime = Field(..., description="Event timestamp")
    transaction_hash: str = Field(..., description="Transaction hash")
    block_number: int = Field(..., description="Block number")
    pool_id: int = Field(..., description="Pool ID")
    user_address: str = Field(..., description="User address")
    multiplier: Optional[Decimal] = Field(None, description="User multiplier")
    error_message: Optional[str] = Field(None, description="Error message if any")
    created_at: Optional[datetime] = Field(None, description="Record creation timestamp")


class RewardSummary(BaseModel):
    """Model for reward_summary table."""
    id: Optional[int] = Field(None, description="Primary key")
    timestamp: datetime = Field(..., description="Event timestamp")
    calculation_block_current: int = Field(..., description="Current block used for calculation")
    calculation_block_past: int = Field(..., description="Past block used for calculation")
    category: str = Field(..., description="Reward category")
    value: Decimal = Field(..., description="Reward value")
    created_at: Optional[datetime] = Field(None, description="Record creation timestamp")


class RewardDetail(BaseModel):
    """Model for reward_detail table."""
    id: Optional[int] = Field(None, description="Primary key")
    summary_id: int = Field(..., description="Reference to reward_summary table")
    user_address: str = Field(..., description="User address")
    pool_id: int = Field(..., description="Pool ID")
    daily_reward: Decimal = Field(..., description="Daily reward")
    total_reward: Decimal = Field(..., description="Total reward")
    created_at: Optional[datetime] = Field(None, description="Record creation timestamp")


class CirculatingSupply(BaseModel):
    """Model for circulating_supply table."""
    id: Optional[int] = Field(None, description="Primary key")
    date: date = Field(..., description="Date")
    circulating_supply_at_that_date: Decimal = Field(..., description="Circulating supply at that date")
    block_timestamp_at_that_date: int = Field(..., description="Block timestamp at that date")
    total_claimed_that_day: Decimal = Field(..., description="Total claimed that day")
    created_at: Optional[datetime] = Field(None, description="Record creation timestamp")


class UserStakedEvent(BaseModel):
    """Model for user_staked_events table."""
    id: Optional[int] = Field(None, description="Primary key")
    timestamp: datetime = Field(..., description="Event timestamp")
    transaction_hash: str = Field(..., description="Transaction hash")
    block_number: int = Field(..., description="Block number")
    pool_id: int = Field(..., description="Pool ID")
    user_address: str = Field(..., description="User address")
    amount: int = Field(..., description="Amount staked")


class UserWithdrawnEvent(BaseModel):
    """Model for user_withdrawn_events table."""
    id: Optional[int] = Field(None, description="Primary key")
    timestamp: datetime = Field(..., description="Event timestamp")
    transaction_hash: str = Field(..., description="Transaction hash")
    block_number: int = Field(..., description="Block number")
    pool_id: int = Field(..., description="Pool ID")
    user_address: str = Field(..., description="User address")
    amount: Decimal = Field(..., description="Amount withdrawn")
    created_at: Optional[datetime] = Field(None, description="Record creation timestamp")


class OverplusBridgedEvent(BaseModel):
    """Model for overplus_bridged_events table."""
    id: Optional[int] = Field(None, description="Primary key")
    timestamp: datetime = Field(..., description="Event timestamp")
    transaction_hash: str = Field(..., description="Transaction hash")
    block_number: int = Field(..., description="Block number")
    amount: Decimal = Field(..., description="Amount bridged")
    unique_id: str = Field(..., description="Unique ID")
    created_at: Optional[datetime] = Field(None, description="Record creation timestamp")