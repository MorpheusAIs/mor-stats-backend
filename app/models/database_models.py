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
    pool_id: int = Field(..., description="Pool ID")
    user_address: str = Field(..., description="User address")
    claim_lock_start: int = Field(..., description="ClaimLockStart")
    claim_lock_end: int = Field(..., description="ClaimLockEnd")

class UserMultiplier(BaseModel):
    """Model for user_multiplier table."""
    id: Optional[int] = Field(None, description="Primary key")
    user_claim_locked_start: int = Field(..., description="Reference to user_claim_locked table")
    user_claim_locked_end: int = Field(..., description="Reference to user_claim_locked table")
    timestamp: datetime = Field(..., description="Event timestamp")
    transaction_hash: str = Field(..., description="Transaction hash")
    block_number: int = Field(..., description="Block number")
    pool_id: int = Field(..., description="Pool ID")
    user_address: str = Field(..., description="User address")
    multiplier: Optional[Decimal] = Field(None, description="User multiplier")


class RewardSummary(BaseModel):
    """Model for reward_summary table."""
    id: Optional[int] = Field(None, description="Primary key")
    timestamp: datetime = Field(..., description="Event timestamp")
    calculation_block_current: int = Field(..., description="Current block used for calculation")
    calculation_block_past: int = Field(..., description="Past block used for calculation")
    category: str = Field(..., description="Reward category")
    value: Decimal = Field(..., description="Reward value")


class RewardDetail(BaseModel):
    """Model for reward_detail table."""
    id: Optional[int] = Field(None, description="Primary key")
    summary_id: int = Field(..., description="Reference to reward_summary table")
    user_address: str = Field(..., description="User address")
    pool_id: int = Field(..., description="Pool ID")
    daily_reward: Decimal = Field(..., description="Daily reward")
    total_reward: Decimal = Field(..., description="Total reward")


class CirculatingSupply(BaseModel):
    """Model for circulating_supply table."""
    id: Optional[int] = Field(None, description="Primary key")
    date: datetime = Field(..., description="Date")
    circulating_supply_at_that_date: Decimal = Field(..., description="Circulating supply at that date")
    block_timestamp_at_that_date: int = Field(..., description="Block timestamp at that date")
    total_claimed_that_day: Decimal = Field(..., description="Total claimed that day")


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


class OverplusBridgedEvent(BaseModel):
    """Model for overplus_bridged_events table."""
    id: Optional[int] = Field(None, description="Primary key")
    timestamp: datetime = Field(..., description="Event timestamp")
    transaction_hash: str = Field(..., description="Transaction hash")
    block_number: int = Field(..., description="Block number")
    amount: Decimal = Field(..., description="Amount bridged")
    unique_id: str = Field(..., description="Unique ID")


class Emission(BaseModel):
    """Model for emissions table."""
    id: Optional[int] = Field(None, description="Primary key")
    day: int = Field(..., description="Day number")
    date: datetime = Field(..., description="Date of emission")
    capital_emission: Decimal = Field(..., description="Capital emission amount")
    code_emission: Decimal = Field(..., description="Code emission amount")
    compute_emission: Decimal = Field(..., description="Compute emission amount")
    community_emission: Decimal = Field(..., description="Community emission amount")
    protection_emission: Decimal = Field(..., description="Protection emission amount")
    total_emission: Decimal = Field(..., description="Total emission amount")
    total_supply: Decimal = Field(..., description="Total supply at this date")