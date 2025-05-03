"""
Examples of how to use the repository classes.
"""
import asyncio
from datetime import datetime, timedelta

from app.models.database_models import (
    CirculatingSupply,
    OverplusBridgedEvent,
    RewardDetail,
    RewardSummary,
    UserClaimLocked,
    UserMultiplier,
    UserStakedEvent,
    UserWithdrawnEvent,
)
from app.repository import (
    CirculatingSupplyRepository,
    OverplusBridgedEventsRepository,
    RewardDetailRepository,
    RewardSummaryRepository,
    UserClaimLockedRepository,
    UserMultiplierRepository,
    UserStakedEventsRepository,
    UserWithdrawnEventsRepository,
)


async def user_claim_locked_example():
    """Example of using UserClaimLockedRepository."""
    repo = UserClaimLockedRepository()
    
    # Create a new record
    new_record = UserClaimLocked(
        timestamp=datetime.now(),
        transaction_hash="0x123abc",
        block_number=12345,
        poolid=0,
        user="0xabc123",
        amount="1000000000000000000"
    )
    created = repo.create(new_record)
    print(f"Created record: {created}")
    
    # Get by ID
    record = repo.get_by_id(created.id)
    print(f"Retrieved record: {record}")
    
    # Get by user
    user_records = repo.get_by_user("0xabc123")
    print(f"User records: {len(user_records)}")
    
    # Update record
    updated = repo.update(created.id, {"amount": "2000000000000000000"})
    print(f"Updated record: {updated}")
    
    # Delete record
    deleted = repo.delete(created.id)
    print(f"Deleted record: {deleted}")


async def user_multiplier_example():
    """Example of using UserMultiplierRepository."""
    # First create a user_claim_locked record
    ucl_repo = UserClaimLockedRepository()
    ucl_record = UserClaimLocked(
        timestamp=datetime.now(),
        transaction_hash="0x456def",
        block_number=12346,
        poolid=1,
        user="0xdef456",
        amount="1500000000000000000"
    )
    ucl_created = ucl_repo.create(ucl_record)
    
    # Now create a user_multiplier record
    repo = UserMultiplierRepository()
    new_record = UserMultiplier(
        user_claim_locked_id=ucl_created.id,
        timestamp=datetime.now(),
        transaction_hash="0x456def",
        block_number=12346,
        pool_id=1,
        user_address="0xdef456",
        multiplier=1.5
    )
    created = repo.create(new_record)
    print(f"Created multiplier record: {created}")
    
    # Get by user_claim_locked_id
    record = repo.get_by_user_claim_locked_id(ucl_created.id)
    print(f"Retrieved by UCL ID: {record}")
    
    # Get by user and pool
    latest = repo.get_latest_by_user_and_pool("0xdef456", 1)
    print(f"Latest multiplier: {latest}")
    
    # Clean up
    repo.delete(created.id)
    ucl_repo.delete(ucl_created.id)


async def reward_example():
    """Example of using RewardSummaryRepository and RewardDetailRepository."""
    # Create a reward summary
    summary_repo = RewardSummaryRepository()
    summary = RewardSummary(
        timestamp=datetime.now(),
        calculation_block_current=12400,
        calculation_block_past=12300,
        category="Daily Pool 0",
        value=100.5
    )
    created_summary = summary_repo.create(summary)
    print(f"Created reward summary: {created_summary}")
    
    # Create reward details
    detail_repo = RewardDetailRepository()
    detail1 = RewardDetail(
        summary_id=created_summary.id,
        user_address="0xabc123",
        pool_id=0,
        daily_reward=50.25,
        total_reward=500.75
    )
    detail2 = RewardDetail(
        summary_id=created_summary.id,
        user_address="0xdef456",
        pool_id=0,
        daily_reward=50.25,
        total_reward=300.5
    )
    created_detail1 = detail_repo.create(detail1)
    created_detail2 = detail_repo.create(detail2)
    print(f"Created reward details: {created_detail1}, {created_detail2}")
    
    # Get details by summary ID
    details = detail_repo.get_by_summary_id(created_summary.id)
    print(f"Details for summary {created_summary.id}: {len(details)}")
    
    # Get latest rewards by category
    latest = summary_repo.get_latest_by_category("Daily Pool 0")
    print(f"Latest Daily Pool 0 reward: {latest}")
    
    # Clean up
    detail_repo.delete(created_detail1.id)
    detail_repo.delete(created_detail2.id)
    summary_repo.delete(created_summary.id)


async def circulating_supply_example():
    """Example of using CirculatingSupplyRepository."""
    repo = CirculatingSupplyRepository()
    
    # Create a new record
    new_record = CirculatingSupply(
        date=datetime.now().date(),
        circulating_supply_at_that_date=1000000.5,
        block_timestamp_at_that_date=12345678,
        total_claimed_that_day=1000.5
    )
    created = repo.create(new_record)
    print(f"Created circulating supply record: {created}")
    
    # Get latest
    latest = repo.get_latest()
    print(f"Latest circulating supply: {latest}")
    
    # Get by date range
    today = datetime.now().date()
    week_ago = today - timedelta(days=7)
    records = repo.get_by_date_range(week_ago, today)
    print(f"Records in last week: {len(records)}")
    
    # Clean up
    repo.delete(created.id)


async def user_staked_events_example():
    """Example of using UserStakedEventsRepository."""
    repo = UserStakedEventsRepository()
    
    # Create a new record
    new_record = UserStakedEvent(
        timestamp=datetime.now(),
        transaction_hash="0x789ghi",
        block_number=12347,
        pool_id=0,
        user_address="0xghi789",
        amount=2000000000000000000
    )
    created = repo.create(new_record)
    print(f"Created staked event record: {created}")
    
    # Get by user
    user_records = repo.get_by_user_address("0xghi789")
    print(f"User staked events: {len(user_records)}")
    
    # Get total staked by user
    total_staked = repo.get_total_staked_by_user("0xghi789")
    print(f"Total staked by user: {total_staked}")
    
    # Clean up
    repo.delete(created.id)


async def user_withdrawn_events_example():
    """Example of using UserWithdrawnEventsRepository."""
    repo = UserWithdrawnEventsRepository()
    
    # Create a new record
    new_record = UserWithdrawnEvent(
        timestamp=datetime.now(),
        transaction_hash="0x101jkl",
        block_number=12348,
        pool_id=1,
        user_address="0xjkl101",
        amount=500000000000000000
    )
    created = repo.create(new_record)
    print(f"Created withdrawn event record: {created}")
    
    # Get by user
    user_records = repo.get_by_user_address("0xjkl101")
    print(f"User withdrawn events: {len(user_records)}")
    
    # Get total withdrawn by user
    total_withdrawn = repo.get_total_withdrawn_by_user("0xjkl101")
    print(f"Total withdrawn by user: {total_withdrawn}")
    
    # Clean up
    repo.delete(created.id)


async def overplus_bridged_events_example():
    """Example of using OverplusBridgedEventsRepository."""
    repo = OverplusBridgedEventsRepository()
    
    # Create a new record
    new_record = OverplusBridgedEvent(
        timestamp=datetime.now(),
        transaction_hash="0x202mno",
        block_number=12349,
        amount=1000000000000000000,
        unique_id="bridge-123"
    )
    created = repo.create(new_record)
    print(f"Created bridged event record: {created}")
    
    # Get by unique ID
    record = repo.get_by_unique_id("bridge-123")
    print(f"Retrieved by unique ID: {record}")
    
    # Get total bridged
    total_bridged = repo.get_total_bridged()
    print(f"Total bridged: {total_bridged}")
    
    # Clean up
    repo.delete(created.id)


async def main():
    """Run all examples."""
    print("Running UserClaimLockedRepository example...")
    await user_claim_locked_example()
    print("\nRunning UserMultiplierRepository example...")
    await user_multiplier_example()
    print("\nRunning Reward repositories example...")
    await reward_example()
    print("\nRunning CirculatingSupplyRepository example...")
    await circulating_supply_example()
    print("\nRunning UserStakedEventsRepository example...")
    await user_staked_events_example()
    print("\nRunning UserWithdrawnEventsRepository example...")
    await user_withdrawn_events_example()
    print("\nRunning OverplusBridgedEventsRepository example...")
    await overplus_bridged_events_example()


if __name__ == "__main__":
    asyncio.run(main())