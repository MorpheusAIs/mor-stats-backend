# Database Repositories

This directory contains repository classes for database operations. Each repository provides CRUD (Create, Read, Update, Delete) operations for a specific database table.

## Base Repository

The `BaseRepository` class provides common CRUD operations that are inherited by all specific repository classes.

## Available Repositories

### UserClaimLockedRepository

Repository for the `user_claim_locked` table, which stores blockchain events related to user claim locking.

### UserMultiplierRepository

Repository for the `user_multiplier` table, which stores user multiplier data calculated from user claim locked events.

### RewardSummaryRepository and RewardDetailRepository

Repositories for the `reward_summary` and `reward_detail` tables, which store reward calculation data.

### CirculatingSupplyRepository

Repository for the `circulating_supply` table, which tracks the circulating supply of tokens over time.

### UserStakedEventsRepository

Repository for the `user_staked_events` table, which stores blockchain events related to user staking.

### UserWithdrawnEventsRepository

Repository for the `user_withdrawn_events` table, which stores blockchain events related to user withdrawals.

### OverplusBridgedEventsRepository

Repository for the `overplus_bridged_events` table, which stores blockchain events related to overplus bridging.

## Usage Example

```python
from app.repository import UserClaimLockedRepository

# Create a repository instance
repo = UserClaimLockedRepository()

# Get all records with pagination
records = repo.get_all(limit=10, offset=0)

# Get a record by ID
record = repo.get_by_id(1)

# Create a new record
from app.models.database_models import UserClaimLocked
from datetime import datetime

new_record = UserClaimLocked(
    timestamp=datetime.now(),
    transaction_hash="0x123...",
    block_number=12345,
    poolid=0,
    user="0xabc...",
    amount="1000000000000000000"
)
created_record = repo.create(new_record)

# Update a record
updated_record = repo.update(1, {"amount": "2000000000000000000"})

# Delete a record
success = repo.delete(1)

# Use specialized methods
records_by_user = repo.get_by_user("0xabc...")
```

## Data Flow

The main data flow in the system follows this path:

1. `user_claim_locked` - Stores raw blockchain events
2. `user_multiplier` - Calculates and stores user multipliers based on claim locked events
3. `reward_summary` and `reward_detail` - Calculates and stores reward data based on user multipliers

The other tables (`circulating_supply`, `user_staked_events`, `user_withdrawn_events`, `overplus_bridged_events`) operate independently to track different aspects of the system.