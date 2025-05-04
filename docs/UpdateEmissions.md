# Emissions Repository

This document describes the Emissions Repository, which stores and manages emission data for different categories.

## Overview

The Emissions Repository stores daily emission data for the following categories:
- Capital Emission
- Code Emission
- Compute Emission
- Community Emission
- Protection Emission

It also tracks the total emission and total supply for each day.

## Database Schema

The emissions data is stored in the `emissions` table with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| day | INTEGER | Day number |
| date | DATE | Date of emission (unique) |
| capital_emission | DECIMAL | Capital emission amount |
| code_emission | DECIMAL | Code emission amount |
| compute_emission | DECIMAL | Compute emission amount |
| community_emission | DECIMAL | Community emission amount |
| protection_emission | DECIMAL | Protection emission amount |
| total_emission | DECIMAL | Total emission amount |
| total_supply | DECIMAL | Total supply at this date |
| created_at | TIMESTAMP | Record creation timestamp |

## Setup

To set up the emissions repository, follow these steps:

1. Run the SQL migration script to create the emissions table:
   ```bash
   psql -U your_username -d your_database -f scripts/migrations/create_emissions_table.sql
   ```

2. Import the initial emissions data from the CSV file:
   ```bash
   python scripts/import_initial_emissions.py
   ```
   
   This script reads data from `data/MASTER MOR EXPLORER - Emissions.csv` and imports it into the database.

## Updating Emissions Data

You can update emissions data using the `vii_update_emissions.py` script. This script supports two methods of input:

1. From a CSV/TSV file:
   ```bash
   python scripts/vii_update_emissions.py --file path/to/emissions.csv
   ```

2. From a string in TSV format:
   ```bash
   python scripts/vii_update_emissions.py --data "Day\tDate\tCapital Emission\t..."
   ```

The expected format for the emissions data is a CSV file with the following columns:
- Day
- Date (in YYYY-MM-DD format)
- Capital Emission
- Code Emission
- Compute Emission
- Community Emission
- Protection Emission
- Total Emission
- Total Supply

## Using the Repository in Code

The Emissions Repository can be used in your code as follows:

```python
from app.repository import EmissionRepository

# Create a repository instance
emission_repo = EmissionRepository()

# Get all emissions
all_emissions = emission_repo.get_all()

# Get emission by date
from datetime import date
today = date.today()
today_emission = emission_repo.get_by_date(today)

# Get emission by day number
day_1_emission = emission_repo.get_by_day(1)

# Get the latest emission
latest_emission = emission_repo.get_latest()

# Get emissions by date range
from datetime import date, timedelta
start_date = date.today() - timedelta(days=30)
end_date = date.today()
recent_emissions = emission_repo.get_by_date_range(start_date, end_date)

# Get emission categories for a specific date
categories = emission_repo.get_emission_categories(today)

# Get historical emissions for the last 30 days
historical = emission_repo.get_historical_emissions(30)

# Get emission growth rates for the last 30 days
growth_rates = emission_repo.get_emission_growth_rate(30)
```

## Integration with Existing Code

The `get_emission_schedule_for_today.py` helper has been updated to use the Emissions Repository instead of reading from a Google Sheet. It will first try to get data from the repository, and if that fails, it will fall back to the Google Sheet method.

This ensures backward compatibility while providing a more robust and efficient way to access emission data.