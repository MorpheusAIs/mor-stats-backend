# MOR Metrics Dashboard Backend V2

## How to Run:

1) Install the requirements: `pip install -r "requirements.txt"`

2) Create a `.env` file and fill in these values (Refer to the `.env.example` to create this file):
    ```
    RPC_URL=
    ARB_RPC_URL=
    BASE_RPC_URL=
    ETHERSCAN_API_KEY=
    ARBISCAN_API_KEY=
    BASESCAN_API_KEY=
    DUNE_API_KEY=
    DUNE_QUERY_ID=
    GITHUB_API_KEY=
    SLACK_URL=
    DB_HOST=
    DB_NAME=
    DB_USER=
    DB_PASSWORD=
    ```

3) Run: `uvicorn main:app --reload` to run the FastAPI backend.

## Testing

Once uvicorn is up and running, you can navigate to `/tests` and
run `pytest full_mor_explorer_v1_test.py -v`

- This script will run all endpoints using `pytest` and test if the requests are successful or not along with providing
the response time for each endpoint.

## Devops Pipeline

We use gitlab actions to run pipelines that will build a docker image, deploy it to Azure container registry, and then deploy the container using Azure Container Apps

To modify the pipeline update `azure-deploy.yml`.

### Deployments

All pushes to main will trigger a pipeline that deploys into a staging environment by default but not production.

If you want to deploy to production, you must tag your commit you want to deploy starting with `v` for the pipeline to enable prod deployment.

`git tag -a v0.0.1 -m "First Prod Deploy"`

Now push the tag to GitHub

`git push origin v0.0.1`

Production deployment requires Staging deployment to also be successful before deploying.

## Infrastructure

Currently, resources in Azure are created and managed manually with the pipeline handling container image builds, pushing imagines into the container registry, and then updating the updating existing Container Apps.

## Cron Log Processor Script for MOR Explorer

### How to run:
1) Create a `.env` file with fields:
```angular2html
RPC_URL=
ETHERSCAN_API_KEY=
SLACK_URL=
```

2) `pip install -r "requirements.txt"`

#### Flow
1) `cron_master_processor.py` calls `scripts/`
- `i_update_user_claim_locked_events.py`
- `ii_update_user_multipliers.py`
- `iii_update_total_daily_rewards.py`
- `iiii_update_circulating_supply.py`
- `iv_update_user_staked_events.py`
- `v_update_user_withdrawn_events.py`
- `vi_update_overplus_bridged_events.py`
- `vii_update_emissions.py` (new)

2) `i_update_user_claim_locked_events.py` fetches the latest `UserClaimLocked` events
from the blockchain and populates the database with that data for `UserClaimLocked`.
3) `ii_update_user_multipliers.py` fetches the `UserClaimLocked` data and fetches the multipliers
for each address in an async fashion using the async provider and uploads to the `UserMultiplier` database.
4) `iii_update_total_daily_rewards.py`fetches the `UserMultiplier` data and for each address, it
calculates the Daily and Total Staked Rewards using the `getCurrentUserReward` ABI function and then
uploads the results to the `RewardSum` database.
5) `vii_update_emissions.py` updates the emissions data in the database. This script can import emissions data from a CSV file or directly from a string in TSV format.

### Details

1) Uses batch processing, async web3 provider, async code and retries for any rate limits
```angular2html:
from web3 import AsyncWeb3

w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(ETH_RPC_URL))
```

NOTE: This project uses sheet utils and slack notification

## Emissions Repository

The project now includes an Emissions Repository for storing and managing emission data for different categories:
- Capital Emission
- Code Emission
- Compute Emission
- Community Emission
- Protection Emission

### Setting Up Emissions Repository

1. Run the SQL migration script to create the emissions table:
   ```bash
   psql -U your_username -d your_database -f scripts/migrations/create_emissions_table.sql
   ```

2. Import the initial emissions data from the CSV file:
   ```bash
   python scripts/import_initial_emissions.py
   ```
   
   This script reads data from `data/MASTER MOR EXPLORER - Emissions.csv` and imports it into the database.

3. To update emissions data, use the `vii_update_emissions.py` script:
   ```bash
   python scripts/vii_update_emissions.py --file path/to/emissions.csv
   ```

For more details on the Emissions Repository, see [UpdateEmissions.md](docs/UpdateEmissions.md).