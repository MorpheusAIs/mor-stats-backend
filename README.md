# MOR Metrics Dashboard Backend V2

## How to Run:

1) Install the requirements: `pip install -r "requirements.txt"`

2) Create a `.env` file and fill in these values (Refer to the `.env.example` to create this file):
- ```
  RPC_URL=
  ARB_RPC_URL=
  BASE_RPC_URL=
  ETHERSCAN_API_KEY=
  ARBISCAN_API_KEY=
  BASESCAN_API_KEY=
  DUNE_API_KEY=
  DUNE_QUERY_ID=
  SPREADSHEET_ID=
  GITHUB_API_KEY=
  SLACK_URL=
  ```
3) Place your Google Sheets Integration Credentials json file in the `sheets_config` directory
4) Run: `uvicorn main:app --reload` to run the FastAPI backend.

## Testing

Once uvicorn is up and running, you can navigate to `/tests` and
run `pytest full_mor_explorer_v1_test.py -v`

- This script will run all endpoints using `pytest` and test if the requests are successful or not along with providing
the response time for each endpoint.

NOTE: Please add your own sheets config file
<!-- TODO: Add a note about the sheets config file -->

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
SPREADSHEET_ID=
```

2) Place `google-sheet-credentials-example.json` in the `sheet_config` directory

3) `pip install -r "requirements.txt"`

#### Flow
1) `cron_master_processor.py` calls `scripts/`
- `i_update_user_claim_locked_events.py`
- `ii_update_user_multipliers.py`
- `iii_update_total_daily_rewards.py`

2) `i_update_user_claim_locked_events.py` fetches the latest `UserClaimLocked` events
from the blockchain and populates the google sheet with that data for
`UserClaimLocked` sub-sheet/worksheet.
3) `ii_update_user_multipliers.py` fetches the `UserClaimLocked` worksheet and fetches the multipliers
for each address in an async fashion using the async provider and uploads to 
the `UserMultiplier` worksheet.
4) `iii_update_total_daily_rewards.py`fetches the `UserMultiplier` worksheet and for each address, it
calculates the Daily and Total Staked Rewards using the `getCurrentUserReward` ABI function and then
uploads the results to the `RewardSum` worksheet.

### Details

1) Uses batch processing, async web3 provider, async code and retries for any rate limits
```angular2html:
from web3 import AsyncWeb3

w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(ETH_RPC_URL))
```

NOTE: This project uses sheet utils and slack notification