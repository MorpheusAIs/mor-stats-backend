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
