import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, date
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from helpers.capital_helpers.capital_main import get_capital_metrics
from helpers.code_helpers.get_github_commits_metrics import get_commits_data
from helpers.staking_helpers.staking_main import (get_wallet_stake_info,
                                                  give_more_reward_response,
                                                  get_analyze_mor_master_dict)
from helpers.staking_helpers.get_mor_amount_staked_over_time import get_mor_staked_over_time
from helpers.supply_helpers.supply_main import (get_combined_supply_data,
                                                get_historical_prices_and_trading_volume, get_market_cap,
                                                get_mor_holders,
                                                get_historical_locked_and_burnt_mor)
from helpers.uniswap_helpers.get_total_combined_uniswap_position import get_combined_uniswap_position
from helpers.code_helpers.code_main import get_total_weights_and_contributors
from helpers.supply_helpers.get_chain_wise_supplies import get_chain_wise_circ_supply
from sheets_config.slack_notify import slack_notification

scheduler = AsyncIOScheduler()
LAST_CACHE_UPDATE_TIME = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global LAST_CACHE_UPDATE_TIME
    # Startup: Initialize the cache update task
    scheduler.add_job(update_cache_task, CronTrigger(hour='*/12'))  # Run every 12 hours
    scheduler.start()
    await update_cache_task()  # Run the task immediately on startup
    LAST_CACHE_UPDATE_TIME = datetime.now().isoformat()
    yield
    # Shutdown: Shut down the scheduler
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)

# Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "https://*.vercel.app",
        "https://morpheus-stats-frontend.vercel.app",
        "https://mor-stats-backend-cfcfatfxejhphfg9.centralus-01.azurewebsites.net"
    ],  # List of allowed origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

logging.getLogger("httpx").disabled = True
logging.getLogger("dune-client").disabled = True
logging.getLogger("DuneClient").disabled = True
logging.getLogger("dune_client.models").disabled = True
logging.getLogger("dune_client").disabled = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CACHE_FILE = 'cache.json'


# Function to read cache from a file
def read_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as file:
                data = file.read()
                if data.strip():  # Check if the file is not empty
                    return json.loads(data)
                else:
                    return {}  # Return an empty dictionary if the file is empty
        except json.JSONDecodeError as e:
            # Log the error
            logger.info(f"Error reading cache file: {e}")
            # Return an empty dictionary if JSON is invalid
            return {}
    return {}


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type not serializable")


def ensure_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: ensure_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [ensure_serializable(i) for i in obj]
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    else:
        return str(obj)  # Convert any other type to string


def write_cache(cache_data: dict):
    try:
        with open(CACHE_FILE, 'w') as file:
            json.dump(cache_data, file, default=json_serial)
    except TypeError as e:
        # This will catch issues with non-serializable objects
        logging.error(f"TypeError in write_cache: {e}")
        # You could add code here to identify which key is causing the problem
        for key, value in cache_data.items():
            try:
                json.dumps({key: value}, default=json_serial)
            except TypeError:
                logging.error(f"Non-serializable value for key: {key}")
    except Exception as e:
        logging.error(f"Error writing to cache file: {e}")


async def update_cache_task() -> None:
    global LAST_CACHE_UPDATE_TIME
    try:
        cache_data = read_cache()

        cache_data['staking_metrics'] = await get_analyze_mor_master_dict()
        cache_data['total_and_circ_supply'] = await get_combined_supply_data()
        cache_data['prices_and_volume'] = await get_historical_prices_and_trading_volume()
        cache_data['market_cap'] = await get_market_cap()
        cache_data['give_mor_reward'] = give_more_reward_response()
        cache_data['stake_info'] = get_wallet_stake_info()
        cache_data['mor_holders_by_range'] = await get_mor_holders()
        cache_data['locked_and_burnt_mor'] = await get_historical_locked_and_burnt_mor()
        cache_data['protocol_liquidity'] = get_combined_uniswap_position()
        cache_data['capital_metrics'] = get_capital_metrics()
        cache_data['github_commits'] = get_commits_data()
        cache_data['historical_mor_rewards_locked'] = ensure_serializable(await get_mor_staked_over_time())
        cache_data['code_metrics'] = await get_total_weights_and_contributors()
        cache_data['chain_wise_supplies'] = get_chain_wise_circ_supply()

        slack_notification("Finished updating cache")

        # Write the updated cache data to the cache file
        try:
            write_cache(cache_data)
            # After all cache updates are done, update the last cache update time
            LAST_CACHE_UPDATE_TIME = datetime.now().isoformat()
            slack_notification(f"Finished writing cache at {LAST_CACHE_UPDATE_TIME}")
        except Exception as cache_write_error:
            slack_notification(f"Error writing to cache: {cache_write_error}")
            logger.info(f"Error writing to cache: {cache_write_error}")
    except Exception as e:
        slack_notification(f"Error in cache update task: {str(e)}")
        logger.info(f"Error in cache update task: {str(e)}")


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/analyze-mor-stakers")
async def get_mor_staker_analysis():
    cache_data = read_cache()

    if 'staking_metrics' in cache_data:
        return cache_data['staking_metrics']

    # If cache not available, load the data and cache it
    try:
        result = await get_analyze_mor_master_dict()

        cache_data['staking_metrics'] = result
        write_cache(cache_data)

        return result

    except Exception as e:
        logger.error(f"Error fetching stakers: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred fetching stakers")


@app.get("/give_mor_reward")
async def give_more_reward():
    cache_data = read_cache()

    if 'give_mor_reward' in cache_data:
        return cache_data['give_mor_reward']

    try:
        # Call the function to generate the response
        res = give_more_reward_response()

        # Cache the result
        # Ensure that keys in the response are strings before saving to cache
        if isinstance(res, dict):
            res = {str(key): value for key, value in res.items()}

        cache_data['give_mor_reward'] = res
        write_cache(cache_data)

        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred")


@app.get("/get_stake_info")
async def get_stake_info():
    cache_data = read_cache()

    if 'stake_info' in cache_data:
        return cache_data['stake_info']

    try:
        # Call the function to get the stake information
        result = get_wallet_stake_info()

        # Ensure all keys and values in the result are serializable
        serializable_result = {str(key): value for key, value in result.items()}

        # Cache the result
        cache_data['stake_info'] = serializable_result
        write_cache(cache_data)

        return serializable_result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred")


@app.get("/total_and_circ_supply")
async def total_and_circ_supply():
    cache_data = read_cache()

    if 'total_and_circ_supply' in cache_data:
        logger.info("Returning cached total_and_circ_supply data")

        # Return the combined supply data from the cache file with the 'data' key
        return {"data": cache_data['total_and_circ_supply']}

    # If cache not available, load the data and cache it
    try:
        logger.info("Cache miss for total_and_circ_supply, fetching new data")

        # Add the combined supply data to the cache file without the 'data' key
        cache_data['total_and_circ_supply'] = await get_combined_supply_data()
        write_cache(cache_data)

        # Return the combined supply data from the freshly written cache with the 'data' key
        return {"data": cache_data['total_and_circ_supply']}

    except Exception as e:
        logger.info(f"Error fetching total_and_circ_supply data")
        raise HTTPException(status_code=500, detail=f"An error occurred")


@app.get("/prices_and_trading_volume")
async def historical_prices_and_volume():
    cache_data = read_cache()

    if 'prices_and_volume' in cache_data:
        return cache_data['prices_and_volume']

    # If cache not available, load the data and cache it
    try:
        cache_data['prices_and_volume'] = await get_historical_prices_and_trading_volume()
        write_cache(cache_data)

        return cache_data['prices_and_volume']

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred")


@app.get("/get_market_cap")
async def market_cap():
    cache_data = read_cache()

    if 'market_cap' in cache_data:
        return cache_data['market_cap']

    # If cache not available, load the data and cache it
    try:
        result = await get_market_cap()
        if "error" in result:
            raise HTTPException(status_code=500, detail=f"An error occurred")

        cache_data['market_cap'] = result
        write_cache(cache_data)

        return result

    except Exception as e:
        logger.error(f"Error fetching market cap: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred")


@app.get("/mor_holders_by_range")
async def mor_holders_by_range():
    cache_data = read_cache()

    if 'mor_holders_by_range' in cache_data:
        logger.info("Returning cached data")
        return cache_data['mor_holders_by_range']

    logger.info("Cache miss, fetching new data")

    try:
        result = await get_mor_holders()
        cache_data['mor_holders_by_range'] = result
        write_cache(cache_data)

        logger.info("New data fetched and cached")
        return result

    except Exception as e:
        logger.exception(f"An error occurred in mor_holders_by_range: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred")


@app.get("/locked_and_burnt_mor")
async def locked_and_burnt_mor():
    cache_data = read_cache()

    if 'locked_and_burnt_mor' in cache_data:
        return cache_data['locked_and_burnt_mor']

    try:
        # Cache the result
        result = await get_historical_locked_and_burnt_mor()

        cache_data['locked_and_burnt_mor'] = result
        write_cache(cache_data)

        # Return the combined data
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred")


@app.get("/protocol_liquidity")
async def get_protocol_liquidity():
    cache_data = read_cache()

    if 'protocol_liquidity' in cache_data:
        return cache_data['protocol_liquidity']

    try:
        # Call the protocol_liquidity function with the default address
        result = get_combined_uniswap_position()

        if not result:
            raise HTTPException(status_code=404, detail="Not Found")

        # Cache the result
        cache_data['protocol_liquidity'] = result
        write_cache(cache_data)

        return result  # Return the calculated liquidity in USD, MOR, and stETH values

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred")


# Function to get the last updated time
@app.get("/last_cache_update_time")
async def get_last_cache_update_time():
    return {"last_updated_time": LAST_CACHE_UPDATE_TIME}


@app.get("/capital_metrics")
async def capital_metrics():
    cache_data = read_cache()

    if 'capital_metrics' in cache_data:
        return cache_data['capital_metrics']

    try:
        result = get_capital_metrics()

        # Cache the result
        cache_data['capital_metrics'] = result
        write_cache(cache_data)

        return result
    except Exception as e:
        logger.error(f"Error fetching capital metrics: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching capital metrics")


@app.get("/github_commits")
async def get_github_commits():
    cache_data = read_cache()

    if 'github_commits' in cache_data:
        return cache_data['github_commits']

    try:
        result = get_commits_data()

        cache_data['github_commits'] = result
        write_cache(cache_data)

        return result
    except Exception as e:
        logger.error(f"Error fetching github commits: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching github commits")


@app.get("/historical_mor_rewards_locked")
async def get_historical_mor_staked():
    cache_data = read_cache()

    if 'historical_mor_rewards_locked' in cache_data:
        return cache_data['historical_mor_rewards_locked']

    try:
        result = await get_mor_staked_over_time()
        serializable_result = ensure_serializable(result)

        cache_data['historical_mor_rewards_locked'] = serializable_result
        write_cache(cache_data)

        return serializable_result
    except Exception as e:
        logger.error(f"Error fetching mor rewards locked: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/code_metrics")
async def get_code_metrics():
    cache_data = read_cache()

    if 'code_metrics' in cache_data:
        return cache_data['code_metrics']

    try:
        result = await get_total_weights_and_contributors()

        cache_data['code_metrics'] = result
        write_cache(cache_data)

        return result

    except Exception as e:
        logger.error(f"Error fetching code metrics: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/chain_wise_supplies")
async def get_circ_supply_by_chains():
    cache_data = read_cache()

    if 'chain_wise_supplies' in cache_data:
        return cache_data['chain_wise_supplies']

    try:
        result = get_chain_wise_circ_supply()

        cache_data['chain_wise_supplies'] = result
        write_cache(cache_data)

        return result

    except Exception as e:
        logger.error(f"Error fetching code in chain-wise supplies: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
