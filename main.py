import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from cron_master_processor import process_blockchain_updates, process_blockchain_updates
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.cache.cache_manager import get_cache_item, get_last_cache_update_time, set_cache_item
from app.core.exceptions import DatabaseError
from app.core.settings import settings
from app.db.database import DBConfig, init_db
from app.middleware.error_handler import add_error_handler
from app.models.responses import DataResponse, HealthCheckResponse, MessageResponse
# from cron_master_processor import run_update_process
from helpers.capital_helpers.capital_main import get_capital_metrics
from helpers.code_helpers.code_main import get_total_weights_and_contributors
from helpers.code_helpers.get_github_commits_metrics import get_commits_data
from helpers.staking_helpers.get_mor_amount_staked_over_time import get_mor_staked_over_time
from helpers.staking_helpers.staking_main import (get_wallet_stake_info,
                                                  give_more_reward_response,
                                                  get_analyze_mor_master_dict)
from helpers.supply_helpers.get_chain_wise_supplies import get_chain_wise_circ_supply
from helpers.supply_helpers.supply_main import (get_combined_supply_data,
                                                get_historical_prices_and_trading_volume, get_market_cap,
                                                get_mor_holders,
                                                get_historical_locked_and_burnt_mor)
from helpers.uniswap_helpers.get_total_combined_uniswap_position import get_combined_uniswap_position

scheduler = AsyncIOScheduler()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.info("Starting application...")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application initialization")
    try:
        # Initialize database
        config = DBConfig(
            host=settings.database.host,
            port=settings.database.port,
            database=settings.database.database,
            user=settings.database.user,
            password=settings.database.password,
            minconn=settings.database.minconn,
            maxconn=settings.database.maxconn,
            autocommit=settings.database.autocommit,
        )

        db = init_db(config)
        try:
            if not db.health_check():
                raise DatabaseError("Database health check failed")
            logger.info("Database connection check successful")
        except Exception as e:
            raise DatabaseError("Database connection error", details={"error": str(e)})

        # Set up scheduler
        try:
            logger.info("Setting up scheduler")
            scheduler.add_job(update_read_cache_task, CronTrigger(hour='*/6'))
            scheduler.add_job(process_blockchain_updates, CronTrigger(hour='*/12'))
            scheduler.start()
        except Exception as scheduler_error:
            logger.error(f"Scheduler error: {str(scheduler_error)}")
            # Continue without scheduler
        
        logger.info("Application startup complete")
        yield
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}", exc_info=True)
        yield  # Still yield to allow the application to start
    finally:
        try:
            logger.info("Shutting down scheduler")
            scheduler.shutdown()
        except Exception as shutdown_error:
            logger.error(f"Error during shutdown: {str(shutdown_error)}")


app = FastAPI(
    title="MOR Stats Backend",
    description="Backend API for MOR statistics",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add error handler middleware
add_error_handler(app)

# Disable noisy loggers
logging.getLogger("httpx").disabled = True
logging.getLogger("dune-client").disabled = True
logging.getLogger("DuneClient").disabled = True
logging.getLogger("dune_client.models").disabled = True
logging.getLogger("dune_client").disabled = True


async def update_read_cache_task() -> None:
    """Update all read cache data."""
    try:
        logger.info("Updating read cache")
        set_cache_item('staking_metrics', await get_analyze_mor_master_dict())
        set_cache_item('total_and_circ_supply', await get_combined_supply_data())
        set_cache_item('prices_and_volume', await get_historical_prices_and_trading_volume())
        set_cache_item('market_cap', await get_market_cap())
        set_cache_item('give_mor_reward', give_more_reward_response())
        set_cache_item('stake_info', get_wallet_stake_info())
        set_cache_item('mor_holders_by_range', await get_mor_holders())
        set_cache_item('locked_and_burnt_mor', await get_historical_locked_and_burnt_mor())
        set_cache_item('protocol_liquidity', get_combined_uniswap_position())
        set_cache_item('capital_metrics', get_capital_metrics())
        set_cache_item('github_commits', get_commits_data())
        set_cache_item('historical_mor_rewards_locked', await get_mor_staked_over_time())
        set_cache_item('code_metrics', await get_total_weights_and_contributors())
        set_cache_item('chain_wise_supplies', get_chain_wise_circ_supply())

        logger.info("Finished updating read cache")
    except Exception as e:
        logger.error(f"Error in read cache update task: {str(e)}")


@app.get("/", response_model=MessageResponse)
async def root():
    """Root endpoint."""
    return MessageResponse(message="MOR Stats API", success=True)


@app.get("/analyze-mor-stakers", response_model=DataResponse)
async def get_mor_staker_analysis():
    """Get MOR staker analysis."""
    cached_data = get_cache_item('staking_metrics')
    logger.debug(f"Cache access for 'staking_metrics': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    # If cache not available, load the data and cache it
    try:
        result = await get_analyze_mor_master_dict()
        set_cache_item('staking_metrics', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching stakers: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred fetching stakers")


@app.get("/give_mor_reward", response_model=DataResponse)
async def give_more_reward():
    """Get MOR reward information."""
    cached_data = get_cache_item('give_mor_reward')
    logger.debug(f"Cache access for 'give_mor_reward': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    try:
        # Call the function to generate the response
        res = give_more_reward_response()
        set_cache_item('give_mor_reward', res)
        return DataResponse(data=res)
    except Exception as e:
        logger.error(f"Error getting MOR reward: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/get_stake_info", response_model=DataResponse)
async def get_stake_info():
    """Get stake information."""
    cached_data = get_cache_item('stake_info')
    logger.debug(f"Cache access for 'stake_info': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    try:
        # Call the function to get the stake information
        result = get_wallet_stake_info()
        set_cache_item('stake_info', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error getting stake info: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/total_and_circ_supply", response_model=DataResponse)
async def total_and_circ_supply():
    """Get total and circulating supply data."""
    cached_data = get_cache_item('total_and_circ_supply')
    logger.debug(f"Cache access for 'total_and_circ_supply': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        logger.info("Returning cached total_and_circ_supply data")
        return DataResponse(data=cached_data)

    # If cache not available, load the data and cache it
    try:
        logger.info("Cache miss for total_and_circ_supply, fetching new data")
        result = await get_combined_supply_data()
        set_cache_item('total_and_circ_supply', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching total_and_circ_supply data: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/prices_and_trading_volume", response_model=DataResponse)
async def historical_prices_and_volume():
    """Get historical prices and trading volume data."""
    cached_data = get_cache_item('prices_and_volume')
    logger.debug(f"Cache access for 'prices_and_volume': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    # If cache not available, load the data and cache it
    try:
        result = await get_historical_prices_and_trading_volume()
        set_cache_item('prices_and_volume', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching prices and volume: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/get_market_cap", response_model=DataResponse)
async def market_cap():
    """Get market cap data."""
    cached_data = get_cache_item('market_cap')
    logger.debug(f"Cache access for 'market_cap': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    # If cache not available, load the data and cache it
    try:
        result = await get_market_cap()
        if "error" in result:
            raise HTTPException(status_code=500, detail="An error occurred")

        set_cache_item('market_cap', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching market cap: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/mor_holders_by_range", response_model=DataResponse)
async def mor_holders_by_range():
    """Get MOR holders by range data."""
    cached_data = get_cache_item('mor_holders_by_range')
    logger.debug(f"Cache access for 'mor_holders_by_range': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        logger.info("Returning cached mor_holders_by_range data")
        return DataResponse(data=cached_data)

    logger.info("Cache miss for mor_holders_by_range, fetching new data")

    try:
        result = await get_mor_holders()
        set_cache_item('mor_holders_by_range', result)
        logger.info("New mor_holders_by_range data fetched and cached")
        return DataResponse(data=result)
    except Exception as e:
        logger.exception(f"An error occurred in mor_holders_by_range: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/locked_and_burnt_mor", response_model=DataResponse)
async def locked_and_burnt_mor():
    """Get locked and burnt MOR data."""
    cached_data = get_cache_item('locked_and_burnt_mor')
    logger.debug(f"Cache access for 'locked_and_burnt_mor': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    try:
        result = await get_historical_locked_and_burnt_mor()
        set_cache_item('locked_and_burnt_mor', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching locked and burnt MOR: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/protocol_liquidity", response_model=DataResponse)
async def get_protocol_liquidity():
    """Get protocol liquidity data."""
    cached_data = get_cache_item('protocol_liquidity')
    logger.debug(f"Cache access for 'protocol_liquidity': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    try:
        result = get_combined_uniswap_position()
        if not result:
            raise HTTPException(status_code=404, detail="Not Found")

        set_cache_item('protocol_liquidity', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching protocol liquidity: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/last_cache_update_time", response_model=DataResponse)
async def get_cache_update_time():
    """Get the last cache update time."""
    return DataResponse(data={"last_updated_time": get_last_cache_update_time()})


@app.get("/capital_metrics", response_model=DataResponse)
async def capital_metrics():
    """Get capital metrics data."""
    cached_data = get_cache_item('capital_metrics')
    logger.debug(f"Cache access for 'capital_metrics': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    try:
        result = get_capital_metrics()
        set_cache_item('capital_metrics', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching capital metrics: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching capital metrics")


@app.get("/github_commits", response_model=DataResponse)
async def get_github_commits():
    """Get GitHub commits data."""
    cached_data = get_cache_item('github_commits')
    logger.debug(f"Cache access for 'github_commits': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    try:
        result = get_commits_data()
        set_cache_item('github_commits', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching github commits: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching github commits")


@app.get("/historical_mor_rewards_locked", response_model=DataResponse)
async def get_historical_mor_staked():
    """Get historical MOR rewards locked data."""
    cached_data = get_cache_item('historical_mor_rewards_locked')
    logger.debug(f"Cache access for 'historical_mor_rewards_locked': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    try:
        result = await get_mor_staked_over_time()
        set_cache_item('historical_mor_rewards_locked', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching mor rewards locked: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/code_metrics", response_model=DataResponse)
async def get_code_metrics():
    """Get code metrics data."""
    cached_data = get_cache_item('code_metrics')
    logger.debug(f"Cache access for 'code_metrics': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    try:
        result = await get_total_weights_and_contributors()
        set_cache_item('code_metrics', result)
        return DataResponse(data=result)
    except Exception as e:
        logger.error(f"Error fetching code metrics: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")


@app.get("/chain_wise_supplies", response_model=DataResponse)
async def get_circ_supply_by_chains():
    """Get chain-wise circulating supply data."""
    cached_data = get_cache_item('chain_wise_supplies')
    logger.debug(f"Cache access for 'chain_wise_supplies': {'hit' if cached_data else 'miss'}, data: {cached_data}")
    if cached_data:
        return DataResponse(data=cached_data)

    try:
        result = get_chain_wise_circ_supply()
        set_cache_item('chain_wise_supplies', result)
        return DataResponse(data=result)

    except Exception as e:
        logger.error(f"Error fetching code in chain-wise supplies: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred")

@app.post("/job/{job_name}/start", status_code=201, response_model=MessageResponse)
async def start_job(job_name: str):
    """Start a background job."""
    if job_name == "process_blockchain_updates":
        scheduler.add_job(process_blockchain_updates, trigger='date', run_date=datetime.now())
    else:
        raise HTTPException(status_code=404, detail="Job not found")

    return MessageResponse(message="Job Accepted", success=True)


@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Health check endpoint."""
    return HealthCheckResponse(
        status="healthy",
        version="1.0.0",
        uptime=0.0,  # This would ideally be calculated from app start time
        timestamp=datetime.now(),
        components={
            "database": {"status": "up", "details": "Connected"},
            "cache": {"status": "up", "details": "In-memory cache active"},
            "env_vars": {
                "status": "up",
                "details": {
                    var: bool(os.getenv(var)) for var in [
                        'RPC_URL', 'ARB_RPC_URL', 'BASE_RPC_URL', 'ETHERSCAN_API_KEY',
                        'ARBISCAN_API_KEY', 'BASESCAN_API_KEY', 'DUNE_API_KEY',
                        'DUNE_QUERY_ID', 'GITHUB_API_KEY'
                    ]
                }
            }
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
