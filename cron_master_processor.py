import asyncio
import logging
from datetime import datetime
import traceback
import time

from app.cache.cache_manager import clear_cache
from app.db.database import get_db
from scripts.i_update_user_claim_locked_events import process_user_claim_locked_events
from scripts.ii_update_user_multipliers import process_user_multiplier_events
from scripts.iii_update_total_daily_rewards import process_reward_events
from scripts.iiii_update_circulating_supply import process_circulating_supply_events
from scripts.iv_update_user_staked_events import process_user_staked_events
from scripts.v_update_user_withdrawn_events import process_user_withdrawn_events
from scripts.vi_update_overplus_bridged_events import process_overplus_bridged_events
# from scripts.vii_update_emissions import update_emissions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='master_update.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

async def process_blockchain_updates():
    start_time = datetime.now()
    logger.info(f"Starting update process at {start_time}")

    try:
        # Step 1: Update User Claim Locked Events
        logger.info("Step 1: Updating User Claim Locked Events")
        process_user_claim_locked_events()
        logger.info("Step 1 completed successfully")
        time.sleep(5)

        # Step 2: Update User Multipliers
        logger.info("Step 2: Updating User Multipliers")
        await process_user_multiplier_events()
        logger.info("Step 2 completed successfully")
        time.sleep(5)

        # Step 3: Update Total and Daily Rewards
        logger.info("Step 3: Updating Total and Daily Rewards")
        await process_reward_events()
        logger.info("Step 3 completed successfully")
        time.sleep(5)

        # Step 4: Update Circulating Supply
        logger.info("Step 4: Updating Circulating Supply")
        process_circulating_supply_events()
        logger.info("Step 4 completed successfully")
        time.sleep(5)

        # Step 5: Update User Staked Events
        logger.info("Step 5: Updating User Staked Events")
        process_user_staked_events()
        logger.info("Step 5 completed successfully")
        time.sleep(5)

        # Step 6: Update User Withdrawn Events
        logger.info("Step 6: Updating User Withdrawn Events")
        process_user_withdrawn_events()
        logger.info("Step 6 completed successfully")
        time.sleep(5)

        # Step 7: Update Overplus Bridged Events
        logger.info("Step 7: Updating Overplus Bridged Events")
        process_overplus_bridged_events()
        logger.info("Step 7 completed successfully")
        time.sleep(5)

        # Step 8: Update Emissions Data
        # Disable this job for now as we have data up till 2027
        # logger.info("Step 8: Updating Emissions Data")
        # update_emissions()
        # logger.info("Step 8 completed successfully")
        # time.sleep(5)

        end_time = datetime.now()
        duration = end_time - start_time
        success_message = (f"Update process completed successfully at {end_time}. "
                           f"Total duration: {duration}")
        logger.info(success_message)

        logger.info("Clearing read cache for new data")
        clear_cache()

    except Exception as e:
        error_message = f"Error in update process: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        raise
    finally:
        try:
            db = get_db()
            db.close()
            logger.info("Database connections closed")
        except Exception as e:
            logger.warning(f"Error closing database connections: {str(e)}")


if __name__ == "__main__":
    asyncio.run(process_blockchain_updates())
