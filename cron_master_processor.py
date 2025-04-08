import asyncio
import logging
from datetime import datetime
import traceback

from scripts.i_update_user_claim_locked_events import process_events
from scripts.ii_update_user_multipliers import calculate_user_multipliers
from scripts.iii_update_total_daily_rewards import calculate_rewards
from scripts.iiii_update_circulating_supply import update_circulating_supply_sheet
from scripts.iv_update_user_staked_events import process_user_staked_events
from scripts.v_update_user_withdrawn_events import process_user_withdrawn_events
from scripts.vi_update_overplus_bridged_events import process_overplus_bridged_events

import time

from sheets_config.slack_notify import slack_notification_cron

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='master_update.log',
    filemode='a'
)
logger = logging.getLogger(__name__)


async def run_update_process():
    start_time = datetime.now()
    logger.info(f"Starting update process at {start_time}")
    slack_notification_cron(f"Starting update process at {start_time}")

    try:
        # Step 1: Update User Claim Locked Events
        logger.info("Step 1: Updating User Claim Locked Events")
        process_events()
        logger.info("Step 1 completed successfully")
        time.sleep(5)

        # Step 2: Update User Multipliers
        logger.info("Step 2: Updating User Multipliers")
        await calculate_user_multipliers()
        logger.info("Step 2 completed successfully")
        time.sleep(5)

        # Step 3: Update Total and Daily Rewards
        logger.info("Step 3: Updating Total and Daily Rewards")
        await calculate_rewards()
        logger.info("Step 3 completed successfully")
        time.sleep(5)

        # Step 4: Update Circulating Supply
        logger.info("Step 4: Updating Circulating Supply Sheet")
        update_circulating_supply_sheet()
        logger.info("Step 4 completed successfully")
        time.sleep(5)

        # Step 5: Update User Staked Events
        logger.info("Step : Updating UserStaked Sheet")
        process_user_staked_events()
        logger.info("Step 5 completed successfully")
        time.sleep(5)

        # Step 6: Update User Withdrawn
        logger.info("Step 6: Updating UserWithdrawn Sheet")
        process_user_withdrawn_events()
        logger.info("Step 6 completed successfully")
        time.sleep(5)

        logger.info("Step 7: Updating OverplusBridged Sheet")
        process_overplus_bridged_events()
        logger.info("Step 7 completed successfully")
        time.sleep(5)

        end_time = datetime.now()
        duration = end_time - start_time
        success_message = (f"Update process completed successfully at {end_time}. "
                           f"Total duration: {duration}")
        logger.info(success_message)
        slack_notification_cron(success_message)

    except Exception as e:
        error_message = f"Error in update process: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        slack_notification_cron(f"Error in update process: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(run_update_process())
