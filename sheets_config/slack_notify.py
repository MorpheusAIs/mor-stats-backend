import logging
import sys
import time
import json
import requests
from app.core.config import NOTIFICATION_CHANNEL
from app.core.config import SLACK_URL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def slack_notification(message):
    time.sleep(1)
    url = SLACK_URL
    slack_data = {
        "username": "morpheus-explorer",
        "icon_emoji": ":satellite_antenna:",
        "channel": NOTIFICATION_CHANNEL,
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        logger.info(f"Failed to send Slack notification: {response.status_code}, {response.text}")
    else:
        logger.info("Slack notification sent successfully")