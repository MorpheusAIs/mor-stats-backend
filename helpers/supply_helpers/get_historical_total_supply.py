from collections import OrderedDict
from datetime import datetime
import pandas as pd
import logging

from helpers.staking_helpers.get_emission_schedule_for_today import get_emissions_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_total_supply_from_emissions_df():
    emissions_df = get_emissions_data()

    # Ensure the DataFrame has the required columns
    required_columns = ['Date', 'Total Supply']
    if not all(col in emissions_df.columns for col in required_columns):
        logger.error(f"Emissions DataFrame is missing required columns. Available columns: {emissions_df.columns}")
        return OrderedDict()

    try:
        # Convert the Date column to datetime for comparison
        emissions_df['Date'] = pd.to_datetime(emissions_df['Date'])

        # Get the current date
        current_date = datetime.utcnow().date()

        # Filter rows from the earliest date in the DataFrame to the current date
        df = emissions_df[emissions_df['Date'].dt.date <= current_date]

        # Sort by date in descending order (latest first)
        df = df.sort_values(by='Date', ascending=False)

        # Round the 'Total Supply' column to 4 decimal places
        df['Total Supply'] = df['Total Supply'].round(4)

        # Create JSON output: dates as keys, total supply as values
        json_output = OrderedDict()
        for _, row in df.iterrows():
            date_str = row['Date'].strftime('%d/%m/%Y')  # Convert to DD/MM/YYYY format
            total_supply = row['Total Supply']
            json_output[date_str] = total_supply

        return json_output

    except Exception as e:
        logger.error(f"Error processing emissions data: {str(e)}")
        return OrderedDict()