import pandas as pd
from typing import Dict, Union
from datetime import datetime
import logging
import time
from app.repository import EmissionRepository

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_emissions_data():
    """
    Get emissions data from the repository or a sheet as fallback.
        
    Returns:
        DataFrame with the emissions data
    """
    try:
        # First try to get data from the repository
        emission_repo = EmissionRepository()
        emissions = emission_repo.get_all(limit=1000)  # Get all emission records
        
        if emissions:
            # Convert to DataFrame
            emissions_data = [{
                'Day': emission.day,
                'Date': emission.date,
                'Capital Emission': float(emission.capital_emission),
                'Code Emission': float(emission.code_emission),
                'Compute Emission': float(emission.compute_emission),
                'Community Emission': float(emission.community_emission),
                'Protection Emission': float(emission.protection_emission),
                'Total Emission': float(emission.total_emission),
                'Total Supply': float(emission.total_supply)
            } for emission in emissions]
            
            emissions_df = pd.DataFrame(emissions_data)
            logger.info(f"Retrieved {len(emissions)} emission records from repository")
            return emissions_df
            
    except Exception as e:
        logger.error(f"Error reading emissions data: {str(e)}")
        raise


def read_emission_schedule(today_date: datetime, emissions_data: Union[str, pd.DataFrame]) -> Dict:
    """
    Read the emission schedule from the repository, Google Sheets, or a provided DataFrame 
    and return processed data for the current day.

    Args:
    today_date (datetime): Current date
    emissions_data (Union[str, pd.DataFrame]): Either the name of the Google Sheet or a DataFrame containing emission data

    Returns:
    Dict: Dictionary containing processed emission data
    """
    try:
        time.sleep(5)
        # If emissions_data is a string, assume it's a sheet name and fetch the data
        if isinstance(emissions_data, str):
            try:
                emissions_df = get_emissions_data(emissions_data)
            except Exception as e:
                logger.error(f"Error reading data source '{emissions_data}': {str(e)}")
                raise
        elif isinstance(emissions_data, pd.DataFrame):
            emissions_df = emissions_data
        else:
            raise ValueError("emissions_data must be either a sheet name (str) or a pandas DataFrame")

        emissions_df = emissions_df.dropna(axis=1, how='all')  # Remove empty columns

        # Strip whitespace from column names
        emissions_df.columns = emissions_df.columns.str.strip()

        # Convert the 'Date' column to datetime format (YYYY-MM-DD)
        emissions_df['Date'] = pd.to_datetime(emissions_df['Date'], format='%Y-%m-%d', errors='coerce').dt.normalize()

        # Normalize today's date (remove any time component)
        today_date_normalized = pd.to_datetime(today_date).normalize()

        # Filter data up to today's date
        df_until_today = emissions_df[emissions_df['Date'] <= today_date_normalized]

        if df_until_today.empty:
            logger.warning("No data found up to the specified date.")
            return {'new_emissions': {}, 'total_emissions': {}}

        # Calculate new emissions for today
        last_day = df_until_today.iloc[-1]
        previous_day = df_until_today.iloc[-2] if len(df_until_today) > 1 else pd.Series(0, index=last_day.index)

        emission_categories = ['Capital Emission', 'Code Emission', 'Compute Emission', 'Community Emission',
                               'Protection Emission']

        new_emissions = {category: float(last_day[category]) - float(previous_day[category]) for category in
                         emission_categories}

        # Calculate total emissions for today
        df_today = emissions_df[emissions_df['Date'] == today_date_normalized]

        if df_today.empty:
            total_emissions = {category: 0 for category in emission_categories}
            total_emissions['Total Emission'] = 0
        else:
            total_emissions = {category: float(df_today.iloc[0][category]) for category in emission_categories}
            total_emissions['Total Emission'] = float(df_today.iloc[0]['Total Emission'])

        new_emissions['Total Emission'] = sum(new_emissions.values())

        logger.info(f"Successfully processed emission data up to {today_date}")

        return {
            'new_emissions': new_emissions,
            'total_emissions': total_emissions
        }

    except Exception as e:
        logger.error(f"Error processing emission schedule: {str(e)}")
        raise


def get_historical_emissions():
    """
    Get historical emissions data from the repository.
    
    Returns:
        Dictionary with dates as keys and Total Emission values
    """
    try:
        # Try to get data from the repository
        emission_repo = EmissionRepository()
        emissions = emission_repo.get_all(limit=1000)  # Get all emission records
        
        if emissions:
            # Sort by date in descending order
            emissions.sort(key=lambda x: x.date, reverse=True)
            
            # Create the dictionary with dates as keys and Total Emission values
            historical_emissions_dict = {
                emission.date.strftime('%d/%m/%Y'): float(emission.total_emission)
                for emission in emissions
            }
            
            return historical_emissions_dict
    except Exception as e:
        logger.error(f"Error getting historical emissions from repository: {str(e)}")
        raise
