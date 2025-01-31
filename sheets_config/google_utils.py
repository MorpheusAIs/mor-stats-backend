import csv
import os
import json
import gspread
import pandas as pd
import logging
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MOUNTED_CREDENTIALS_PATH = "/config/credentials.json"
FALLBACK_CREDENTIALS_PATH = os.path.join(os.getcwd(), 'sheets_config', 'credentials.json')

def get_credentials():
    # Try mounted path first
    if os.path.exists(MOUNTED_CREDENTIALS_PATH):
        logger.info(f"Using credentials from mounted path: {MOUNTED_CREDENTIALS_PATH}")
        return ServiceAccountCredentials.from_json_keyfile_name(MOUNTED_CREDENTIALS_PATH, scope)

    # Try environment variable
    GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if GOOGLE_SHEETS_CREDENTIALS:
        logger.info("Using credentials from environment variable")
        try:
            credentials_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
            return ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse GOOGLE_SHEETS_CREDENTIALS: {str(e)}")
            raise

    # Try fallback path
    if os.path.exists(FALLBACK_CREDENTIALS_PATH):
        logger.info(f"Using credentials from fallback path: {FALLBACK_CREDENTIALS_PATH}")
        return ServiceAccountCredentials.from_json_keyfile_name(FALLBACK_CREDENTIALS_PATH, scope)

    # If all attempts fail, raise error
    available_paths = {
        "mounted_path": os.path.exists(MOUNTED_CREDENTIALS_PATH),
        "env_var": bool(GOOGLE_SHEETS_CREDENTIALS),
        "fallback_path": os.path.exists(FALLBACK_CREDENTIALS_PATH)
    }
    raise FileNotFoundError(f"No credentials found. Attempted paths: {available_paths}")

try:
    credentials = get_credentials()
    gc = gspread.authorize(credentials)
    sh = gc.open_by_key(SPREADSHEET_ID)
except Exception as e:
    logger.error(f"Failed to initialize Google Sheets client: {str(e)}")
    raise

def get_worksheet(sheet_name):
    return sh.worksheet(sheet_name)

def download_sheet(sheet_name):
    worksheet = get_worksheet(sheet_name)
    logger.info(f"Downloading current uploaded sheet for {sheet_name}...")
    filename = f'downloaded_{sheet_name}.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(worksheet.get_all_values())
    logger.info(f"Downloaded data as CSV: {filename}")
    return filename

def read_sheet_to_dataframe(sheet_name):
    worksheet = sh.worksheet(sheet_name)
    return pd.DataFrame(worksheet.get_all_records())

def append_to_sheet(sheet_name, dataframe):
    worksheet = get_worksheet(sheet_name)
    values = dataframe.values.tolist()
    worksheet.append_rows(values)
    print(f"Appended {len(values)} rows to {sheet_name}")

def clear_and_upload_new_records(sheet_name, dataframe):
    worksheet = get_worksheet(sheet_name)
    worksheet.clear()
    print(f"Clearing existing data from {sheet_name}...")

    values = [dataframe.columns.tolist()] + dataframe.values.tolist()
    worksheet.update(values)
    print(f"Uploaded {len(values)} rows to {sheet_name}")

def cleanup_temp_files():
    for filename in os.listdir():
        if filename.startswith('downloaded_') and filename.endswith('.csv'):
            os.remove(filename)
            print(f"Removed temporary file: {filename}")

