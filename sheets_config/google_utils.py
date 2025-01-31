import csv
import os
import json
import gspread
import pandas as pd
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
MOUNTED_CREDENTIALS_PATH = "/config/credentials.json"

try:
    # Try to use the mounted credentials file
    credentials = ServiceAccountCredentials.from_json_keyfile_name(MOUNTED_CREDENTIALS_PATH, scope)
except Exception as e:
    # Fallback to environment variable if file not found
    GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if not GOOGLE_SHEETS_CREDENTIALS:
        raise ValueError("Neither mounted credentials file nor GOOGLE_SHEETS_CREDENTIALS environment variable found")
    
    try:
        credentials_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    except json.JSONDecodeError as e:
        raise ValueError(f"GOOGLE_SHEETS_CREDENTIALS is not valid JSON: {str(e)}")
    except Exception as e:
        raise Exception(f"Failed to create credentials: {str(e)}")

gc = gspread.authorize(credentials)
sh = gc.open_by_key(SPREADSHEET_ID)


def get_worksheet(sheet_name):
    return sh.worksheet(sheet_name)


def download_sheet(sheet_name):
    worksheet = get_worksheet(sheet_name)
    print(f"Downloading current uploaded sheet for {sheet_name}...")
    filename = f'downloaded_{sheet_name}.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(worksheet.get_all_values())
    print(f"Downloaded data as CSV: {filename}")
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

