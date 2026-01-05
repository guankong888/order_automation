#!/usr/bin/env python3 COMMIT
import os
import logging
import pandas as pd
from pyairtable import Api
from msal import ConfidentialClientApplication  # Updated import
import requests
import base64
import re
from datetime import datetime, timedelta
import string
import usaddress
from fuzzywuzzy import process, fuzz

# === Loggging Configuration ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# === Helper Function for Date Range ===
def get_current_week_date_range():
    """
    Returns the current week's date range in the format "mm/dd-mm/dd/yyyy".
    Adjusted to start from **Sunday** and end on **Saturday**.
    Example: "12/29-01/04/2025"
    """
    today = datetime.today().date()
    start_of_week = today - timedelta(days=today.weekday() + 1)  # Adjusted to start on Sunday
    end_of_week = start_of_week + timedelta(days=6)  # Ends on Saturday
    start_str = start_of_week.strftime('%m/%d')
    end_str = end_of_week.strftime('%m/%d/%Y')
    date_range = f"{start_str}-{end_str}"
    return date_range

# === Configuration (From Env Vars or Hardcoded) ===
CLIENT_ID = os.environ.get("AZURE_CLIENT_ID", "2c775946-9535-45e5-9dc5-474c3da52e22")
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET", "wjT8Q~8iN1nhYUjYxg17lEs_fTGu7bF.mxmY4bNl")  # Ensure this is set
TENANT_ID = os.environ.get("AZURE_TENANT_ID", "d72741b9-6bf4-4282-8dfd-0af4f56d4023")
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"
EMAIL_ADDRESS = os.environ.get("EMAIL_ADDRESS", "stefan@n2gsupps.com")  # The mailbox to access
SAVE_DIR = os.environ.get("SAVE_DIR", "downloads")
MASTER_SHEET_PATH = os.environ.get("MASTER_SHEET_PATH", "NEW Master Location Sheet.csv")
AIRTABLE_ACCESS_TOKEN = os.environ.get("AIRTABLE_ACCESS_TOKEN", "pathhrkh0NSWxdpVy.452fa8995535bdf0bb0b76de386eb60ee6c98088c9eb0ef567a425f6d5ae831c")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appJrWoXe5H2YZnmU")
AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME", get_current_week_date_range())  # Updated line

TARGET_FOLDER_NAME = "REPORTS"
SUBJECT_FILTER = "contains(subject, 'PEPSI') or contains(subject, 'Supps') or contains(subject, 'N2G Water') or contains(subject, 'MF and DNA')"
ORDER_LOG_FILENAME = "club_order_log.csv"

REPORT_ADDRESS_COLUMN = "Shipping Address"

# === Helper Functions for Dates and Addresses ===
def parse_date_range(table_name):
    """
    Parses a date range string in the format "mm/dd-mm/dd/yyyy" and returns
    corresponding start and end dates as datetime.date objects.

    If the start month is greater than the end month, or the start month is equal
    to the end month but the start day is greater than the end day, it assumes
    the start date is in the previous year.

    Args:
        table_name (str): The table name containing the date range.

    Returns:
        tuple: (start_date, end_date) as datetime.date objects.
               Returns (None, None) if parsing fails.
    """
    # Split the table name into start and end parts
    parts = table_name.split('-')
    if len(parts) != 2:
        logging.error("Table name format unexpected. Cannot parse date range.")
        return None, None

    start_str = parts[0]  # "mm/dd"
    end_str = parts[1]    # "mm/dd/yyyy"

    try:
        # Parse end date components
        end_month, end_day, end_year = end_str.split('/')
        end_month = int(end_month)
        end_day = int(end_day)
        end_year = int(end_year)

        # Parse start date components
        start_month, start_day = start_str.split('/')
        start_month = int(start_month)
        start_day = int(start_day)

        # Determine if the date range spans across years
        if (start_month > end_month) or (start_month == end_month and start_day > end_day):
            # Start date is in the previous year
            start_year = end_year - 1
        else:
            # Start date is in the same year as end date
            start_year = end_year

        # Create datetime.date objects for start and end dates
        start_date = datetime.strptime(f"{start_month}/{start_day}/{start_year}", "%m/%d/%Y").date()
        end_date = datetime.strptime(f"{end_month}/{end_day}/{end_year}", "%m/%d/%Y").date()
        return start_date, end_date

    except ValueError as ve:
        logging.error("Could not parse start or end date from table name. Error: %s", ve)
        return None, None

start_date, end_date = parse_date_range(AIRTABLE_TABLE_NAME)
if start_date is None or end_date is None:
    logging.error("Could not parse date range from table name. Exiting.")
    exit()

logging.info("Filtering reports from %s to %s", start_date, end_date)

# Removed extract_report_date since we're pulling date from receivedDateTime

def authenticate_graph():
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )

    # Define the scope for application permissions
    scope = ["https://graph.microsoft.com/.default"]

    result = app.acquire_token_for_client(scopes=scope)

    if "access_token" in result:
        logging.info("Successfully authenticated using Confidential Client.")
        return result["access_token"]
    else:
        logging.error("Failed to authenticate: %s", result.get("error_description"))
        exit()

def get_folder_id(access_token, folder_name, user_email):
    headers = {"Authorization": f"Bearer {access_token}"}
    folders_url = f"{GRAPH_API_ENDPOINT}/users/{user_email}/mailFolders"
    while folders_url:
        response = requests.get(folders_url, headers=headers)
        if response.status_code != 200:
            logging.error("Error fetching folders: %s", response.text)
            break
        data = response.json()
        for folder in data.get("value", []):
            if folder["displayName"].lower() == folder_name.lower():
                return folder["id"]
        folders_url = data.get("@odata.nextLink")
    return None

def fetch_messages(access_token, folder_id, user_email):
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{GRAPH_API_ENDPOINT}/users/{user_email}/mailFolders/{folder_id}/messages?$filter={SUBJECT_FILTER}"
    all_messages = []
    while url:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logging.error("Error fetching messages: %s", response.text)
            break
        data = response.json()
        messages = data.get("value", [])
        all_messages.extend(messages)
        url = data.get("@odata.nextLink")
    return all_messages

def download_attachments(access_token, message):
    headers = {"Authorization": f"Bearer {access_token}"}
    attachments_url = f"{GRAPH_API_ENDPOINT}/users/{EMAIL_ADDRESS}/messages/{message['id']}/attachments"
    filepaths = []
    while attachments_url:
        attach_resp = requests.get(attachments_url, headers=headers)
        if attach_resp.status_code != 200:
            logging.warning("Error fetching attachments: %s", attach_resp.text)
            break
        attach_data = attach_resp.json()
        attachments = attach_data.get("value", [])
        for attachment in attachments:
            if attachment.get("name", "").endswith(".xlsx"):
                filepath = os.path.join(SAVE_DIR, attachment["name"])
                try:
                    with open(filepath, "wb") as f:
                        f.write(base64.b64decode(attachment["contentBytes"]))
                    logging.info("Downloaded: %s", attachment["name"])
                    filepaths.append(filepath)
                except Exception as e:
                    logging.error("Failed to write attachment %s to disk: %s", attachment["name"], e)
        attachments_url = attach_data.get("@odata.nextLink")
    return filepaths

def download_reports(access_token):
    logging.info("Fetching emails from the '%s' folder...", TARGET_FOLDER_NAME)
    target_folder_id = get_folder_id(access_token, TARGET_FOLDER_NAME, EMAIL_ADDRESS)
    if not target_folder_id:
        logging.error("'%s' folder not found.", TARGET_FOLDER_NAME)
        return []

    messages = fetch_messages(access_token, target_folder_id, EMAIL_ADDRESS)
    os.makedirs(SAVE_DIR, exist_ok=True)
    all_filepaths = []

    for msg in messages:
        subject = msg.get('subject', '')
        received_str = msg.get('receivedDateTime', '')
        logging.info("Processing email: %s", subject)

        if not received_str:
            logging.warning("Email '%s' does not have a 'receivedDateTime'; skipping.", subject)
            continue

        try:
            # Parse receivedDateTime to date
            # Example format: "2025-12-30T10:00:00Z"
            received_date = datetime.strptime(received_str, "%Y-%m-%dT%H:%M:%SZ").date()
            logging.debug("Email received date: %s", received_date)
        except ValueError:
            logging.warning("Could not parse 'receivedDateTime' for email '%s': %s", subject, received_str)
            continue

        # Check if the received date falls within the desired range
        if start_date <= received_date <= end_date:
            try:
                filepaths = download_attachments(access_token, msg)
                all_filepaths.extend(filepaths)
            except Exception as e:
                logging.error("Error downloading attachments for message '%s': %s", subject, e)
        else:
            logging.info("Skipping email '%s' because it does not fall within %s to %s", subject, start_date, end_date)

    return all_filepaths

def normalize_address(addr):
    if pd.isnull(addr) or addr.strip().lower() in ['nan', '']:
        logging.warning("Address is NaN or empty.")
        return None

    try:
        # Parse the address using usaddress
        parsed_address, address_type = usaddress.tag(addr)
    except usaddress.RepeatedLabelError as e:
        logging.warning("RepeatedLabelError for address '%s': %s", addr, e)
        return None

    # Define mapping for standardization
    directional_map = {
        'north': 'N',
        'south': 'S',
        'east': 'E',
        'west': 'W',
        'northeast': 'NE',
        'northwest': 'NW',
        'southeast': 'SE',
        'southwest': 'SW'
    }

    street_suffix_map = {
        'street': 'ST',
        'road': 'RD',
        'avenue': 'AVE',
        'boulevard': 'BLVD',
        'drive': 'DR',
        'lane': 'LN',
        'place': 'PL',
        'suite': 'STE',
        'apartment': 'APT',
        'floor': 'FL',
        'building': 'BLDG',
        'parkway': 'PKWY',
        'highway': 'HWY',
        'route': 'RTE',
        # Add more as needed
    }

    # Standardize directional
    for key, abbr in directional_map.items():
        if 'StreetNamePreDirectional' in parsed_address and parsed_address['StreetNamePreDirectional'].lower() == key:
            parsed_address['StreetNamePreDirectional'] = abbr
        if 'StreetNamePostDirectional' in parsed_address and parsed_address['StreetNamePostDirectional'].lower() == key:
            parsed_address['StreetNamePostDirectional'] = abbr

    # Standardize street suffix
    if 'StreetNamePostType' in parsed_address:
        suffix = parsed_address['StreetNamePostType'].lower()
        if suffix in street_suffix_map:
            parsed_address['StreetNamePostType'] = street_suffix_map[suffix]

    # Standardize suite/unit
    if 'OccupancyType' in parsed_address:
        occupancy = parsed_address['OccupancyType'].lower()
        parsed_address['OccupancyType'] = 'STE' if occupancy in ['suite', 'ste'] else parsed_address['OccupancyType'].upper()

    # Reconstruct the normalized address
    components = []
    if 'AddressNumber' in parsed_address:
        components.append(parsed_address['AddressNumber'].upper())
    if 'StreetNamePreDirectional' in parsed_address:
        components.append(parsed_address['StreetNamePreDirectional'])
    if 'StreetName' in parsed_address:
        components.append(parsed_address['StreetName'].upper())
    if 'StreetNamePostType' in parsed_address:
        components.append(parsed_address['StreetNamePostType'])
    if 'StreetNamePostDirectional' in parsed_address:
        components.append(parsed_address['StreetNamePostDirectional'])
    if 'OccupancyType' in parsed_address and 'OccupancyIdentifier' in parsed_address:
        components.append(f"{parsed_address['OccupancyType']} {parsed_address['OccupancyIdentifier'].upper()}")

    # Join street components
    street_address = ' '.join(components)

    # City, State ZIP
    city = parsed_address.get('PlaceName', '').upper()
    state = parsed_address.get('StateName', '').upper()
    zip_code = parsed_address.get('ZipCode', '')

    # Check for missing city, state, or zip
    if not city or not state or not zip_code:
        logging.warning("Incomplete address components for '%s'. Expected City, State, ZIP.", addr)

    normalized = f"{street_address}, {city}, {state} {zip_code}".strip()

    return normalized

def extract_street_address(addr):
    if pd.isnull(addr) or not isinstance(addr, str):
        return addr
    # Split by comma and take the first part (street address)
    parts = addr.split(',')
    street = parts[0].strip()
    return street

def load_master_data(path):
    try:
        master_data = pd.read_csv(path, encoding='latin-1')
        if master_data.shape[1] < 3:
            logging.error("Master sheet does not have at least 3 columns.")
            return None

        master_data = master_data.iloc[:, [1, 2]]
        master_data.columns = ["Club Code", "Address"]

        # Remove invalid entries
        master_data = master_data[master_data["Club Code"].str.upper() != 'NAN']
        master_data = master_data[master_data["Address"].str.lower() != 'nan']

        # Normalize the Club Code and Address
        master_data["Club Code"] = master_data["Club Code"].astype(str).str.strip().str.upper()
        master_data["Normalized_Address"] = master_data["Address"].astype(str).apply(normalize_address)

        # Drop rows with invalid or missing addresses
        master_data = master_data.dropna(subset=["Normalized_Address"])

        # Extract Street_Address from Normalized_Address
        master_data["Street_Address"] = master_data["Normalized_Address"].apply(extract_street_address)

        # Drop rows with invalid or missing street addresses
        master_data = master_data.dropna(subset=["Street_Address"])

        # Log the first few rows of master data
        logging.info("Master data preview:\n%s", master_data.head().to_string(index=False))

        # Log all unique normalized street addresses for verification
        logging.info("Total unique normalized street addresses in master data: %d", master_data["Street_Address"].nunique())

        # Filter master data to only 5-character uppercase alphanumeric Club Codes
        master_data = master_data[master_data["Club Code"].str.match(r"^[A-Z0-9]{5}$")]

        # Log the first few rows of filtered master data
        logging.info("Filtered master data to only 5-character uppercase alphanumeric codes:\n%s", master_data.head().to_string(index=False))

        return master_data
    except Exception as e:
        logging.error("Error reading master sheet: %s", e)
        return None

def normalize_addresses_in_report(report, address_column):
    # Create a new 'Normalized_Address' column instead of overwriting the original
    report['Normalized_Address'] = report[address_column].astype(str).apply(normalize_address)
    # Drop rows with invalid or missing normalized addresses
    report = report.dropna(subset=['Normalized_Address'])
    # Extract Street_Address from Normalized_Address using .loc to avoid SettingWithCopyWarning
    report.loc[:, 'Street_Address'] = report['Normalized_Address'].apply(
        lambda x: x.split(',')[0].strip() if ',' in x else x
    )

    # Debug: Log unique normalized report addresses
    logging.debug("Unique normalized report addresses:\n%s", report['Street_Address'].unique())

    return report

def process_reports(filepaths, master_data):
    logging.info("Processing reports and mapping to club codes...")
    all_club_data = {}
    unmatched_addresses = set()

    for file in filepaths:
        file_lower = os.path.basename(file).lower()
        try:
            report = pd.read_excel(file)
            logging.info("Loaded report file: %s", file)
        except Exception as e:
            logging.warning("Skipping file %s due to read error: %s", file, e)
            continue

        if REPORT_ADDRESS_COLUMN not in report.columns:
            logging.warning("File %s has no '%s' column; skipping.", file, REPORT_ADDRESS_COLUMN)
            continue

        # Determine report type
        is_pepsi = "pepsi" in file_lower
        is_n2g = "n2g water" in file_lower or "n2g_water" in file_lower
        is_supps = "supps" in file_lower

        # Normalize addresses in the report
        report = normalize_addresses_in_report(report, REPORT_ADDRESS_COLUMN)

        # Extract Street_Address from Normalized_Address
        report["Street_Address"] = report["Normalized_Address"].apply(
            lambda x: x.split(',')[0].strip() if ',' in x else x
        )
        unique_addresses = report["Street_Address"].unique()

        # Check if "Variant SKU" column exists
        has_variant_sku = "Variant SKU" in report.columns

        for address in unique_addresses:
            if not address:
                continue

            # Exact match on street address
            if address in master_data["Street_Address"].tolist():
                club_code = master_data.set_index("Street_Address").loc[address, "Club Code"]
                if club_code not in all_club_data:
                    all_club_data[club_code] = {
                        "Club Code": club_code, 
                        "PEPSI": "N", 
                        "Supps": "N", 
                        "N2G WATER": "N", 
                        "DNA Order": "N", 
                        "MF/FAIRE Order": "N"
                    }

                if is_pepsi:
                    all_club_data[club_code]["PEPSI"] = "Y"
                if is_supps:
                    all_club_data[club_code]["Supps"] = "Y"
                    logging.info("Supps order detected for address: %s -> Club Code: %s", address, club_code)
                if is_n2g:
                    all_club_data[club_code]["N2G WATER"] = "Y"
                
                logging.info("Exact match found for address: %s -> Club Code: %s", address, club_code)

                # **Skip Variant SKU logic for Supps reports**
                if is_supps:
                    continue

                # **Apply Variant SKU logic for non-Supps reports**
                if has_variant_sku:
                    filtered_rows = report[report["Street_Address"] == address]
                    for _, row in filtered_rows.iterrows():
                        variant_sku = str(row.get("Variant SKU", ""))
                        if not variant_sku:
                            continue
                        if '-' in variant_sku:
                            all_club_data[club_code]["DNA Order"] = "Y"
                        else:
                            all_club_data[club_code]["MF/FAIRE Order"] = "Y"
            else:
                # Fuzzy match on street address
                master_street_addresses = master_data["Street_Address"].tolist()
                match, score = process.extractOne(address, master_street_addresses, scorer=fuzz.token_sort_ratio)
                if score >= 90:
                    club_code = master_data.set_index("Street_Address").loc[match, "Club Code"]
                    if club_code not in all_club_data:
                        all_club_data[club_code] = {
                            "Club Code": club_code, 
                            "PEPSI": "N", 
                            "Supps": "N", 
                            "N2G WATER": "N", 
                            "DNA Order": "N", 
                            "MF/FAIRE Order": "N"
                        }

                    if is_pepsi:
                        all_club_data[club_code]["PEPSI"] = "Y"
                    if is_supps:
                        all_club_data[club_code]["Supps"] = "Y"
                        logging.info("Supps order detected (fuzzy match) for address: %s -> Club Code: %s", address, club_code)
                    if is_n2g:
                        all_club_data[club_code]["N2G WATER"] = "Y"

                    # **Skip Variant SKU logic for Supps reports**
                    if is_supps:
                        continue

                    if has_variant_sku:
                        filtered_rows = report[report["Street_Address"] == address]
                        for _, row in filtered_rows.iterrows():
                            variant_sku = str(row.get("Variant SKU", ""))
                            if not variant_sku:
                                continue
                            if '-' in variant_sku:
                                all_club_data[club_code]["DNA Order"] = "Y"
                            else:
                                all_club_data[club_code]["MF/FAIRE Order"] = "Y"
                else:
                    unmatched_addresses.add(address)
                    logging.warning("No match found for address: %s", address)

    if unmatched_addresses:
        logging.info("Unmatched addresses:")
        for ua in unmatched_addresses:
            logging.info("- %s", ua)

    if not all_club_data:
        logging.info("No club data matched.")
        return pd.DataFrame(columns=["Club Code", "PEPSI", "Supps", "N2G WATER", "DNA Order", "MF/FAIRE Order"])

    club_summary = pd.DataFrame.from_dict(all_club_data, orient="index")
    club_summary.reset_index(drop=True, inplace=True)

    logging.info("Processed club summary:\n%s", club_summary.to_string(index=False))

    return club_summary

def save_order_database(club_summary):
    if not club_summary.empty:
        logging.info("Final Order Database:\n%s", club_summary.to_string(index=False))
    else:
        logging.info("Final Order Database is empty.")
    output_path = os.path.join(SAVE_DIR, ORDER_LOG_FILENAME)
    try:
        club_summary.to_csv(output_path, index=False)
        logging.info("Order database saved to %s", output_path)
    except Exception as e:
        logging.error("Failed to save order database to %s: %s", output_path, e)

def update_airtable(club_summary):
    if club_summary.empty:
        logging.info("No clubs to update in Airtable.")
        return

    logging.info("Updating Airtable...")
    # Initialize Airtable API
    try:
        api = Api(api_key=AIRTABLE_ACCESS_TOKEN)
        table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    except Exception as e:
        logging.error("Failed to initialize Airtable API: %s", e)
        return

    for _, row in club_summary.iterrows():
        club_code = row["Club Code"]
        pepsi_val = (row["PEPSI"] == "Y")
        supps_val = (row["Supps"] == "Y")
        n2g_val = (row["N2G WATER"] == "Y")
        dna_order_val = (row.get("DNA Order", "N") == "Y")
        mf_faire_order_val = (row.get("MF/FAIRE Order", "N") == "Y")

        # Airtable formula for exact match on Club Code
        formula = f"{{New Code}} = '{club_code}'"
        try:
            records = table.all(formula=formula)
        except Exception as e:
            logging.error("Error fetching records for Club Code '%s': %s", club_code, e)
            continue

        if records:
            for rec in records:
                record_id = rec["id"]
                try:
                    table.update(record_id, {
                        "PEPSI": pepsi_val,
                        "SUPP RESTOCK": supps_val,
                        "N2G Water": n2g_val,
                        "DNA Order": dna_order_val,
                        "MF/FAIRE Order": mf_faire_order_val
                    })
                    logging.info("Updated Airtable record for Club Code '%s'.", club_code)
                except Exception as e:
                    logging.error("Error updating Airtable record '%s' for Club Code '%s': %s", record_id, club_code, e)
        else:
            logging.warning("Club Code '%s' not found in Airtable; skipping.", club_code)

def main():
    access_token = authenticate_graph()
    report_files = download_reports(access_token)

    logging.info("Report files found: %d", len(report_files))

    if not report_files:
        logging.info("No reports downloaded after filtering by date. Exiting.")
        return

    master_data = load_master_data(MASTER_SHEET_PATH)
    if master_data is None or master_data.empty:
        logging.error("Master data could not be loaded or is empty after filtering. Exiting.")
        return

    logging.info("Master data rows loaded: %d", len(master_data))

    try:
        club_summary = process_reports(report_files, master_data)

        if club_summary is not None:
            logging.info("Club summary rows generated: %d", len(club_summary))

        save_order_database(club_summary)

        if not club_summary.empty:
            update_airtable(club_summary)
            logging.info("Process completed successfully!")
        else:
            logging.info("No clubs to update in Airtable.")
    except Exception as e:
        logging.error("Error during processing: %s", e)

if __name__ == "__main__":
    main()
