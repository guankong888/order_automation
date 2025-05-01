#!/usr/bin/env python3
import os
import logging
import pandas as pd
import requests
import base64
import re
from datetime import datetime, timedelta
from pyairtable import Api
from msal import ConfidentialClientApplication
import usaddress
from fuzzywuzzy import process, fuzz

# === Logging Configuration ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# === Helper Function for Date Range ===
def get_current_week_date_range():
    today = datetime.today().date()
    start_of_week = today - timedelta(days=today.weekday() + 1)  # Sunday
    end_of_week   = start_of_week + timedelta(days=6)            # Saturday
    return f"{start_of_week.strftime('%m/%d')}-{end_of_week.strftime('%m/%d/%Y')}"

# === Configuration ===
CLIENT_ID        = os.environ.get("AZURE_CLIENT_ID",     "2c775946-9535-45e5-9dc5-474c3da52e22")
CLIENT_SECRET    = os.environ.get("AZURE_CLIENT_SECRET", "wjT8Q~8iN1nhYUjYxg17lEs_fTGu7bF.mxmY4bNl")
TENANT_ID        = os.environ.get("AZURE_TENANT_ID",     "d72741b9-6bf4-4282-8dfd-0af4f56d4023")
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"

EMAIL_ADDRESS       = os.environ.get("EMAIL_ADDRESS",       "stefan@n2gsupps.com")
SAVE_DIR            = os.environ.get("SAVE_DIR",            "downloads")
MASTER_SHEET_PATH   = os.environ.get(
    "MASTER_SHEET_PATH",
    "https://raw.githubusercontent.com/guankong888/n2g_mls-_fetcher/main/master_location_sheet.csv"
)
AIRTABLE_ACCESS_TOKEN = os.environ.get(
    "AIRTABLE_ACCESS_TOKEN",
    "pathhrkh0NSWxdpVy.452fa8995535bdf0bb0b76de386eb60ee6c98088c9eb0ef567a425f6d5ae831c"
)
AIRTABLE_BASE_ID      = os.environ.get("AIRTABLE_BASE_ID",      "appJrWoXe5H2YZnmU")
AIRTABLE_TABLE_NAME   = os.environ.get("AIRTABLE_TABLE_NAME",   get_current_week_date_range())

TARGET_FOLDER_NAME    = "REPORTS"
SUBJECT_FILTER        = "contains(subject, 'PEPSI') or contains(subject, 'Supps') or contains(subject, 'N2G Water') or contains(subject, 'MF and DNA')"
ORDER_LOG_FILENAME    = "club_order_log.csv"
REPORT_ADDRESS_COLUMN = "Shipping Address"

# ────────────────────────────────────────────────────────────────────────────────
# Date‐range parser for AIRTABLE_TABLE_NAME
def parse_date_range(table_name):
    parts = table_name.split('-')
    if len(parts) != 2:
        logging.error("Table name format unexpected. Cannot parse date range.")
        return None, None
    start_str, end_str = parts
    try:
        em, ed, ey = map(int, end_str.split('/'))
        sm, sd      = map(int, start_str.split('/'))
        sy = ey - 1 if (sm, sd) > (em, ed) else ey
        start_date = datetime.strptime(f"{sm}/{sd}/{sy}", "%m/%d/%Y").date()
        end_date   = datetime.strptime(f"{em}/{ed}/{ey}", "%m/%d/%Y").date()
        return start_date, end_date
    except Exception as e:
        logging.error("Could not parse date range '%s': %s", table_name, e)
        return None, None

start_date, end_date = parse_date_range(AIRTABLE_TABLE_NAME)
if not start_date:
    logging.error("Aborting: bad AIRTABLE_TABLE_NAME.")
    exit()
logging.info("Filtering reports from %s to %s", start_date, end_date)

# ────────────────────────────────────────────────────────────────────────────────
# Graph Authentication
def authenticate_graph():
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise RuntimeError("Graph auth failed: " + result.get("error_description","<no error>"))
    return result["access_token"]

# ────────────────────────────────────────────────────────────────────────────────
# Mail / Attachment Fetching
def get_folder_id(token, name, user_email):
    hdr = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_API_ENDPOINT}/users/{user_email}/mailFolders"
    while url:
        r = requests.get(url, headers=hdr); r.raise_for_status()
        data = r.json()
        for f in data.get("value", []):
            if f["displayName"].lower() == name.lower():
                return f["id"]
        url = data.get("@odata.nextLink")
    return None

def fetch_messages(token, folder_id):
    hdr = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_API_ENDPOINT}/users/{EMAIL_ADDRESS}/mailFolders/{folder_id}/messages?$filter={SUBJECT_FILTER}"
    msgs = []
    while url:
        r = requests.get(url, headers=hdr); r.raise_for_status()
        data = r.json(); msgs += data.get("value", [])
        url = data.get("@odata.nextLink")
    return msgs

def download_attachments(token, msg):
    hdr = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_API_ENDPOINT}/users/{EMAIL_ADDRESS}/messages/{msg['id']}/attachments"
    paths = []
    while url:
        r = requests.get(url, headers=hdr); r.raise_for_status()
        for att in r.json().get("value", []):
            if att.get("name","").endswith(".xlsx"):
                os.makedirs(SAVE_DIR, exist_ok=True)
                p = os.path.join(SAVE_DIR, att["name"])
                open(p,"wb").write(base64.b64decode(att["contentBytes"]))
                logging.info("Downloaded: %s", p)
                paths.append(p)
        url = r.json().get("@odata.nextLink")
    return paths

def download_reports(token):
    fid = get_folder_id(token, TARGET_FOLDER_NAME, EMAIL_ADDRESS)
    if not fid:
        logging.error("Folder '%s' not found", TARGET_FOLDER_NAME); return []
    msgs = fetch_messages(token, fid)
    out = []
    for m in msgs:
        dt = m.get("receivedDateTime","")
        try:
            d = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").date()
        except:
            continue
        if start_date <= d <= end_date:
            out += download_attachments(token, m)
    return out

# ────────────────────────────────────────────────────────────────────────────────
# Address Normalization
def normalize_address(addr):
    if pd.isnull(addr) or not isinstance(addr, str) or not addr.strip():
        return None
    try:
        parsed, _ = usaddress.tag(addr)
    except usaddress.RepeatedLabelError:
        return None
    # directional / suffix maps omitted for brevity—reuse yours
    # ...do your directional_map, street_suffix_map, occupancy...
    comp = []
    for part in ["AddressNumber","StreetNamePreDirectional","StreetName",
                 "StreetNamePostType","StreetNamePostDirectional"]:
        if part in parsed:
            comp.append(parsed[part].upper())
    if "OccupancyType" in parsed and "OccupancyIdentifier" in parsed:
        comp.append(f"{parsed['OccupancyType'].upper()} {parsed['OccupancyIdentifier'].upper()}")
    street = " ".join(comp)
    city  = parsed.get("PlaceName","").upper()
    state = parsed.get("StateName","").upper()
    zipc  = parsed.get("ZipCode","")
    if not (street and city and state and zipc):
        return None
    return f"{street}, {city}, {state} {zipc}"

def extract_street_address(addr):
    return addr.split(",")[0].strip() if isinstance(addr, str) and "," in addr else addr

# ────────────────────────────────────────────────────────────────────────────────
# Master‐sheet loader (from URL or disk)
def load_master_data(path):
    try:
        df = pd.read_csv(path, dtype=str)
        df = df.iloc[:,[1,2]]                     # only two cols
        df.columns = ["Club Code","Address"]
        # drop accidental extra headers
        df = df[df["Club Code"]!="Club Code"]
        df = df.dropna(subset=["Club Code","Address"])
        df["Club Code"] = df["Club Code"].str.upper().str.strip()
        df["Normalized_Address"] = df["Address"].apply(normalize_address)
        df = df.dropna(subset=["Normalized_Address"])
        df["Street_Address"] = df["Normalized_Address"].apply(extract_street_address)
        df = df[df["Club Code"].str.match(r"^[A-Z0-9]{5}$")]
        logging.info("Loaded %d clubs from master sheet", len(df))
        return df
    except Exception as e:
        logging.error("Error loading master data: %s", e)
        return None

# ────────────────────────────────────────────────────────────────────────────────
# Report processing & Airtable updating…
def normalize_addresses_in_report(report, col):
    report["Normalized_Address"] = report[col].astype(str).apply(normalize_address)
    report = report.dropna(subset=["Normalized_Address"])
    report["Street_Address"]   = report["Normalized_Address"].apply(extract_street_address)
    return report

def process_reports(files, master_data):
    all_clubs = {}
    unmatched = set()
    for f in files:
        df = pd.read_excel(f)
        if REPORT_ADDRESS_COLUMN not in df.columns:
            continue
        df = normalize_addresses_in_report(df, REPORT_ADDRESS_COLUMN)
        is_pepsi = "pepsi" in f.lower()
        is_supps = "supps" in f.lower()
        is_n2g   = "n2g water" in f.lower()
        for addr in df["Street_Address"].unique():
            if not addr: continue
            if addr in master_data["Street_Address"].tolist():
                code = master_data.set_index("Street_Address").at[addr,"Club Code"]
            else:
                match,score = process.extractOne(addr, master_data["Street_Address"], scorer=fuzz.token_sort_ratio)
                if score>=90:
                    code = master_data.set_index("Street_Address").at[match,"Club Code"]
                else:
                    unmatched.add(addr)
                    continue
            entry = all_clubs.setdefault(code, dict(Club=code, PEPSI="N",Supps="N",N2G="N",DNA="N",MF="N"))
            if is_pepsi: entry["PEPSI"] = "Y"
            if is_supps:  entry["Supps"] = "Y"
            if is_n2g:    entry["N2G"]   = "Y"
            # variant‐SKU logic omitted here for brevity…
    if unmatched:
        logging.info("Unmatched addrs:\n%s", "\n".join(unmatched))
    return pd.DataFrame(all_clubs.values())

def save_order_database(df):
    os.makedirs(SAVE_DIR, exist_ok=True)
    out = os.path.join(SAVE_DIR, ORDER_LOG_FILENAME)
    df.to_csv(out, index=False)
    logging.info("Saved order log to %s", out)

def update_airtable(df):
    if df.empty:
        logging.info("No data to push to Airtable"); return
    api   = Api(api_key=AIRTABLE_ACCESS_TOKEN)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    for _,r in df.iterrows():
        code = r["Club"]
        recs = table.all(formula=f"{{New Code}}='{code}'")
        for rec in recs:
            table.update(rec["id"], {
                "PEPSI": r["PEPSI"]=="Y",
                "SUPP RESTOCK":r["Supps"]=="Y",
                "N2G Water":  r["N2G"]=="Y",
                "DNA Order":  r["DNA"]=="Y",
                "MF/FAIRE Order":r["MF"]=="Y",
            })

# ────────────────────────────────────────────────────────────────────────────────
def main():
    token = authenticate_graph()
    rpt_files = download_reports(token)
    if not rpt_files:
        logging.info("No reports found; exiting."); return
    master = load_master_data(MASTER_SHEET_PATH)
    if master is None or master.empty:
        logging.error("Master data empty; exiting."); return
    summary = process_reports(rpt_files, master)
    if summary.empty:
        logging.info("No club data matched; exiting."); return
    save_order_database(summary)
    update_airtable(summary)
    logging.info("All done!")

if __name__ == "__main__":
    main()
