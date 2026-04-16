"""
Airtable Close Rate Summary Script
-----------------------------------
Reads all "Showed" calls from the Calls table, groups them by
Rep / Week / Company / Package, then upserts aggregated rows into
the Close Rate Summary table using the Row Key field as the unique key.

Required environment variables (or a .env file):
    AIRTABLE_API_KEY   – your personal access token
    AIRTABLE_BASE_ID   – e.g. appXXXXXXXXXXXXXX
    CALLS_TABLE_ID     – table ID (or name) for the Calls table
    SUMMARY_TABLE_ID   – table ID (or name) for the Close Rate Summary table
"""

import os
import sys
import time
import logging
from collections import defaultdict

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY          = os.environ["AIRTABLE_API_KEY"]
BASE_ID          = os.environ["AIRTABLE_BASE_ID"]
CALLS_TABLE      = os.environ["CALLS_TABLE_ID"]
SUMMARY_TABLE    = os.environ["SUMMARY_TABLE_ID"]

BASE_URL = f"https://api.airtable.com/v0/{BASE_ID}"
HEADERS  = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type":  "application/json",
}

# Airtable allows max 10 records per create/update request
BATCH_SIZE = 10


# ── Airtable helpers ──────────────────────────────────────────────────────────

def fetch_all_records(table: str, filter_formula: str = None) -> list[dict]:
    """Page through all records in a table, optionally applying a formula filter."""
    url     = f"{BASE_URL}/{table}"
    records = []
    params  = {}
    if filter_formula:
        params["filterByFormula"] = filter_formula

    while True:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        records.extend(body.get("records", []))
        offset = body.get("offset")
        if not offset:
            break
        params["offset"] = offset
        time.sleep(0.2)   # be polite to the API

    log.info("Fetched %d records from '%s'", len(records), table)
    return records


def batch_create(table: str, records: list[dict]) -> None:
    """Create records in batches of BATCH_SIZE."""
    url = f"{BASE_URL}/{table}"
    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]
        resp  = requests.post(url, headers=HEADERS, json={"records": chunk}, timeout=30)
        resp.raise_for_status()
        log.info("Created %d records (batch %d)", len(chunk), i // BATCH_SIZE + 1)
        time.sleep(0.2)


def batch_update(table: str, records: list[dict]) -> None:
    """PATCH records in batches of BATCH_SIZE."""
    url = f"{BASE_URL}/{table}"
    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]
        resp  = requests.patch(url, headers=HEADERS, json={"records": chunk}, timeout=30)
        resp.raise_for_status()
        log.info("Updated %d records (batch %d)", len(chunk), i // BATCH_SIZE + 1)
        time.sleep(0.2)


# ── Core logic ────────────────────────────────────────────────────────────────

def build_row_key(rep: str, week: str, company: str, package: str) -> str:
    """Deterministic composite key used to identify a unique summary row."""
    return f"{rep}|{week}|{company}|{package}"


def aggregate_calls(call_records: list[dict]) -> dict[str, dict]:
    """
    Group Showed calls by (Rep, Week, Company, Package) and count:
      - showed_calls : total records in this group
      - closes       : records where Closed == True
    Returns a dict keyed by row_key.
    """
    groups: dict[str, dict] = defaultdict(lambda: {"showed_calls": 0, "closes": 0})

    for rec in call_records:
        fields  = rec.get("fields", {})
        rep     = str(fields.get("Sales Rep",      "") or "").strip()
        week    = str(fields.get("Week",           "") or "").strip()
        company = str(fields.get("Company",        "") or "").strip()
        package = str(fields.get("Package Closed", "") or "").strip()
        closed  = fields.get("Closed", False)

        key = build_row_key(rep, week, company, package)
        groups[key]["rep"]     = rep
        groups[key]["week"]    = week
        groups[key]["company"] = company
        groups[key]["package"] = package
        groups[key]["showed_calls"] += 1
        if closed is True or str(closed).lower() in ("true", "1", "yes"):
            groups[key]["closes"] += 1

    log.info("Aggregated into %d groups", len(groups))
    return dict(groups)


def upsert_summary(aggregated: dict[str, dict]) -> None:
    """
    Load existing Close Rate Summary rows, then create or update as needed.
    """
    # ── Load existing summary rows ────────────────────────────────────────────
    existing_records = fetch_all_records(SUMMARY_TABLE)

    # Map  row_key -> airtable record id
    existing_map: dict[str, str] = {}
    for rec in existing_records:
        key = rec.get("fields", {}).get("Row Key", "")
        if key:
            existing_map[key] = rec["id"]

    log.info("Found %d existing summary rows", len(existing_map))

    to_create: list[dict] = []
    to_update: list[dict] = []

    for row_key, data in aggregated.items():
        fields_payload = {
            "Rep":          data["rep"],
            "Week":         data["week"],
            "Company":      data["company"],
            "Package":      data["package"],
            "Showed Calls": data["showed_calls"],
            "Closes":       data["closes"],
            "Row Key":      row_key,
        }

        if row_key in existing_map:
            to_update.append({"id": existing_map[row_key], "fields": fields_payload})
        else:
            to_create.append({"fields": fields_payload})

    log.info("Records to create: %d | to update: %d", len(to_create), len(to_update))

    if to_create:
        batch_create(SUMMARY_TABLE, to_create)
    if to_update:
        batch_update(SUMMARY_TABLE, to_update)

    log.info("Upsert complete.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== Airtable Close Rate Summary ===")

    # Only pull records where Call Status is "Showed"
    filter_formula = "({Call Status} = 'Showed')"
    call_records   = fetch_all_records(CALLS_TABLE, filter_formula)

    if not call_records:
        log.warning("No 'Showed' records found – nothing to aggregate.")
        sys.exit(0)

    aggregated = aggregate_calls(call_records)
    upsert_summary(aggregated)

    log.info("Done ✓")


if __name__ == "__main__":
    main()
