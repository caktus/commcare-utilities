import csv
import json
import os
import pathlib
import sys

from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string

from cc_utilities.command_line.bulk_upload_legacy_contact_data import main_with_args
from cc_utilities.common import logger

WB_CONTACT_SHEET_NAME = "contacts"


def lookup_owner_id_for_project(project_slug, agency_owner_lookup_path):
    with open(agency_owner_lookup_path) as fl:
        reader = csv.DictReader(fl)
        row = next(row for row in reader if row["project_slug"] == project_slug)
    return row["owner_id"]


def main():
    required_vars = {
        "COMMCARE_USER_NAME": os.environ.get("COMMCARE_USER_NAME"),
        "COMMCARE_API_KEY": os.environ.get("COMMCARE_API_KEY"),
        "COMMCARE_CONTACT_DATA_DICT_CSV": os.environ.get(
            "COMMCARE_CONTACT_DATA_DICT_CSV"
        ),
        "PROJECT_AGENCY_OWNER_LOOKUP_PATH": os.environ.get(
            "PROJECT_AGENCY_OWNER_LOOKUP_PATH"
        ),
    }

    missing = [k for k in required_vars if not required_vars[k]]

    if missing:
        print(f"The following env var(s) need(s) to be set: {', '.join(missing)}")
        sys.exit()
    while True:
        file_path = input("Enter the path to the contact data Excel file to upload:  ")
        if file_path:
            break
    while True:
        reporting_path = input(
            "Enter the path the folder to store reporting data in:  "
        )
        if reporting_path:
            break
    while True:
        project_slug = input("Enter the project slug: ")
        if project_slug:
            break

    owner_id = lookup_owner_id_for_project(
        project_slug, required_vars["PROJECT_AGENCY_OWNER_LOOKUP_PATH"]
    )

    drop_columns = os.environ.get("DROP_COLUMNS")
    if drop_columns:
        drop_columns = drop_columns.split(",")
        logger.info(
            f"Altering original workbook, dropping columns: {', '.join(drop_columns)}"
        )
        path = pathlib.Path(file_path).expanduser()
        wb = load_workbook(path)
        ws = wb[WB_CONTACT_SHEET_NAME]
        for col in drop_columns:
            ws.delete_cols(column_index_from_string(col), 1)
        wb.save(path)

    rename_columns_json_path = os.environ.get("RENAME_COLUMNS_JSON_PATH")
    rename_columns = None

    if rename_columns_json_path:
        with open(rename_columns_json_path) as f:
            rename_columns = json.load(f)
    main_with_args(
        required_vars["COMMCARE_USER_NAME"],
        required_vars["COMMCARE_API_KEY"],
        project_slug,
        file_path,
        required_vars["COMMCARE_CONTACT_DATA_DICT_CSV"],
        reporting_path,
        rename_columns=rename_columns,
        **dict(owner_id=owner_id),
    )
