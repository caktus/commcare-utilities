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
    "Based on project slug, look up the value for owner_id"
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
        "REQUIRED_ONE_OFS": os.environ.get("REQUIRED_ONE_OFS"),
    }

    missing = [k for k in required_vars if not required_vars[k]]

    if missing:
        print(f"The following env var(s) need(s) to be set: {', '.join(missing)}")
        sys.exit()

    required_one_ofs = required_vars["REQUIRED_ONE_OFS"].split(",")

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

    drop_columns_after = os.environ.get("DROP_COLUMNS_AFTER")
    if drop_columns_after:
        logger.info(
            f"Altering original workbook, dropping columns after colum "
            f"{drop_columns_after}"
        )
        # open the Excel workbook and delete the columns then save it
        path = pathlib.Path(file_path).expanduser()
        wb = load_workbook(path)
        ws = wb[WB_CONTACT_SHEET_NAME]
        # we drop from after the drop_columns_after index to fifty columns across
        # just to give more than comfortable buffer
        ws.delete_cols(column_index_from_string(drop_columns_after) + 1, 50)
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
        required_one_ofs=required_one_ofs,
        **dict(owner_id=owner_id),
    )
