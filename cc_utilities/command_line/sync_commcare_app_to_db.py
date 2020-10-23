import argparse
import subprocess
import sys
import tempfile
import traceback
from datetime import datetime
from pathlib import PurePath

from openpyxl import Workbook

from cc_utilities.command_line.generate_case_export_query_file import make_sql_friendly
from cc_utilities.common import get_application_structure
from cc_utilities.constants import COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS
from cc_utilities.logger import logger

PARENT_PROPERTY_PREFIX = "parent/"


def make_commcare_export_sync_xl_wb(mappings):
    """Create an Excel workbook in format required for commcare-export script

    NB: This does not save the workbook, and will need to call wb.save() on object
    returned by this function in order to persist.

    Args:
        source_target_mappings (dict): dict of case types, and for each case type the
            value is a dict of source field to target db column mappings
    Returns:
        obj: An Openpyxl workbook
    """
    sheet_headers = [
        "Data Source",
        "Filter Name",
        "Filter Value",
        "",
        "Field",
        "Source Field",
        "Alternate Source Field 1",
    ]
    wb = Workbook()
    for idx, case_type in enumerate(mappings):
        if idx == 0:
            ws = wb.active
        else:
            ws = wb.create_sheet()
        ws.title = case_type
        ws.append(sheet_headers)
        ws["A2"] = "case"
        ws["B2"] = "type"
        ws["C2"] = case_type

        row_offset = 2

        for idx, item in enumerate(
            sorted(
                [(k, mappings[case_type][k]) for k in mappings[case_type]],
                key=lambda x: x[0],
            )
        ):
            row_num = idx + row_offset
            ws[f"F{row_num}"], ws[f"E{row_num}"] = item

    return wb


def do_commcare_export_to_db(
    database_url_string,
    commcare_project_name,
    wb_file_path,
    commcare_user_name,
    commcare_api_key,
):
    """Run `commcare-export` as subprocess to export data to SQL db

    Args:
        database_url_string (str): Full db url to export to
        commcare_project_name (str): The Commcare project being exported from
        wb_file_path (str): Where the workbook with source-column mappings lives
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
    """
    commands = (
        f"commcare-export --output-format sql "
        f"--output {database_url_string} --project {commcare_project_name} "
        f"--query {wb_file_path} --username {commcare_user_name} "
        f"--auth-mode apikey --password {commcare_api_key} --batch-size 5000"
    ).split(" ")
    subprocess.run(commands)


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    commcare_app_id,
    db_url,
    case_types,
    mapping_workbook_path,
):
    """The main routine.

    Args:
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be imported
        commcare_app_id (str): The ID of the Commcare app.
        db_url (str): Connection string for the db
        case_types (list): Optional. List of case types. If provided, only the provided
            case types will be synced.
        mapping_workbook_path (str):  Optional. If provided, the Excel workbook
            containing source-target mappings will be saved in this folder.
    """
    logger.info(
        f"Retrieving application structure for {commcare_project_name} with ID: "
        f"{commcare_app_id}. This may take a while."
    )
    app_structure = get_application_structure(
        commcare_project_name, commcare_user_name, commcare_api_key, commcare_app_id
    )
    cases_with_properties = {}

    for module in app_structure["modules"]:
        # oddly, there are module types that appear that don't have a case type
        if not module["case_type"]:
            continue
        if not cases_with_properties.get(module["case_type"]):
            cases_with_properties[module["case_type"]] = set(
                [
                    prop
                    for prop in module["case_properties"]
                    if not prop.startswith(PARENT_PROPERTY_PREFIX)
                ]
            )
        else:
            cases_with_properties[module["case_type"]] = cases_with_properties[
                module["case_type"]
            ].union(
                set(
                    [
                        prop
                        for prop in module["case_properties"]
                        if not prop.startswith(PARENT_PROPERTY_PREFIX)
                    ]
                )
            )
    unfound = list(
        set(case_types).difference(set([k for k in cases_with_properties.keys()]))
    )

    if case_types and len(unfound) == len(case_types):
        logger.warn("None of the case types you requested were found")
        return
    if unfound:
        logger.warn(f"Some case types were not found: {', '.join(unfound)}")
        logger.info("Will continuing process the other requested case types")

    if case_types:
        cases_with_properties = {
            k: v for (k, v) in cases_with_properties.items() if k in case_types
        }

    def _generate_standard_app_state_file_name(
        date_part=datetime.now().strftime("%Y_%m_%d-%H_%M_%S"),
    ):
        return f"app-{commcare_app_id}-by-case-by-property_{date_part}.json"

    mappings = {}
    for case_type in cases_with_properties:
        mappings[case_type] = {
            **COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS,
            **{
                f"properties.{item}": make_sql_friendly(item)
                for item in sorted(cases_with_properties[case_type])
            },
        }
    logger.info("Generating db sync mapping workbook")
    wb = make_commcare_export_sync_xl_wb(mappings)
    if mapping_workbook_path:
        wb.save(
            PurePath(mapping_workbook_path).joinpath(
                f"mappings-{datetime.now().strftime('%m_%d_%Y_%H-%M')}.xlsx"
            )
        )
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_file_path = PurePath(tmpdir).joinpath("mapping.xlsx")
        wb.save(tmp_file_path)
        logger.info("Attempting to sync to db")
        do_commcare_export_to_db(
            db_url,
            commcare_project_name,
            tmp_file_path,
            commcare_user_name,
            commcare_api_key,
        )
    logger.info("I am quite done now.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--username",
        help="The Commcare username (email address)",
        dest="commcare_user_name",
    )
    parser.add_argument("--apikey", help="A Commcare API key", dest="commcare_api_key")
    parser.add_argument(
        "--project", help="The Commcare project name", dest="commcare_project_name"
    )
    parser.add_argument(
        "--app-id", help="The ID of the CommCare app", dest="application_id"
    )
    parser.add_argument(
        "--db-url", help="The URL string of the db to sync to", dest="db_url"
    )
    parser.add_argument(
        "--case-types",
        help="Optional. Comma-separated list of case types to sync",
        dest="case_types",
        nargs="*",
    )
    parser.add_argument(
        "--mapping-path",
        help="Optional. Path to folder to store source-target mapping workbook to",
        dest="mapping_workbook_path",
    )
    args = parser.parse_args()
    try:
        main_with_args(
            args.commcare_user_name,
            args.commcare_api_key,
            args.commcare_project_name,
            args.application_id,
            args.db_url,
            args.case_types,
            args.mapping_workbook_path,
        )
    except Exception as exc:
        logger.error(exc)
        logger.error(traceback.print_exc())
        sys.exit(1)
    sys.exit(0)
