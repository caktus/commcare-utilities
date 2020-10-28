import argparse
import subprocess
import sys
import tempfile
import traceback
from datetime import datetime
from pathlib import PurePath

from openpyxl import load_workbook

from cc_utilities.common import (
    get_application_structure,
    make_commcare_export_sync_xl_wb,
    make_sql_friendly,
)
from cc_utilities.constants import (
    APPLICATION_STRUCTURE_DEFAULT_TIMEOUT,
    COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS,
)
from cc_utilities.logger import logger

PARENT_PROPERTY_PREFIX = "parent/"


def do_commcare_export_to_db(
    database_url_string,
    commcare_project_name,
    wb_file_path,
    commcare_user_name,
    commcare_api_key,
    commcare_export_script_options,
    commcare_export_script_flags,
):
    """Run `commcare-export` as subprocess to export data to SQL db

    Args:
        database_url_string (str): Full db url to export to
        commcare_project_name (str): The Commcare project being exported from
        wb_file_path (str): Where the workbook with source-column mappings lives
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_export_script_options (dict): A dict of additional args to get passed
            to the `commcare-export` subprocess as command line options.
        commcare_export_script_flags (list): A list of command line flags (with no args)
            to pass to `commcare-export` subprocess.
    """
    commcare_export_script_options = (
        commcare_export_script_options if commcare_export_script_options else {}
    )
    commcare_export_script_flags = (
        commcare_export_script_flags if commcare_export_script_flags else []
    )
    commands = (
        f"commcare-export --output-format sql "
        f"--output {database_url_string} --project {commcare_project_name} "
        f"--query {wb_file_path} --username {commcare_user_name} "
        f"--auth-mode apikey --password {commcare_api_key}"
    )

    additional_options = " ".join(
        [f"--{k} {v}" for (k, v) in commcare_export_script_options.items()]
    )
    additional_flags = " ".join([f"--{flag}" for flag in commcare_export_script_flags])
    commands = " ".join([commands, additional_options, additional_flags]).strip()
    commands = commands.split(" ")
    subprocess.run(commands)


def get_mappings_from_app_structure(
    commcare_project_name,
    commcare_user_name,
    commcare_api_key,
    commcare_app_id,
    app_structure_api_timeout,
):
    """Get data about each case type and its known properties (historical and current)
        from the Application Structure API.

    Args:

        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be
            imported
        commcare_app_id (str): The ID of the Commcare app.
        app_structure_api_timeout (int): Optional. If provided will override default
            timeout for the call to Application Structure API (
            which tends to take a while)
    Returns:
        dict: Whose keys are case types and whose values are lists of property names
    """
    logger.info(
        f"Retrieving application structure for {commcare_project_name} with ID: "
        f"{commcare_app_id} from API. This may take a while."
    )
    app_structure = get_application_structure(
        commcare_project_name,
        commcare_user_name,
        commcare_api_key,
        commcare_app_id,
        app_structure_api_timeout,
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
    return cases_with_properties


def load_app_mappings_from_workbook(mapping_path):
    """Mimic the values returned by `get_mappings_from_app_structure` by loading
        mapping data stored in a sourc-target mapping workbook.

    Args:
        mapping_path (str): Path to an Excel wb containing source target mappings,
            likely originally created by in the workbook creation portion of
            `main_with_args` below.
    Returns:
        dict: Whose keys are case types and whose values are lists of property names
    """
    wb = load_workbook(filename=mapping_path)
    cases_with_source_fields = {}
    for sheet in wb:
        source_fields = list(
            map(lambda cell: cell.value.replace("properties.", ""), sheet["F:F"])
        )[1:]
        source_fields = set(source_fields).difference(
            set([item[0] for item in COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS])
        )
        cases_with_source_fields[sheet.title] = source_fields
    return cases_with_source_fields


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    commcare_app_id,
    db_url,
    case_types,
    existing_mapping_path,
    mapping_storage_path,
    app_structure_api_timeout,
    commcare_export_script_options=None,
    commcare_export_script_flags=None,
):
    """The main routine.

    Args:
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be
            imported
        commcare_app_id (str): The ID of the Commcare app.
        db_url (str): Connection string for the db
        case_types (list): Optional. List of case types. If provided, only the provided
            case types will be synced.
        existing_mapping_path (str): Path to an existing Excel wb containing
            source-target mappings. If provided, this asset will be used, and the
            Application Structure API will not be called to get this data.
        mapping_storage_path (str): If provided, the Excel workbook
            containing source-target mappings will be saved in this folder.
        app_structure_api_timeout (int):If provided will override default
            timeout for the call to Application Structure API (
            which tends to take a while)
        commcare_export_script_options (dict): A dict of additional args to get passed
            to the `commcare-export` subprocess as command line options.
        commcare_export_script_flags (list): A list of command line flags (with no args)
            to pass to `commcare-export` subprocess.
    """
    cases_with_properties = (
        load_app_mappings_from_workbook(existing_mapping_path)
        if existing_mapping_path
        # NB: This API call can take a long time: ~2-3 minutes
        else get_mappings_from_app_structure(
            commcare_project_name,
            commcare_user_name,
            commcare_api_key,
            commcare_app_id,
            app_structure_api_timeout,
        )
    )
    unfound_requested_case_types = list(
        set(case_types).difference(set([k for k in cases_with_properties.keys()]))
    )
    if case_types and len(unfound_requested_case_types) == len(case_types):
        logger.warn("None of the case types you requested were found")
        return
    if unfound_requested_case_types:
        logger.warn(
            f"Some case types were not found: {', '.join(unfound_requested_case_types)}"
        )
        logger.info("Will continuing process the other requested case types")
    if case_types:
        cases_with_properties = {
            k: v for (k, v) in cases_with_properties.items() if k in case_types
        }

    mappings = {}
    for case_type in cases_with_properties:
        mappings[make_sql_friendly(case_type)] = list(
            set(
                [
                    *COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS,
                    *[
                        (f"properties.{item}", make_sql_friendly(item))
                        for item in sorted(cases_with_properties[case_type])
                    ],
                ]
            )
        )
    logger.info("Generating db sync mapping workbook")
    wb = make_commcare_export_sync_xl_wb(mappings)
    if mapping_storage_path:
        wb.save(
            PurePath(mapping_storage_path).joinpath(
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
            commcare_export_script_options,
            commcare_export_script_flags,
        )
    logger.info("I am quite done now.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--username",
        help="The Commcare username (email address)",
        dest="commcare_user_name",
    )
    parser.add_argument("--api_key", help="A Commcare API key", dest="commcare_api_key")
    parser.add_argument(
        "--project", help="The Commcare project name", dest="commcare_project_name"
    )
    parser.add_argument(
        "--app-id", help="The ID of the CommCare app", dest="application_id"
    )
    parser.add_argument(
        "--db-url", help="The URL string of the db to sync to",
    )
    parser.add_argument(
        "--case-types",
        help="Optional. Comma-separated list of case types to sync",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "--existing-mapping-path",
        help=(
            "Optional. Path to xl wb containing existing source-target mapping. "
            "If included, the script will not make a call to the Application Structure "
            "API and will instead use the mappings contained in this file"
        ),
    )
    parser.add_argument(
        "--mapping-storage-path",
        help="Optional. Path to folder to store source-target mapping workbook to",
    )
    parser.add_argument(
        "--app-structure-api-timeout",
        help="Optional. Seconds for timeout for request to application structure API",
        type=int,
        default=APPLICATION_STRUCTURE_DEFAULT_TIMEOUT,
    )
    parser.add_argument(
        "--since", help="Optional. Export all data after this date. Format YYYY-MM-DD",
    )
    parser.add_argument(
        "--until", help="Optional. Export all up until this date. Format YYYY-MM-DD",
    )
    parser.add_argument(
        "--batch-size",
        help=(
            "Optional. Integer. If included, records will be streamed to the SQL "
            "db in batches of this size"
        ),
        default=5000,
    )
    parser.add_argument(
        "--verbose",
        help="If flag included, logs of the db sync will be verbose.",
        action="store_true",
    )
    parser.add_argument(
        "--users",
        help="If flag included, export table with data about project's mobile workers",
        action="store_true",
    )
    parser.add_argument(
        "--locations",
        help="If flag included, export table with data about project's locations",
        action="store_true",
    )
    parser.add_argument(
        "--with-organization",
        help=(
            "If flag included, export tables containing mobile worker data and "
            "location data and add a commcare_userid field to any exported form or "
            "case"
        ),
        action="store_true",
    )
    args = parser.parse_args()

    try:
        commcare_export_script_options = {
            "since": args.since,
            "until": args.until,
        }
        commcare_export_script_options = {
            k: v for (k, v) in commcare_export_script_options.items() if v
        }
        flags = []
        for arg in ["verbose", "users", "locations", "with_organization"]:
            if args.__dict__[arg]:
                flags.append(arg.replace("_", "-"))
        main_with_args(
            args.commcare_user_name,
            args.commcare_api_key,
            args.commcare_project_name,
            args.application_id,
            args.db_url,
            args.case_types,
            args.existing_mapping_path,
            args.mapping_storage_path,
            args.app_structure_api_timeout,
            commcare_export_script_options=commcare_export_script_options,
            commcare_export_script_flags=flags,
        )
    except Exception as exc:
        logger.error(exc)
        logger.error(traceback.print_exc())
        sys.exit(1)
    sys.exit(0)
