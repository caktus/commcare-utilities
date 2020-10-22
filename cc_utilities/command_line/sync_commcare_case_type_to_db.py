import argparse
import json
import sys
import tempfile
import traceback
from datetime import datetime
from pathlib import PurePath

from cc_utilities.command_line.generate_case_export_query_file import make_sql_friendly
from cc_utilities.common import get_application_structure
from cc_utilities.constants import COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS
from cc_utilities.logger import logger
from cc_utilities.sync_case_type_to_db import (
    do_commcare_export_to_db,
    make_commcare_export_sync_xl_wb,
)

PARENT_PROPERTY_PREFIX = "parent/"


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    commcare_app_id,
    case_type,
    db_url,
    state_dir=None,
    previous_state_file_path=None,
):
    """The main routine.

    Args:
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be imported
        commcare_app_id (str): The ID of the Commcare app.
        case_type (str): The case type to be synced (e.g., contact or patient, etc.)
        db_url (str): Connection string for the db
        state_dir (str): Optional. A directory to store JSON file with info about the
            properties found for the app.
        previous_state_file_path (str): Optional. A path to a previously generated
            state JSON file.
    """
    logger.info(
        f"Retrieving application structure for {commcare_project_name} with ID: "
        f"{commcare_app_id}. This may take a while."
    )
    app_structure = get_application_structure(
        commcare_project_name, commcare_user_name, commcare_api_key, commcare_app_id
    )
    currently_seen_case_properties = set()
    for module in app_structure["modules"]:
        if module["case_type"] == case_type:
            currently_seen_case_properties = currently_seen_case_properties.union(
                [
                    prop
                    for prop in module["case_properties"]
                    if not prop.startswith(PARENT_PROPERTY_PREFIX)
                ]
            )

    def _generate_standard_app_state_file_name(
        date_part=datetime.now().strftime("%Y_%m_%d-%H_%M_%S"),
    ):
        return f"app-{commcare_app_id}-by-case-by-property_{date_part}.json"

    previously_seen_app_state = {}
    if previous_state_file_path:
        with open(PurePath(previous_state_file_path), "r") as fl:
            previously_seen_app_state = json.load(fl)
    previously_seen_properties = set(previously_seen_app_state.get(case_type, []))
    new_properties = currently_seen_case_properties.difference(
        previously_seen_properties
    )
    if new_properties:
        logger.info(
            f"Encountered {len(new_properties)} new properties for the "
            f"{case_type} case type: {', '.join(new_properties)}"
        )
    current_app_state = {
        **previously_seen_app_state,
        **{case_type: sorted(list(new_properties.union(previously_seen_properties)))},
    }
    if new_properties and state_dir:
        latest_file_path = PurePath(state_dir).joinpath(
            _generate_standard_app_state_file_name(date_part="latest")
        )
        dated_file_path = PurePath(state_dir).joinpath(
            _generate_standard_app_state_file_name()
        )
        logger.info(
            f"Saving case property for app at {latest_file_path} and {dated_file_path}"
        )
        with open(latest_file_path, "w") as latest, open(dated_file_path, "w") as dated:
            json.dump(current_app_state, latest, indent=4)
            json.dump(current_app_state, dated, indent=4)
    mapping = {
        **COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS,
        **{
            f"properties.{item}": make_sql_friendly(item)
            for item in current_app_state[case_type]
        },
    }
    logger.info("Generating db sync mapping workbook")
    wb = make_commcare_export_sync_xl_wb(mapping, case_type)

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
        "--case-type",
        help=("The type of case to be synced (e.g., patient or contact, etc."),
        dest="case_type",
    )
    parser.add_argument(
        "--state-dir",
        help="The path where to output reports on what happened to",
        dest="state_dir",
    )
    parser.add_argument(
        "--previous-state-file",
        help="The path to a previous JSON file containing application state data",
        dest="previous_state_file_path",
    )
    args = parser.parse_args()
    try:
        main_with_args(
            args.commcare_user_name,
            args.commcare_api_key,
            args.commcare_project_name,
            args.application_id,
            args.case_type,
            args.db_url,
            args.state_dir,
            args.previous_state_file_path,
        )
    except Exception as exc:
        logger.error(exc)
        logger.error(traceback.print_exc())
        sys.exit(1)
    sys.exit(0)
