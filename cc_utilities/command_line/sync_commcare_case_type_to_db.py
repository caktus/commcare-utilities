import argparse
import json
import sys
from datetime import datetime
from pathlib import Path, PurePath

from cc_utilities.logger import logger


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    commcare_app_id,
    case_type,
    db_url,
    reporting_path,
    **additional_mappings,
):
    """The main routine.

    Args:
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be imported
                commcare_app_id (str): The ID of the Commcare app.
        case_type (str): The case type to be synced (e.g., contact or patient, etc.)
        db_url (str): Connection string for the db
        additional_mappings (dict): Additional source-target mappings to use when
                        syncing to the db — specifically intended for mappings that user wants
                        synced to db but that won't be returned by the Application Structure API,
                        which this routine makes use of behind the scenes.
    """
    logger.info()
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
        "-case-type",
        help=("The type of case to be synced (e.g., patient or contact, etc."),
        dest="case_type"
    )
    parser.add_argument(
        "--reporting-path",
        help="The path where to output reports on what happened to",
        dest="reporting_path",
    )
    parser.add_argument(
        "--additional_mappings",
        help=(
            "Additional source/target mappings to include when syncing from CommCare "
            "to the SQL db. Suppplied as a JSON string. "
            "For instance `--additional_mappings \"{'source_name': 'target_name'}\""
        ),
        dest="additional_mappings",
        type=json.loads,
    )
    args = parser.parse_args()
    try:
        main_with_args(
            args.commcare_user_name,
            args.commcare_api_key,
            args.commcare_project_name,
            args.application_id,
            args.case_type,
            arg.db_url,
            args.reporting_path,
            **args.additional_mappings,
        )
    except Exception:
        sys.exit(1)
    sys.exit(0)
