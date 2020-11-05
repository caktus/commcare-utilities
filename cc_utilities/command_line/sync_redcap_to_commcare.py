import argparse
import os
from datetime import datetime

import redcap
import yaml

from cc_utilities.common import upload_data_to_commcare
from cc_utilities.logger import logger
from cc_utilities.redcap_sync import collapse_checkbox_columns, split_cases_and_contacts


def get_redcap_state(state_file):
    "Read state required for REDCap sync."
    if not os.path.exists(state_file):
        return {
            "date_begin": None,
            "in_progress": False,
        }
    with open(state_file) as f:
        return yaml.safe_load(f)


def save_redcap_state(state, state_file):
    "Save state required for REDCap sync."
    with open(state_file, "w") as f:
        yaml.dump(state, f)


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    redcap_api_url,
    redcap_api_key,
    external_id_col,
    state_file,
    sync_all,
):
    """
    Script to download case and contact records for the given `redcap_api_url` and
    `redcap_api_key` and upload them to the provided `commcare_project_name` via
    CommCare's bulk upload API.

    Args:
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be imported
        redcap_api_url (str): The URL to the REDCap API server
        redcap_api_key (str): The REDCap API key
        external_id_col (str): The name of the column in REDCap that contains the external_id for CommCare
        state_file (str): File path to a local file where state about this sync can be kept
        sync_all (bool): If set, ignore the date_begin in the state_file and sync all records
    """

    # Try to avoid starting a second process, if one is already going
    # (this approach is not free of race conditions, but should catch
    # the majority of accidental duplicate runs).
    state = get_redcap_state(state_file)
    if state["in_progress"]:
        raise ValueError("There may be another process running. Exiting.")
    state["in_progress"] = True
    save_redcap_state(state, state_file)

    try:
        # Save next_date_begin before retrieving records so we don't miss any
        # on the next run (this might mean some records are synced twice, but
        # that's better than never at all).
        next_date_begin = datetime.now()

        logger.info("Retrieving and cleaning data from REDCap...")
        redcap_project = redcap.Project(redcap_api_url, redcap_api_key)
        redcap_records = redcap_project.export_records(
            # date_begin corresponds to the dateRangeBegin field in the REDCap
            # API, which "return[s] only records that have been created or modified
            # *after* a given date/time." Note that REDCap expects this to be in
            # server time, so the script and server should be run in the same time
            # zone (or this script modified to accept a timezone argument).
            date_begin=state["date_begin"] if not sync_all else None,
            # Tell PyCap to return a pandas DataFrame.
            format="df",
            df_kwargs={
                # Without index_col=False, read_csv() will use the first column
                # ("record_id") as the index, which is problematic because it's
                # not unique and is easier to handle as a separate column anyways.
                "index_col": False,
                # We import everything as a string, to avoid pandas coercing ints
                # to floats and adding unnecessary decimal points in the data when
                # uploaded to CommCare.
                "dtype": str,
            },
        )
        if len(redcap_records.index) == 0:
            logger.info("No records returned from REDCap; aborting sync.")
        else:
            cases_df, contacts_df = redcap_records.pipe(collapse_checkbox_columns).pipe(
                split_cases_and_contacts, external_id_col
            )
            logger.info(
                f"Uploading {len(cases_df.index)} found patients (cases) to CommCare..."
            )
            upload_data_to_commcare(
                cases_df,
                commcare_project_name,
                "patient",
                "external_id",
                commcare_user_name,
                commcare_api_key,
                search_field="external_id",
            )
            logger.info(
                f"Uploading {len(contacts_df.index)} found contacts to CommCare..."
            )
            upload_data_to_commcare(
                contacts_df,
                commcare_project_name,
                "contact",
                "external_id",
                commcare_user_name,
                commcare_api_key,
                search_field="external_id",
            )
        state["date_begin"] = next_date_begin
    finally:
        # Whatever happens, don't keep our lock open.
        state["in_progress"] = False
        save_redcap_state(state, state_file)
    logger.info("Sync done.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--username",
        help="The Commcare username (email address)",
        dest="commcare_user_name",
        required=True,
    )
    parser.add_argument(
        "--apikey", help="A Commcare API key", dest="commcare_api_key", required=True,
    )
    parser.add_argument(
        "--project",
        help="The Commcare project name",
        dest="commcare_project_name",
        required=True,
    )
    parser.add_argument(
        "--redcap-api-url", help="The REDCap API URL", required=True,
    )
    parser.add_argument(
        "--redcap-api-key", help="A REDCap API key", required=True,
    )
    parser.add_argument(
        "--external-id-col",
        help="Name of column in REDCap that should be used as the external_id in CommCare",
        required=True,
    )
    parser.add_argument(
        "--state-file",
        help="The path where state should be read and saved",
        required=True,
    )
    parser.add_argument(
        "--sync-all",
        help="If set, ignore the begin date in the state file and sync all records",
        action="store_true",
    )
    args = parser.parse_args()
    main_with_args(
        args.commcare_user_name,
        args.commcare_api_key,
        args.commcare_project_name,
        args.redcap_api_url,
        args.redcap_api_key,
        args.external_id_col,
        args.state_file,
        args.sync_all,
    )
