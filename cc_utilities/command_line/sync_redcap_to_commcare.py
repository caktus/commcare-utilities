import argparse
import os
from datetime import datetime

import redcap
import yaml

from cc_utilities.common import upload_data_to_commcare
from cc_utilities.logger import logger
from cc_utilities.redcap_sync import collapse_checkbox_columns, split_cases_and_contacts


def get_state(state_file):
    "Read state required for REDCap sync."
    if not os.path.exists(state_file):
        return {
            "date_begin": None,
        }
    with open(state_file) as f:
        return yaml.safe_load(f)


def save_state(state, state_file):
    "Save state required for REDCap sync."
    with open(state_file, "w") as f:
        yaml.dump(state, f)


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    redcap_api_url,
    redcap_api_key,
    state_file,
    data_dictionary_path,
    sync_all,
):
    """TBD


    Args:
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be imported
        redcap_api_url (str): The URL to the REDCap API server
        redcap_api_key (str): The REDCap API key
        state_file (str): File path to a local file where state about this sync can be kept
        data_dictionary_path (str): The path to the Commcare data dictionary (optional)
    """

    state = get_state(state_file)
    redcap_project = redcap.Project(redcap_api_url, redcap_api_key)
    next_date_begin = datetime.now()

    logger.info("Retrieving and cleaning data from REDCap...")
    cases_df, contacts_df = (
        redcap_project.export_records(
            format="df",
            date_begin=state["date_begin"] if not sync_all else None,
            # Without index_col=False, read_csv() will use the first column
            # ("record_id") as the index, which is problematic because it's
            # not unique and is easier to handle as a separate column anyways.
            df_kwargs={"index_col": False},
        )
        .pipe(collapse_checkbox_columns)
        .pipe(split_cases_and_contacts)
    )
    logger.info("Uploading found patients (cases) to CommCare...")
    upload_data_to_commcare(
        cases_df,
        commcare_project_name,
        "patient",
        "external_id",
        commcare_user_name,
        commcare_api_key,
        search_field="external_id",
    )
    logger.info("Uploading found contacts to CommCare...")
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
    save_state(state, state_file)
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
        "--redcap-api-url",
        help="The REDCap API URL",
        dest="redcap_api_url",
        required=True,
    )
    parser.add_argument(
        "--redcap-api-key",
        help="A REDCap API key",
        dest="redcap_api_key",
        required=True,
    )
    parser.add_argument(
        "--state-file",
        help="The path where state should be read and saved",
        dest="state_file",
        required=True,
    )
    parser.add_argument(
        "--data-dict-path",
        help="The path where the data dictionary CSV is located, for validation purposes (optional)",
        dest="data_dictionary_path",
    )
    parser.add_argument(
        "--sync-all",
        help="The path where state should be read and saved",
        dest="sync_all",
        action="store_true",
    )
    args = parser.parse_args()
    main_with_args(
        args.commcare_user_name,
        args.commcare_api_key,
        args.commcare_project_name,
        args.redcap_api_url,
        args.redcap_api_key,
        args.state_file,
        args.data_dictionary_path,
        args.sync_all,
    )
