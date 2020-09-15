import argparse
from math import ceil

from cc_utilities.common import upload_data_to_commcare
from cc_utilities.logger import logger
from cc_utilities.twilio_lookup import (
    cleanup_processed_contacts_with_numbers,
    get_unprocessed_contact_phone_numbers,
    process_contacts,
)


def chunk_list(lst, chunk_size):
    """Yield successive `chunk_size` chunks from `lst`"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


def main_with_args(
    db_url,
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    twilio_sid,
    twilio_token,
    search_column,
    batch_size=100,
):
    """The main routine

    Args:
        db_url (str): the db connection URL
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project being exported from
        twilio_sid (str): A Twilio SID
        twilio_token (str): A Twilio auth token
        search_column (str): : The name of the column in the db for contact that
            CommCare will match against in the bulk upload step. See
            https://confluence.dimagi.com/display/commcarepublic/Bulk+Upload+Case+Data
        batch_size (int): The size to batch process requests in. Each batch_size batch
            will be looked up in Twilio, and then script attempts to upload the
            results for that batch to CommCare, before moving on to next batch.

    """
    unprocessed = get_unprocessed_contact_phone_numbers(db_url, search_column)
    logger.info(f"{len(unprocessed)} unprocessed contacts found")
    expected_batches = ceil(len(unprocessed) / batch_size)
    logger.info(
        f"Processing contacts in {expected_batches} "
        f"{'batch' if expected_batches == 1 else 'batches'} of {batch_size} contacts "
        f"per batch."
    )
    for i, subset in enumerate(chunk_list(unprocessed, 100)):
        batch_num = i + 1
        logger.info(
            f"Processing batch {batch_num} of {expected_batches} consisting of "
            f"{len(subset)} contacts."
        )
        try:
            contacts_data = cleanup_processed_contacts_with_numbers(
                process_contacts(subset, search_column, twilio_sid, twilio_token,)
            )
        except Exception as exc:
            logger.error(f"Something unexpected happened: {exc.message}")
            raise exc
        logger.info(
            f"Uploading SMS capability status for {len(contacts_data)} contacts from "
            f"batch {batch_num} of {expected_batches} to CommCare."
        )
        upload_data_to_commcare(
            contacts_data,
            commcare_project_name,
            "contact",
            search_column,
            commcare_user_name,
            commcare_api_key,
            "off",
        )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--db",
        help="The db url string of the db that contains contact data",
        dest="db_url",
    )
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
        "--twilioSID", help="The SID of a Twilio account", dest="twilio_sid"
    )
    parser.add_argument(
        "--twilioToken", help="Auth token for the Twilio account", dest="twilio_token"
    )
    parser.add_argument(
        "--searchColumn",
        help="The column in db that will be matched as ID against Commcare's ID",
        dest="search_column",
        default="id",
    )
    args = parser.parse_args()
    main_with_args(
        args.db_url,
        args.commcare_user_name,
        args.commcare_api_key,
        args.commcare_project_name,
        args.twilio_sid,
        args.twilio_token,
        args.search_column,
    )
