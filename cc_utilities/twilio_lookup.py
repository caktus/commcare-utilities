import copy
import sqlite3

import pandas as pd
import phonenumbers
import requests
from phonenumbers import NumberParseException
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.sql import and_, or_, select
from sqlalchemy.sql.expression import func

from .constants import (
    COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME,
    COMMCARE_CAN_SMS_LABEL,
    COMMCARE_CANNOT_SMS_LABEL,
    COMMCARE_PHONE_FIELD,
    COMMCARE_UNSET_CAN_SMS_LABEL,
    TWILIO_LOOKUP_URL,
    TWILIO_MOBILE_CODE,
    WHITE_LISTED_TWILIO_CODES,
)
from .logger import logger

# State file for the Twilio sync. It can safely be deleted if need
# (it will be recreated on the next run).
TWILIO_LOOKUP_STATE_DB_FILE = "twilio_lookup_state.db"
# Table name for bad CommCare IDs in the Twilio lookup state DB.
BAD_PCC_IDS_TABLE_NAME = "bad_commcare_ids"


class TwilioLookUpError(Exception):
    def __init__(self, message, info):
        super(TwilioLookUpError, self).__init__(message)
        self.info = info


def process_records(data, search_column, twilio_sid, twilio_token):
    """Process a set of records' phone numbers to determine if can have SMS sent"""

    records = [
        dict(
            copy.deepcopy(item),  # vs fear of mutability...
            **{
                COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME: None,
                "standard_formatted_number": None,
            },
        )
        for item in data
    ]
    for record in records:
        try:
            record["standard_formatted_number"] = format_phone_number(
                record[COMMCARE_PHONE_FIELD]
            )
        except NumberParseException:
            logger.warning(
                f"The number `{record[COMMCARE_PHONE_FIELD]}` for "
                f"`{record[search_column]}` cannot be parsed and will be marked as "
                f"unable to receive sms."
            )
            record[COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME] = COMMCARE_CANNOT_SMS_LABEL
    for record in records:
        if record["standard_formatted_number"] is not None:
            record[COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME] = process_phone_number(
                record[COMMCARE_PHONE_FIELD], twilio_sid, twilio_token,
            )

    return records


def cleanup_processed_records_with_numbers(processed):
    """Remove unneeded key/value pairs from processed results to prep for CommCare"""
    for item in processed:
        item.pop(COMMCARE_PHONE_FIELD)
        item.pop("standard_formatted_number")
    return processed


def format_phone_number(raw, region="US"):
    """Format raw phone number into standardized format required by Twilio lookup API

    Args:
        raw (str): raw string representing a phone number
        region (str): the expected region of the phone number. Defaults to US.

    Returns:
        str: Number formatted to Twilio lookup spec `+<country_code><national_number>`
    """
    parsed = phonenumbers.parse(raw, region=region)
    return f"+{parsed.country_code}{parsed.national_number}"


def twilio_http_request(method, url, sid, auth_token):
    """
    Issue an HTTP request to Twilio with a retry strategy.
    """
    retry_strategy = Retry(
        total=5,
        backoff_factor=6,
        # 429 = "Too Many Requests"
        # https://support.twilio.com/hc/en-us/articles/360044308153-Twilio-API-response-Error-429-Too-Many-Requests-
        status_forcelist=[429],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    with requests.Session() as session:
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session.request(
            method, url, auth=(sid, auth_token), params={"Type": "carrier"},
        )


def twilio_lookup_phone_number_type(formatted_number, sid, auth_token):
    """Determine phone number carrier type for a formatted number

    Args:
        formatted_number (str): Appropriately formatted number
            `+<country_code><national_number>`
        sid (str): A Twilio SID
        auth_token (str): A Twilio auth token

    Returns:
        str indicating carrier type if number can be looked up, else 'unknown'
        if number can't be looked up.
    """
    response = twilio_http_request(
        "GET", f"{TWILIO_LOOKUP_URL}/{formatted_number}", sid, auth_token
    )
    if response.ok:
        return response.json()["carrier"]["type"]
    elif response.status_code in WHITE_LISTED_TWILIO_CODES:
        return "unknown"
    else:
        message = f"Something went wrong looking up number `{formatted_number}`."
        info = {
            "twilio_status_code": response.json().get("status_code"),
            "twilio_error_code": response.json().get("code"),
            "twilio_message": response.json().get("message"),
        }
        raise TwilioLookUpError(message, info)


def get_sms_capability(number_type):
    """Get sms capability label for a number type."""
    return (
        COMMCARE_CAN_SMS_LABEL
        if number_type == TWILIO_MOBILE_CODE
        else COMMCARE_CANNOT_SMS_LABEL
    )


def process_phone_number(formatted_number, sid, auth_token):
    """Get label for SMS capability of a phone number

    Args:
        formatted_number (str): Appropriately formatted number
            `+<country_code><national_number>`
        sid (str): A Twilio SID
        auth_token (str): A Twilio auth token

    Returns:
        Str indicating sms capability of looked up number
    """
    return get_sms_capability(
        twilio_lookup_phone_number_type(formatted_number, sid, auth_token)
    )


def get_unprocessed_phone_numbers(db_url, table_name="contact", search_column="id"):
    """Get a list of contact phone numbers that haven't been verified for SMS

    Args:
        db_url (str): the db connection URL
        search_column (str): the name of the unique id column in the db for contact

    Returns:
        list: List of dicts with key/values for the search column and
            `contact_phone_number`
    """
    engine = create_engine(db_url)
    meta = MetaData(bind=engine)
    table = Table(table_name, meta, autoload=True, autoload_with=engine)
    assert COMMCARE_PHONE_FIELD in [
        col.name for col in table.columns
    ], f"{COMMCARE_PHONE_FIELD} not in {table_name} table"
    # There is an edge case where COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME will not
    # be in the table's columns even if it's in case type on CommCare,
    # if there are no cases for this property with non-null values. We need to
    # only filter our query by this column value if it exists...
    has_can_sms_column = COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME in [
        col.name for col in table.columns
    ]

    wheres = [
        getattr(table.c, COMMCARE_PHONE_FIELD).isnot(None),
        func.length(getattr(table.c, COMMCARE_PHONE_FIELD)) > 0,
        table.c.id.notin_(get_bad_ids(table_name)),
    ]
    if has_can_sms_column:
        wheres.append(
            or_(
                getattr(table.c, COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME).is_(None),
                getattr(table.c, COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME)
                == COMMCARE_UNSET_CAN_SMS_LABEL,
            )
        )
    query = select(
        [getattr(table.c, search_column), getattr(table.c, COMMCARE_PHONE_FIELD)]
    ).where(and_(*wheres))
    conn = engine.connect()
    try:
        result = conn.execute(query)
        return [dict(row) for row in result.fetchall()]
    finally:
        conn.close()


def get_sqlite_conn():
    """
    Obtains a connection to a local state file in the form of a sqlite3 database
    that allows us to keep track of CommCare IDs that don't match existing
    records in CommCare.
    """
    con = sqlite3.connect(TWILIO_LOOKUP_STATE_DB_FILE)
    cur = con.cursor()
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {BAD_PCC_IDS_TABLE_NAME} (case_type text, id text)"
    )
    cur.close()
    return con


def add_bad_ids(case_type, ids):
    """
    Adds new bad CommCare IDs to the sqlite3 database.
    """
    df = pd.DataFrame({"id": ids, "case_type": case_type})
    df.to_sql(
        BAD_PCC_IDS_TABLE_NAME, get_sqlite_conn(), if_exists="append", index=False
    )


def get_bad_ids(case_type):
    """
    Returns a DataFrame with all bad CommCare IDs currently known.
    """
    return pd.read_sql(
        f"SELECT DISTINCT id FROM {BAD_PCC_IDS_TABLE_NAME} WHERE case_type = ?",
        get_sqlite_conn(),
        params=(case_type,),
    )["id"].tolist()
