import copy

import phonenumbers
import requests
from phonenumbers import NumberParseException
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.sql import and_, select
from sqlalchemy.sql.expression import func

from cc_utilities.logger import logger

TWILIO_LOOKUP_URL = "https://lookups.twilio.com/v1/PhoneNumbers"
COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME = "contact_phone_can_receive_sms"

WHITE_LISTED_TWILIO_CODES = [
    404,  # Twilio thinks it's not a valid number or otherwise can't find it
]

TWILIO_MOBILE_CODE = "mobile"


class TwilioLookUpError(Exception):
    def __init__(self, message, info):
        super(TwilioLookUpError, self).__init__(message)
        self.info = info


def process_contacts(data, search_column, twilio_sid, twilio_token):
    """Process a set of contacts' phone numbers to determine if can have SMS sent"""

    contacts = [
        dict(
            copy.deepcopy(item),  # vs fear of mutability...
            **{
                COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME: None,
                "standard_formatted_number": None,
            },
        )
        for item in data
    ]
    for contact in contacts:
        try:
            contact["standard_formatted_number"] = format_phone_number(
                contact["contact_phone_number"]
            )
        except NumberParseException:
            logger.warning(
                f"The number `{contact['contact_phone_number']}` for contcact "
                f"`{contact[search_column]}` cannot be parsed and will be marked as "
                f"unable to receive sms."
            )
            contact[COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME] = False
    for contact in [
        contact
        for contact in contacts
        if contact["standard_formatted_number"] is not None
    ]:
        contact[COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME] = process_phone_number(
            contact["contact_phone_number"], twilio_sid, twilio_token,
        )

    return contacts


def cleanup_processed_contacts_with_numbers(processed):
    """Remove unneeded key/value pairs from processed results to prep for CommCare"""
    for item in processed:
        item.pop("contact_phone_number")
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
    response = requests.get(
        f"{TWILIO_LOOKUP_URL}/{formatted_number}",
        auth=(sid, auth_token),
        params={"Type": "carrier"},
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


def can_receive_sms(number_type):
    """Determine if number type is SMS capable."""
    return number_type == TWILIO_MOBILE_CODE


def process_phone_number(formatted_number, sid, auth_token):
    """Determine if a formatted number can receive SMS.

    Args:
        formatted_number (str): Appropriately formatted number
            `+<country_code><national_number>`
        sid (str): A Twilio SID
        auth_token (str): A Twilio auth token

    Returns:
        bool indicating if number can receive SMS if number can be looked up
    """
    return can_receive_sms(
        twilio_lookup_phone_number_type(formatted_number, sid, auth_token)
    )


def get_unprocessed_contact_phone_numbers(db_url, search_column="id"):
    """Get a list of contact phone numbers that haven't been processed for SMS cap.

    Args:
        db_url (str): the db connection URL
        search_column (str): the name of the unique id column in the db for contact

    Returns:
        list: List of dicts with key/values for the search column and
            `contact_phone_number`
    """
    engine = create_engine(db_url)
    meta = MetaData(bind=engine)
    contact = Table("contact", meta, autoload=True, autoload_with=engine)
    # There is an edge case where COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME will not
    # be in contact table columns even if it's in contact case type on CommCare,
    # if there are no cases for this property with non-null values. We need to
    # only filter our query by this column value if it exists...
    has_can_sms_column = COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME in [
        col.name for col in contact.columns
    ]
    wheres = [
        contact.c.contact_phone_number.isnot(None),
        func.length(contact.c.contact_phone_number) > 0,
    ]
    if has_can_sms_column:
        wheres.append(contact.c.contact_phone_can_receive_sms.is_(None))
    query = select(
        [getattr(contact.c, search_column), contact.c.contact_phone_number]
    ).where(and_(*wheres))
    conn = engine.connect()
    try:
        result = conn.execute(query)
        return [dict(row) for row in result.fetchall()]
    finally:
        conn.close()
