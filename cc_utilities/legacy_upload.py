import csv
from math import ceil, log2
from uuid import uuid4

import dateparser
import phonenumbers
from phonenumbers import NumberParseException
from retry import retry

from cc_utilities.common import (
    chunk_list,
    get_commcare_case,
    get_commcare_cases,
    upload_data_to_commcare,
)
from cc_utilities.logger import logger

MAX_CONTACTS_PER_PARENT_PATIENT = 100


class LegacyUploadError(Exception):
    def __init__(self, message, info=None):
        super(LegacyUploadError, self).__init__(message)
        self.info = None


MAX_DELAY = 512


@retry(
    exceptions=LegacyUploadError,
    delay=1,
    tries=log2(MAX_DELAY),
    max_delay=MAX_DELAY,
    backoff=2,
    logger=logger,
)
def get_commcare_case_with_backoff(case_id, project_slug, cc_user_name, cc_api_key):
    # parent_case = get_commcare_case(
    #     case_id, project_slug, cc_user_name, cc_api_key, include_child_cases=True
    # )
    case = get_commcare_case()
    return case


@retry(
    exceptions=LegacyUploadError,
    delay=1,
    tries=log2(MAX_DELAY),
    max_delay=MAX_DELAY,
    backoff=2,
    logger=logger,
)
def get_commcare_cases_with_backoff(
    project_slug, cc_user_name, cc_api_key, external_id
):
    cases = get_commcare_cases(
        project_slug, cc_user_name, cc_api_key, external_id=external_id
    )
    if len(cases) == 0:
        raise LegacyUploadError("Expected cases, but none returned yet")
    return cases


def validate_case_data_columns(column_names, allowed_columns, required_columns=None):
    """Determine if all columns are allowed and all required columns appear

     Args:
        column_names (list): list of column name strings being validated
        allowed_columns (list): list of allowed column names
        required_columns (list): list of columns that must appear
    Returns:
        bool: True if valid, else False
    """
    required_columns = required_columns if required_columns else []
    problems = []
    unexpected_columns = list(set(column_names).difference(set(allowed_columns)))
    missing_required_columns = list(set(required_columns).difference(set(column_names)))
    for col in unexpected_columns:
        problems.append(
            f"Found column `{col}` in case data but this does not appear in data "
            f"dictionary"
        )
    for col in missing_required_columns:
        problems.append(f"Column `{col}` is required, but was missing from case data")
    if len(problems):
        msg = f"`validate_case_data_columns` found {len(problems)} problems"
        logger.error(msg)
        for problem in problems:
            logger.error(problem)
        raise LegacyUploadError(
            f"{msg}. See `.info` on this error for details.", problems
        )
    return True


def get_lookup_ids_for_project_slug(project_slug, lookup_table_path, id_names):
    """Get required ids that are derived from a project slug

    This function allows dynamic retrieval of any required ids for uploading case
    data for a given project. Depending on the CommCare instance, different ids may
    be required based on the project slug (for instance, if CommCare is being deployed
    accross several agencies within a state, it might be desirable to include an
    `agency_id` field to each uploaded case. Rather than putting this reponsibility
    on the users at the agency who are preparing data for upload, this data can be
    derived from a lookup table that the data analyst running this script supplies.

    Args:
        project_slug (str): The CommCare project slug
        lookup_table_path (str): Where the lookup table is located
        id_names (list): A list of ids to lookup from lookup table, based on the
            project_slug
    Returns:
        dict: A dict whose keys are id_names and whose values are the discovered
            values for project matching project_slug
    )
    """
    with open(lookup_table_path) as fl:
        reader = csv.DictReader(fl)
    lookup_ids = dict.fromkeys(id_names)
    for row in reader:
        if row["project_slug"] == project_slug:
            for key in lookup_ids:
                lookup_ids[key] = row.get(key)
            break
    return lookup_ids


def load_data_dict(data_dict_path):
    """Load a data dict based on a path.

    Args:
        data_dict_path (str): Where the data dict is located. This file is a CSV.

    Returns:
        dict: A dict whose keys are allowed fields for a case type. For each field,
            there is a dict with the field's group, allowed values, data type,
            description, and whether or not it is required.
    """
    with open(data_dict_path) as fl:
        reader = csv.DictReader(fl)
    data_dict = {
        item["field"]: {
            "group": item["group"],
            "allowed_values": [i.strip() for i in ",".split(item["allowed_values"])],
            "data_type": item["data_type"],
            "description": item["description"],
            "required": item["required"],
        }
        for item in reader
    }
    return data_dict


def create_dummy_patient_case_data(external_id):
    """Create data for a dummy patient

    When importing legacy contacts into CommCare, a dummy patient id needs to be
    provided. We inclued an external_id so once created, dummy patients can be
    retrieved and their CommCare id can be attached to any contacts we upload.

    Args:
        external_id (str): Gets attached to the patient as `external_id` property,
            which can be used to retrieve the patient in order to get its CommCare id.

    Returns:
        dict: A dict with necessary data for creating a dummy patient.
    """
    return {
        "external_id": external_id,
        "case_id": None,
        "stub": "yes",
        "name": "(no index case)",
        "stub_type": "contact_without_index",
        "close": "yes",
    }


def generate_cc_dummy_patient_cases(
    project_slug, cc_user_name, cc_api_key, num_dummies=1
):
    """Generate 1 or more dummy patient cases on CommCare and return their data

    Args:
        project_slug (str):
        cc_user_name (str):
        cc_api_key (str):
        num_dummies (int):

    Returns:
        list:
    """
    external_id = str(uuid4())
    dummies_data = [
        create_dummy_patient_case_data(external_id) for i in range(num_dummies)
    ]
    upload_data_to_commcare(
        dummies_data, project_slug, "patient", "case_id", cc_user_name, cc_api_key
    )
    cc_dummy_patients = get_commcare_cases_with_backoff(
        project_slug, cc_user_name, cc_api_key, external_id=external_id
    )
    closed_patient_data = [
        dict(case_id=patient["case_id"], closed=True) for patient in cc_dummy_patients
    ]
    upload_data_to_commcare(
        closed_patient_data,
        project_slug,
        "patient",
        "case_id",
        cc_user_name,
        cc_api_key,
        create_new_cases="off",
    )
    return [patient["case_id"] for patient in cc_dummy_patients]


def upload_legacy_contacts_to_commcare(
    valid_normalized_contacts_data,  # these need some sort of tempid that allows connecting back to dataframe, and ultimately
    project_slug,
    cc_user_name,
    cc_api_key,
    report_handler,
    owner_id=None,
    agency_id=None,
    county_id=None,
):
    num_dummy_patients = ceil(
        len(valid_normalized_contacts_data) / MAX_CONTACTS_PER_PARENT_PATIENT
    )
    patients = generate_cc_dummy_patient_cases(
        project_slug, owner_id, cc_user_name, cc_api_key, num_dummies=num_dummy_patients
    )
    created_contacts = []
    try:
        for batch in chunk_list(
            valid_normalized_contacts_data, MAX_CONTACTS_PER_PARENT_PATIENT
        ):
            # for i, subset in enumerate(chunk_list(unprocessed, batch_size)):
            # batch_num = i + 1
            # logger.info(
            #     f"Processing batch {batch_num} of {expected_batches} consisting of "
            #     f"{len(subset)} contacts."
            # )
            # try:
            #     contacts_data = cleanup_processed_contacts_with_numbers(
            #         process_contacts(subset, search_column, twilio_sid, twilio_token,)
            #     )
            # except Exception as exc:
            #     logger.error(f"Something unexpected happened: {exc.message}")
            #     raise exc
            # logger.info(
            #     f"Uploading SMS capability status for {len(contacts_data)} contacts from "
            #     f"batch {batch_num} of {expected_batches} to CommCare."
            # )
            parent_id = patients.pop()
            prepped_contacts = [
                generate_commcare_contact_data(
                    contact, parent_id, owner_id, agency_id, county_id
                )
                for contact in batch
            ]
            upload_data_to_commcare(
                prepped_contacts,
                project_slug,
                "contact",
                "case_id",
                cc_user_name,
                cc_api_key,
            )
            # parent_case = get_commcare_case_with_backoff(
            #     parent_id, project_slug, cc_user_name, cc_api_key
            # )
            import pdb

            pdb.set_trace()
            # get the contact_ids and add them to created_contacts
    finally:
        report_handler(created_contacts)


def normalize_plain_field(raw_value):
    return raw_value.strip()


def validate_phone_number_field(raw_value):
    if raw_value.strip() == "":
        return True
    try:
        number = phonenumbers.parse(raw_value, region="US")
        return phonenumbers.is_possible_number(number)
    except NumberParseException:
        return False


def validate_number_field(raw_value):
    try:
        int(raw_value)
        return True
    except ValueError:
        return False


def normalize_number_field(validated_raw_value):
    return int(validated_raw_value)


def validate_date_field(raw_value):
    try:
        parsed = dateparser.parse(raw_value)
        return False if parsed is None else True
    except TypeError:
        return False


def normalize_date_field(validated_raw_value):
    parsed = dateparser.parse(validated_raw_value)
    return parsed.strftime("%Y-%m-%d")


def validate_select_field(value, accepted_values):
    return value in accepted_values


def validate_multi_select_field(raw_value, accepted_values):
    values = [val.strip() for val in "".split(raw_value)]
    return set(values).issubset(set(accepted_values))


def get_validation_fn(col_name, data_dict):
    col_type = data_dict[col_name]["data_type"]
    if col_type not in (
        "plain",
        "phone_number",
        "number",
        "date",
        "select",
        "multi_select",
    ):
        msg = f"Unexpected column type for column `{col_name}`: {col_type}"
        logger.error(msg)
        raise LookupError(msg)
    if col_type == "plain":
        return lambda val: True
    if col_type == "phone_number":
        return validate_phone_number_field
    if col_type == "number":
        return validate_number_field
    if col_type == "date":
        return validate_date_field
    if col_type == "select":
        return lambda val: validate_select_field(
            val, data_dict[col_name]["accepted_values"]
        )
    if col_type == "multi-select":
        return lambda val: validate_multi_select_field(
            val, data_dict[col_name]["accepted_values"]
        )


def validate_legacy_case_data(df, data_dict):
    df["is_valid"] = True
    df["validation_problems"] = ""
    for col in df.columns:
        df["tmp_col_is_valid"] = df[col].apply(
            get_validation_fn(col, data_dict), axis=1
        )
        df["is_valid"] = df["is_valid"] & df["tmp_col_is_valid"]
        df["validation_problems"] = (
            df["validation_problems"]
            if df["tmp_col_is_valid"]
            else df[["validation_problems", "tmp_col_is_valid"]].apply(
                lambda x, y: ", ".join(x, f"Invalid value for {col}"), axis=1
            )
        )
        df.drop(columns=["tmp_col_is_valid"], inplace=True)
    return df


def get_normalization_fn(col_name, data_dict):
    col_type = data_dict[col_name]["data_type"]
    if col_type not in (
        "plain",
        "phone_number",
        "number",
        "date",
        "select",
        "multi_select",
    ):
        msg = f"Unexpected column type for column `{col_name}`: {col_type}"
        logger.error(msg)
        raise LookupError(msg)
    if col_type == "plain":
        return normalize_plain_field
    if col_type == "number":
        return normalize_number_field
    if col_type == "date":
        return normalize_date_field
    if col_type in ("select", "multi-select", "phone_number"):
        return lambda val: val


def normalize_legacy_case_data(validated_df, data_dict):
    for col in validated_df.columns:
        validated_df[col].apply(get_normalization_fn(col, data_dict), axis=1)
    return validated_df


def generate_commcare_contact_data(
    valid_normalized_data, patient_id, owner_id, agency_id, county_id=None
):
    default_data = {
        "owner_id": owner_id,
        "AgencyID": agency_id,
        "CountyID": county_id,
        "parent_type": "patient",
        "parent_id": patient_id,
        "contact_id": uuid4().upper().replace("-", "")[:5],
        "ooj": "no",
    }

    return {**default_data, **valid_normalized_data}
