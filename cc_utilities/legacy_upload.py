import csv
from functools import partial
from math import ceil, log2
from urllib.parse import urljoin
from uuid import uuid4

import dateparser
import phonenumbers
from retry import retry

from cc_utilities.common import (
    chunk_list,
    get_commcare_case,
    get_commcare_cases,
    upload_data_to_commcare,
)
from cc_utilities.constants import (
    CASE_REPORT_URL,
    COMMCARE_DEFAULT_CONTACT_KEY_VALUES,
    EMPTY_PHONE_VALUES,
    EMPTY_SELECT_VALUES,
)
from cc_utilities.logger import logger

MAX_CONTACTS_PER_PARENT_PATIENT = 100
MAX_RETRY_DELAY = 512


class LegacyUploadError(Exception):
    def __init__(self, message, info=None):
        super(LegacyUploadError, self).__init__(message)
        self.info = None


@retry(
    exceptions=LegacyUploadError,
    delay=1,
    tries=log2(MAX_RETRY_DELAY),
    max_delay=MAX_RETRY_DELAY,
    backoff=2,
    logger=logger,
)
def get_commcare_cases_by_external_id_with_backoff(
    project_slug, cc_user_name, cc_api_key, external_id
):
    """Wraps `get_commcare_cases` with retry and backoff behavior

    Attempts to retrieve CommCare cases by `external_id` for a given project space.
    Used when creating a case and immediately trying to retrieve. Newly created
    cases are not immediately available for retrieval via the API, and have been
    observed by the code author to take as long as 4 minutes to appear, though more
    often they are available within a few seconds.

    Args:
        project_slug (str): The name of the CommCare project (aka "domain")
        cc_user_name (str): Valid CommCare username
        cc_api_key (str): Valid CommCare API key
        external_id (str): Cases with the specified `external_id` will be retrieved

    Returns:
        list: A list of comprised of dicts representing a CommCare case
    """
    cases = get_commcare_cases(
        project_slug, cc_user_name, cc_api_key, external_id=external_id
    )
    if len(cases) == 0:
        # raising an exception triggers the retry behavior
        raise LegacyUploadError("Expected cases, but none returned yet")
    return cases


def clean_raw_case_data_df(df, data_dict):
    """Clean up a dataframe of raw case data ahead of validation and normalization

    Sometimes users provide data with strange idiosyncracies such as new lines in
    phone number fields (stemming from copy-pasta). This function fixes that problem

    Additionally, it converts all NA values to empty strings.

    Args:

        df (object): A pandas dataframe
        data_dict (dict): A dictionary whose keys are `col_name`s and whose values
            are a dict.

    Returns:
        df: A dataframe
    """
    # we need empty strings not NaN or other default missing vals, which will create
    # downstream problems. Note that this also effectively copies the dataframe
    # into a new variable, so we won't have side effect on the df passed to function
    df = df.fillna("")
    cleanup_column_types = [
        "date",
        "phone_number",
        "multi_select",
        "number",
        "select",
    ]
    cleanup_cols = [
        col for col in df.columns if data_dict[col]["data_type"] in cleanup_column_types
    ]
    removeable_chars = ["\n", "\r"]

    def _remove_chars(val, removable_chars=removeable_chars):
        for char in removable_chars:
            val = val.replace(char, "")
        return val

    for col in cleanup_cols:
        df[col] = df[col].apply(lambda val: _remove_chars(val.strip()))

    return df


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
        return False
    return True


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
                "allowed_values": [
                    i.strip() for i in item["allowed_values"].split(",")
                ],
                "data_type": item["data_type"],
                "required": item["required"],
            }
            for item in reader
        }
    return data_dict


def create_dummy_patient_case_data(external_id):
    """Create data for a dummy patient

    When importing legacy contacts into CommCare, a dummy patient id needs to be
    provided. We inclued an external_id so once created, dummy patients can be
    retrieved and their CommCare id can be attached to any contacts we upload. The
    values for `stub`, `name`, and `stub_type` are what CommCare wants to see for
    dummy patients.

    Args:
        external_id (str): Gets attached to the patient as `external_id` property,
            which can be used to retrieve the patient in order to get its CommCare id.

    Returns:
        dict: A dict with necessary data for creating a dummy patient.
    """
    return {
        "external_id": external_id,
        # `case_id` must be included in uploaded data, but when null, CommCare
        # will create a value
        "case_id": None,
        "stub": "yes",
        "name": "(no index case)",
        "stub_type": "contact_without_index",
        "current_status": "closed",
    }


def generate_cc_dummy_patient_cases(
    project_slug, cc_user_name, cc_api_key, num_dummies=1
):
    """Generate 1 or more dummy patient cases on CommCare and return their data

    Args:
        project_slug (str): The name of the CommCare project (aka "domain")
        cc_user_name (str): Valid CommCare username
        cc_api_key (str): Valid CommCare API key
        num_dummies (int): Number of dummy patients to create

    Returns:
        list: List comprised of `case_id`s for each created dummy patient case
    """
    external_ids = [generate_commcare_external_id() for i in range(num_dummies)]
    dummies_data = [create_dummy_patient_case_data(ext_id) for ext_id in external_ids]
    upload_data_to_commcare(
        dummies_data, project_slug, "patient", "case_id", cc_user_name, cc_api_key
    )
    # retrieve the dummies by contact id so we can get their case_ids, which we
    # will attach to contacts we later upload
    cc_dummy_patients = []
    for ext_id in external_ids:
        cc_dummy_patients.extend(
            get_commcare_cases_by_external_id_with_backoff(
                project_slug, cc_user_name, cc_api_key, external_id=ext_id
            )
        )
    return [patient["case_id"] for patient in cc_dummy_patients]


def upload_legacy_contacts_to_commcare(
    valid_normalized_contacts_data,
    project_slug,
    cc_user_name,
    cc_api_key,
    **contact_kwargs,
):
    """Upload a set of legacy contacts to CommCare.

    This function expects that contacts data sent to it has been validated and
    normalized beforehand. This function ultimately returns a dict whose keys are
    `contact_id`s and whose values are CommCare-generated `case_ids` for the associated
    contacts. These mappings can be used in the calling context to generate a report
    that provides URLs to view uploaded cases in CommCare, alongside the original user-
    supplied data.

    Args:
        valid_normalized_contacts_data (list): A list of dicts with user-supplied data
            for contacts to be uploaded. Additionally, each dict must contain a unique
            value for a `contact_id` field, which is not user-supplied, and should be
            dynamically generated in the calling context.
        project_slug (str): The name of the CommCare project (aka "domain")
        cc_user_name (str): Valid CommCare username
        contact_kwargs (dict): Additional key-value pairs to add to each contact.
            This is to support per-CommCare install specific requirements around
            fields that should be included on uploaded legacy-contacts.
    Returns:
        dict: A dict whose keys are 'contact_ids' and whose values are 'case_ids' of
            created contacts
    """
    num_dummy_patients = ceil(
        len(valid_normalized_contacts_data) / MAX_CONTACTS_PER_PARENT_PATIENT
    )
    logger.info(f"Generating {num_dummy_patients} dummy patients")
    patients = generate_cc_dummy_patient_cases(
        project_slug, cc_user_name, cc_api_key, num_dummies=num_dummy_patients
    )
    expected_batches = ceil(
        len(valid_normalized_contacts_data) / MAX_CONTACTS_PER_PARENT_PATIENT
    )
    logger.info(
        f"Processing contacts in {expected_batches} "
        f"{'batch' if expected_batches == 1 else 'batches'} of "
        f"{MAX_CONTACTS_PER_PARENT_PATIENT} contacts per batch."
    )

    created_contacts = []

    for i, batch in enumerate(
        chunk_list(valid_normalized_contacts_data, MAX_CONTACTS_PER_PARENT_PATIENT)
    ):
        batch_num = i + 1
        logger.info(
            f"Processing batch {batch_num} of {expected_batches} consisting of "
            f"{len(batch)} contacts."
        )
        parent_id = patients.pop()
        prepped_contacts = [
            generate_commcare_contact_data(contact, parent_id, **contact_kwargs)
            for contact in batch
        ]
        try:
            logger.info(f"Uploading contacts from batch {batch_num} to CommCare")
            upload_data_to_commcare(
                prepped_contacts,
                project_slug,
                "contact",
                "case_id",
                cc_user_name,
                cc_api_key,
            )
            logger.info(
                f"Retrieving parent case with case_id `{parent_id}` "
                f"for batch {batch_num}"
            )
            parent_case = get_commcare_case(
                parent_id,
                project_slug,
                cc_user_name,
                cc_api_key,
                include_child_cases=True,
            )
            for k in parent_case["child_cases"]:
                created_contacts.append(
                    (
                        parent_case["child_cases"][k]["properties"]["contact_id"],
                        parent_case["child_cases"][k]["case_id"],
                    )
                )
        # This is a rare exception (hah!) where a catch all except block is a good idea.
        # If there are multiple batches to be processed, and early ones succeed, but
        # a later one fails, we want to return a result to the calling context so a
        # report can be generated indicating which contacts were succesfully uploaded.
        # This will make it possible to remove rows that were succesfully uploaded from
        # the originally supplied data and try again later, without generating duplicate
        # case data in CommCare.
        except Exception:
            logger.exception(
                "[upload_legacy_contacts_to_commcare] Something went wrong"
            )
    result = {}
    for item in created_contacts:
        result[item[0]] = item[1]
    return result


def normalize_plain_field(raw_value):
    "Normalize a value whose CommCare data type is `plain`"
    return raw_value.strip()


def validate_number_field(raw_value):
    "Validate a value whose CommCare data type is `number`"
    if raw_value == "":
        return True
    try:
        int(raw_value)
        return True
    except ValueError:
        return False


def normalize_number_field(validated_raw_value):
    "Normalize a value whose CommCare data type is `number`"
    return int(validated_raw_value)


def validate_date_field(raw_value):
    "Validate a value whose CommCare data type is `date`"
    if raw_value == "":
        return True
    try:
        parsed = dateparser.parse(raw_value)
        return False if parsed is None else True
    except TypeError:
        return False


def normalize_date_field(validated_raw_value):
    "Normalize a value whose CommCare data type is `date`"
    if validated_raw_value == "":
        return ""
    parsed = dateparser.parse(validated_raw_value)
    return parsed.strftime("%Y-%m-%d")


def validate_select_field(raw_value, allowed_values):
    "Validate a value whose CommCare data type is `select`"
    if raw_value in EMPTY_SELECT_VALUES:
        return True
    return raw_value.strip() in allowed_values


def validate_multi_select_field(raw_value, allowed_values):
    "Validate a value whose CommCare data type is `multi_select`"
    if raw_value in EMPTY_SELECT_VALUES:
        return True
    values = [val.strip() for val in raw_value.split(",")]
    return set(values).issubset(set(allowed_values))


def validate_phone_number_field(raw_value, country_code="US"):
    "Validate a value whose CommCare data type is `phone_number`"
    # if this field is blank we just treat it as valid
    if raw_value in EMPTY_PHONE_VALUES:
        return True
    try:
        number = phonenumbers.parse(raw_value, country_code)
    except phonenumbers.NumberParseException:
        return False
    return phonenumbers.is_valid_number(number)


def normalize_phone_number(raw_value, col_name=None, country_code="US"):
    """Normalize a phone number to standard 10 digits (assuming US number)

    If the column name (`col_name`) is "contact_phone_number", the national number
    will be prepended with the country code, as this is what CommCareHQ wants to see
    for this field (vs. "phone_home" and "phone_work" which should be 10 digits alone)
    """
    # if this field is blank, we pass through empty string
    if raw_value in EMPTY_PHONE_VALUES:
        return ""
    number = phonenumbers.parse(raw_value, country_code)
    return (
        f"{number.country_code}{number.national_number}"
        if col_name == "contact_phone_number"
        else str(number.national_number)
    )


def get_validation_fn(col_name, data_dict):
    """Look up the validation function for a given column based on data dictionary

    Args:
        col_name (str): The name of a column
        data_dict (dict): A dictionary whose keys are `col_name`s and whose values
            are a dict.

    Returns:
        function: The function for validating the column.
    """
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
    # there's no good way to validate phone numbers unless users supply country code
    # and that is unlikely for our use case.  Alternatively, we could parse only for US
    # numbers, but contacts might have non-US phone numbers.
    if col_type == "phone_number":
        return validate_phone_number_field
    if col_type == "number":
        return validate_number_field
    if col_type == "date":
        return validate_date_field
    if col_type == "select":
        return lambda val: validate_select_field(
            val, data_dict[col_name]["allowed_values"]
        )
    if col_type == "multi_select":
        return lambda val: validate_multi_select_field(
            val, data_dict[col_name]["allowed_values"]
        )


def accumulate_row_validation_problems(
    row,
    colname=None,
    is_missing_required_error=False,
    fails_required_one_ofs=False,
    required_one_ofs=None,
):
    """Helper for accumulating validation problems across columns for a given row

    Args:
        row (object): A Pandas dataframe row
        colname (string): Optional. A name of a column validating for.
        is_missing_required_error (bool): Defaults to False. If `True` will generate
            a validation error message about a missing required value
        fails_required_one_ofs (bool): Defaults to False. If `True` will generate
            a validation error message about a row that doesn't have at least one
            of several "one-of" required values
        required_one_ofs (list): A list of required one ofs to be included in a
            validation message about required one ofs.

    Returns:
        str: A string of accumulated validation problem messages (i.e., what was
        there already + new validation problem messages)
    """
    required_one_ofs = required_one_ofs if required_one_ofs else []

    if fails_required_one_ofs:
        assert required_one_ofs
    assert not all([is_missing_required_error, fails_required_one_ofs])
    if is_missing_required_error:
        msg = f"A value must be supplied for {colname}"
    elif fails_required_one_ofs:
        msg = (
            f"A valid value must be supplied for one of the following "
            f"columns: {required_one_ofs}"
        )
    else:
        msg = f"Invalid value for {colname}"
    return (
        ", ".join([row["validation_problems"], msg])
        if row["validation_problems"]
        else msg
    )


def validate_and_annotate_row_values(df, data_dict, drop_columns=None):
    """Determine if each row is valid, and if not, annotate row with validation problems

    Args:

        df (object): Pandas dataframe
        data_dict (dict): A dictionary whose keys are `col_name`s and
            whose values are a dict whose keys are field, group, allowed_values,
            data_type, and required
        drop_columns (list): Optional. If included, these columns will not be considered
            when validating
    Returns:
        df: A new df based on original plus validation information
    """
    drop_columns = drop_columns if drop_columns else []
    df = df.copy(deep=True)
    for col in df.drop(drop_columns, axis=1).columns:
        df["tmp_col_is_valid"] = df[col].apply(get_validation_fn(col, data_dict))
        df["is_valid"] = df["is_valid"] & df["tmp_col_is_valid"]
        # if `row["tmp_col_is_valid"] is False` we apply
        # accumulate_row_validation_problems, otherwise we return the existing
        # value for `row["validation_problems"]`
        df["validation_problems"] = df.apply(
            lambda row: accumulate_row_validation_problems(row, colname=col)
            if not row["tmp_col_is_valid"]
            else row["validation_problems"],
            axis=1,
        )
        df.drop(columns=["tmp_col_is_valid"], inplace=True)
    return df


def validate_and_annotate_required_values(df, data_dict):
    """Determine if all required values have been fulfilled, and annotate accordingly

    NB: This does not validate if those required values are valid. That is assumed
    to have happened in a previous step.

    Args:
        df (object): Pandas dataframe
        data_dict (dict): A dictionary whose keys are `col_name`s and
            whose values are a dict whose keys are field, group, allowed_values,
            data_type, and required
    Returns:
        df: A new df based on original plus validation information
    """
    df = df.copy(deep=True)
    # validate no required values are missing
    for col_name in [col for col in data_dict if data_dict[col]["required"]]:
        df["tmp_col_is_valid"] = df[col_name].apply(lambda x: x not in ("", None))
        df["is_valid"] = df["is_valid"] & df["tmp_col_is_valid"]
        # if `row["tmp_col_is_valid"] is False` we apply
        # accumulate_row_validation_problems, otherwise we return the existing
        # value for `row["validation_problems"]`
        df["validation_problems"] = df.apply(
            lambda row: accumulate_row_validation_problems(
                row, colname=col_name, is_missing_required_error=True
            )
            if not row["tmp_col_is_valid"]
            else row["validation_problems"],
            axis=1,
        )
        df.drop(columns=["tmp_col_is_valid"], inplace=True)
    return df


def validate_and_annotate_one_of_required(df, data_dict, required_one_ofs=None):
    """Determine if "one-of" required logic is valid, and annotate accordingly

    Args:
        df (object): Pandas dataframe
        data_dict (dict): A dictionary whose keys are `col_name`s and
            whose values are a dict whose keys are field, group, allowed_values,
            data_type, and required
        required_one_ofs (list): Optional. A list of columns from which at least one
            must have a valid, non-null value per row
    Returns:
        df: A new df based on original plus validation information
    """
    required_one_ofs = required_one_ofs if required_one_ofs else []
    df = df.copy(deep=True)

    def _ensure_one_of_required(row, required_one_ofs, data_dict):
        # if no required_one_ofs sent over, then row is inherently valid for this rule
        if len(required_one_ofs) == 0:
            return True

        is_valid = False
        # if any of the required one ofs has a value that's valid, `is_valid` flips
        # to `True`
        for col_name in required_one_ofs:
            if row[col_name] and get_validation_fn(col_name, data_dict)(row[col_name]):
                is_valid = True
                break
        return is_valid

    df["tmp_col_is_valid"] = df.apply(
        _ensure_one_of_required, args=(required_one_ofs, data_dict), axis=1
    )
    df["is_valid"] = df["is_valid"] & df["tmp_col_is_valid"]
    # if `row["tmp_col_is_valid"] is False` we apply accumulate_row_validation_problems,
    # otherwise we return the existing value for `row["validation_problems"]`
    df["validation_problems"] = df.apply(
        lambda row: accumulate_row_validation_problems(
            row, fails_required_one_ofs=True, required_one_ofs=required_one_ofs
        )
        if not row["tmp_col_is_valid"]
        else row["validation_problems"],
        axis=1,
    )
    df.drop(columns=["tmp_col_is_valid"], inplace=True)
    return df


def validate_legacy_case_data(df, data_dict, required_one_ofs=None):
    """Validate user-supplied legacy case data based on a data dictionary

    Args:
        df (object): A Pandas dataframe generated by loading user-supplied legacy
            case data
        data_dict (dict): A dictionary whose keys are `col_name`s and
            whose values are a dict whose keys are field, group, allowed_values,
            data_type, and required
        required_one_ofs (list): Optional. A list of columns from which at least one
            must have a valid, non-null value per row
    Returns:
        obj: A copy of the original df, with additional columns with validation data.
    """
    required_one_ofs = required_one_ofs if required_one_ofs else []

    df = df.copy(deep=True)
    df["is_valid"] = True
    df["validation_problems"] = None
    df = validate_and_annotate_row_values(
        df, data_dict, ["is_valid", "validation_problems"]
    )
    df = validate_and_annotate_required_values(df, data_dict)
    df = validate_and_annotate_one_of_required(
        df, data_dict, required_one_ofs=required_one_ofs
    )
    return df


def get_normalization_fn(col_name, data_dict):
    """Look up the normalization function for a given column based on data dictionary

    Args:
        col_name (str): The name of a column
        data_dict (dict): A dictionary whose keys are `col_name`s and whose values
            are a dict.

    Returns:
        function: The function for normalizing a column.
    """
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
    if col_type == "phone_number":
        return partial(normalize_phone_number, col_name=col_name)
    if col_type == "select":
        return lambda val: val.strip()
    if col_type == "multi_select":
        return lambda val: " ".join([item.strip() for item in val.split(",")])


def normalize_legacy_case_data(validated_df, data_dict, ignore_columns=None):
    """Normalize (validated) legacy case data based on a data dictionary

    Args:
        validated_df (object): A Pandas dataframe generated by loading user-supplied
            legacy case data. Assumed to contain validated data.
        data_dict (dict): data_dict (dict): A dictionary whose keys are `col_name`s and
            whose values are a dict whose keys are field, group, allowed_values,
            data_type, and required
        ingore_columns (list): A list of columns to be ignored when normalizing.
    Returns:
        obj: A copy of the original df, with values normalized.
    """
    validated_df = validated_df.copy(deep=True)
    ignore_columns = ignore_columns if ignore_columns else []
    for col in validated_df.columns.drop(ignore_columns):
        validated_df[col] = validated_df[col].apply(
            get_normalization_fn(col, data_dict)
        )
    return validated_df


def generate_commcare_external_id(length=6):
    "Generate a 6-char long unique ID comprised of numbers and uppercase letters"
    return str(uuid4()).upper().replace("-", "")[:length]


def generate_commcare_case_report_url(case_id, project_slug):
    """Generate a URL for the detail view for a case in the CommCare dashboard"""
    return urljoin(CASE_REPORT_URL.format(project_slug), case_id)


def generate_commcare_contact_data(valid_normalized_data, patient_id, **kwargs):
    """Generate a dict representing a contact to be uploaded to CommCare.

    Args:
        valid_normalized_data (dict): Dict reprsenting a contact
        patient_id (str): String of a parent dummy patient
        **kwargs: Additional fields to be included on the contact.
    Returns:
        dict: A dict containing all key/val pairs that will be uploaded to CommCare.
    """
    if any(
        [
            "contact_id" not in valid_normalized_data,
            valid_normalized_data.get("contact_id") is None,
        ]
    ):
        msg = "[generate_commcare_contact_data] Must include a `contact_id`"
        logger.error(msg)
        raise LegacyUploadError(msg)

    return {
        **COMMCARE_DEFAULT_CONTACT_KEY_VALUES,
        **{"parent_id": patient_id},
        **valid_normalized_data,
        **kwargs,
    }
