from collections import defaultdict
from datetime import datetime
from functools import partial

import pandas as pd
import redcap
from sqlalchemy import MetaData, Table, create_engine, select

from .common import (
    get_commcare_cases_by_external_id_with_backoff,
    upload_data_to_commcare,
)
from .constants import (
    ACCEPTED_INTERVIEW_DISPOSITION_VALUES,
    DOB_FIELD,
    EXTERNAL_ID,
    INTERVIEW_DISPOSITION,
    REDCAP_HOUSING_1_FIELD,
    REDCAP_HOUSING_2_FIELD,
    REDCAP_HOUSING_FIELD,
    REDCAP_HOUSING_OTHER,
    REDCAP_INTEGRATION_STATUS,
    REDCAP_INTEGRATION_STATUS_REASON,
    REDCAP_INTEGRATION_STATUS_TIMESTAMP,
    REDCAP_RECORD_ID,
    REDCAP_REJECTED_PERSON,
    REDCAP_SENT_TO_COMMCARE,
)
from .legacy_upload import normalize_phone_number
from .logger import logger


def get_cc_properties_and_source_val_lists(df):
    """
    Collects all checkbox columns in `df` that need to be collapsed (those
    including "___") and returns an iterator of (cc_property, source_val_list)
    tuples, where source_val_list is a list of (redcap_col, cc_value) tuples.
    """
    cc_properties = defaultdict(list)
    for redcap_col in [col for col in df.columns if "___" in col]:
        cc_property, cc_value = redcap_col.split("___", 1)
        cc_properties[cc_property].append((redcap_col, cc_value))
    return cc_properties.items()


def get_checkbox_values(row, source_val_list):
    """
    For the given DataFrame row and source_val_list (tuples of (redcap_col, cc_value)),
    collect the string representations of the checkbox values for CommCare and
    return the space-delimited list. Intended to be used via DataFrame.apply().
    """
    return (
        " ".join(
            # If REDCap value is equal to "1", return cc_value,
            # else return False (to be filtered out by filter()).
            filter(
                bool,
                [
                    cc_value
                    if pd.notnull(row[redcap_col]) and row[redcap_col].strip() == "1"
                    else False
                    for redcap_col, cc_value in source_val_list
                ],
            )
        )
        # If "", return None instead so empty columns can be filtered out properly
        # by split_cases_and_contacts().
        or None
    )


def collapse_checkbox_columns(df):
    """
    Converts REDCap-style checkbox columns ("box1___yellow", "box1___green") to
    CommCare-style checkbox files ("box1": "yellow green"). See test for details.
    """
    df = df.copy()
    for cc_property, source_val_list in get_cc_properties_and_source_val_lists(df):
        # Add new column with the checkbox values collapsed into a single column
        logger.info(f"Adding column {cc_property} to df")
        df[cc_property] = df.apply(
            get_checkbox_values, args=(source_val_list,), axis=1,
        )
        # Remove the obsolete columns
        redcap_cols = [col for col, _ in source_val_list]
        logger.info(f"Dropping columns {redcap_cols} from df")
        df.drop(columns=redcap_cols, inplace=True)
    return df


def collapse_housing_fields(df):
    """
    Reduce the 3 REDCap housing columns to one 'housing' column;
    By default, use the value of housing_1 and add to 'housing'.
    If the value of housing_1 is 'other', then select the value of 'housing_2'.
    """
    df = df.copy()

    def apply_collapse_housing(row):
        housing = row[REDCAP_HOUSING_1_FIELD]
        if row[REDCAP_HOUSING_1_FIELD] == REDCAP_HOUSING_OTHER:
            housing = row[REDCAP_HOUSING_2_FIELD]
        return housing

    df[REDCAP_HOUSING_FIELD] = df.apply(lambda row: apply_collapse_housing(row), axis=1)
    df = df.drop([REDCAP_HOUSING_1_FIELD, REDCAP_HOUSING_2_FIELD], axis=1)
    return df


def normalize_phone_cols(df, phone_cols):
    """
    For the given phone number columns, apply normalize_phone_number().
    """
    df = df.copy()
    for col_name in phone_cols:
        if col_name in df.columns:
            df[col_name] = (
                df[col_name]
                # Replace N/A values with empty string before normalizing.
                .fillna("").apply(partial(normalize_phone_number, col_name=col_name))
            )
        else:
            # Don't fail altogether in case of a misconfiguration in the calling
            # script, but do issue a warning.
            logger.warning(
                f'Phone column "{col_name}" requested to be normalized '
                "but not found in dataframe."
            )
    return df


def set_external_id_column(df, external_id_col):
    """
    For the given external_id_col, drop any rows with no value and
    copy to a new column named "external_id"
    """
    df = df.copy()
    df = df.dropna(subset=[external_id_col])
    df[EXTERNAL_ID] = df[external_id_col]
    return df


def query_cdms_for_external_ids_and_dobs(
    df, db_url, external_id_col, table_name="patient"
):
    """
    Look up records in the SQL Mirror and get CDMS ID and DOB.
    This will be used to reject records that do not match,
    to avoid overwriting existing patient records with
    another patient's data.

    Returns a list of matching rows, as dictionaries with external_id_col values.
    """
    external_ids = df[EXTERNAL_ID].tolist()
    engine = create_engine(db_url)
    meta = MetaData(bind=engine)
    table = Table(table_name, meta, autoload=True, autoload_with=engine)
    query = select(
        [getattr(table.c, external_id_col), getattr(table.c, DOB_FIELD)]
    ).where(
        getattr(table.c, external_id_col).in_(external_ids),
        getattr(table.c, DOB_FIELD).isnot(None),
        getattr(table.c, DOB_FIELD) != "",
    )
    cdms_patients_data = pd.read_sql(query, engine).to_dict(orient="records")
    return cdms_patients_data


def drop_external_ids_not_in_cdms(df, external_id_col, cdms_patients_data):
    """
    If a CDMS ID was not returned by the SQL Mirror, ignore it so that
    we can sync it if it does ever come around in future syncs.

    Returns a DataFrame minus records not in external_ids.
    """
    external_ids = [d[external_id_col] for d in cdms_patients_data]
    df = df.where(df[external_id_col].isin(external_ids)).dropna(
        subset=[external_id_col]
    )
    return df


def get_records_matching_dob(df, external_id_col, cdms_patients_data):
    """
    Accept records where the DOB from REDCap (in df) matches
    the DOB in the CDMS patients data.

    Returns a list of external IDs that may continue to be synced to CommCare.
    """
    lookup_df = df.set_index(external_id_col)
    matching_ids_dobs = {d[external_id_col]: d[DOB_FIELD] for d in cdms_patients_data}
    accepted_external_ids = []
    for external_id, cdms_dob in matching_ids_dobs.items():
        redcap_dob = lookup_df.loc[external_id][DOB_FIELD]
        if redcap_dob == cdms_dob:
            accepted_external_ids.append(external_id)
    return accepted_external_ids


def split_records_by_accepted_external_ids(df, accepted_external_ids, external_id_col):
    """
    Given the subset of external IDs in accepted_external_ids,
    return two DataFrames; one with these IDs and one without.
    """
    reject_records = df.where(~df[external_id_col].isin(accepted_external_ids)).dropna(
        subset=[external_id_col]
    )
    accept_records = df.where(df[external_id_col].isin(accepted_external_ids)).dropna(
        subset=[external_id_col]
    )
    logger.info(
        f"{len(accept_records.index)} were accepted, "
        f"and {len(reject_records.index)} records were rejected."
    )
    return accept_records, reject_records


def add_integration_status_columns(df, status, reason=""):
    """
    Add integration status columns with values to indicate rejection or success
    of syncing the records. A reason should be included with rejected records
    to be reviewed by a human.

    :param df: DataFrame with REDCap record IDs
    :param status: str, must be either REDCAP_REJECTED_PERSON or REDCAP_SENT_TO_COMMCARE.
    :param reason: str explaining the integration status reason if being rejected.
    """
    df = df.copy()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_columns_and_values = {
        REDCAP_INTEGRATION_STATUS: status,
        REDCAP_INTEGRATION_STATUS_TIMESTAMP: timestamp,
    }
    if reason:
        status_columns_and_values[REDCAP_INTEGRATION_STATUS_REASON] = reason
    logger.info(
        f"Adding integration status values to records: " f"{status_columns_and_values}"
    )
    for col_name, value in status_columns_and_values.items():
        df[col_name] = value
    return df


def import_records_to_redcap(df, redcap_api_url, redcap_api_key):
    """
    This is used to update records in REDCap with the integration status.
    """
    logger.info(f"Updating {len(df.index)} records in REDCap.")
    redcap_project = redcap.Project(redcap_api_url, redcap_api_key)
    response = redcap_project.import_records(
        to_import=df, overwrite="normal",  # Default, ignores blank values.
    )
    logger.info(f"Done updating {response.get('count')} REDCap records.")
    return response


def update_successful_records_in_redcap(
    complete_records, incomplete_records, redcap_api_url, redcap_api_key
):
    """
    When records are successfully sent to CommCare, update the integration status
    in REDCap to indicate success.
    """
    df = pd.concat([complete_records, incomplete_records])
    if len(df) > 0:
        df = df[[REDCAP_RECORD_ID]]
        df = add_integration_status_columns(df, status=REDCAP_SENT_TO_COMMCARE)
        response = import_records_to_redcap(df, redcap_api_url, redcap_api_key)
        return response


def handle_cdms_matching(df, db_url, external_id_col, redcap_api_url, redcap_api_key):
    """
    Query the CommCare SQL mirror to match these records by ID and DOB,
    reject / send any non-matching records back to REDCap with integration
    status columns, and return a DataFrame of accepted records that can be
    sent to CommCare.
    """
    logger.info(
        f"Checking CommCare DB mirror for DOB and ID matches on {len(df.index)} records."
    )
    cdms_patients_data = query_cdms_for_external_ids_and_dobs(
        df, db_url, external_id_col
    )
    df = drop_external_ids_not_in_cdms(df, external_id_col, cdms_patients_data)
    matching_ids = get_records_matching_dob(df, external_id_col, cdms_patients_data)
    accept_records, reject_records = split_records_by_accepted_external_ids(
        df, matching_ids, external_id_col
    )
    if len(reject_records) > 0:
        # Select only the 'record_id' column to identify records in REDCap
        # and only update the integration status columns added below.
        reject_records = reject_records[[REDCAP_RECORD_ID]]
        reject_records = add_integration_status_columns(
            reject_records,
            status=REDCAP_REJECTED_PERSON,
            reason=f"mismatched {DOB_FIELD} and {external_id_col}",
        )
        import_records_to_redcap(
            reject_records,
            redcap_api_url=redcap_api_url,
            redcap_api_key=redcap_api_key,
        )
    return accept_records


def get_commcare_cases_with_acceptable_interview_dispositions(
    df, external_id_col, cc_api_key, cc_user_name, project_slug
):
    """
    Look up existing cases in CommCare and compare with accepted interview_disposition
    values. This should allow us to filter out cases which have already been filled
    out by a case investigator so that patient surveys do not override case
    investigator's data. We will reject the cases not matching and send
    them back to REDCap, and otherwise continue to send them to CommCare.
    """
    accepted_external_ids = []
    external_ids = df[external_id_col].to_list()
    for ext_id in external_ids:
        # Get cases in CommCare to compare interview_disposition. Querying
        # the SQL mirror would be a favorable source of truth for this, but did
        # not seem to have this column available at the time of implementing this.
        cases = get_commcare_cases_by_external_id_with_backoff(
            project_slug, cc_user_name, cc_api_key, external_id=ext_id
        )
        if cases:
            case_properties = cases[0].get("properties")
            interview_disposition = case_properties.get(INTERVIEW_DISPOSITION)
            if interview_disposition in ACCEPTED_INTERVIEW_DISPOSITION_VALUES:
                accepted_external_ids.append(ext_id)
    return accepted_external_ids


def reject_records_already_filled_out_by_case_investigator(
    df,
    external_id_col,
    project_slug,
    cc_user_name,
    cc_api_key,
    redcap_api_url,
    redcap_api_key,
):
    """
    Look up records in CommCare and determine if they are OK to push based on
    logic that determines whether they have already been filled out by
    a Case Investigator.
    Reject any records not matched and send back to REDCap to update the integration
    status to be reviewed by a human.
    Returns a DataFrame of records that may continue being synced to CommCare.
    """
    logger.info("Checking for records already filled out by a Case Investigator...")
    accepted_external_ids = get_commcare_cases_with_acceptable_interview_dispositions(
        df, external_id_col, cc_api_key, cc_user_name, project_slug
    )
    accept_records, reject_records = split_records_by_accepted_external_ids(
        df, accepted_external_ids, external_id_col
    )
    if len(reject_records) > 0:
        reject_records = reject_records[[REDCAP_RECORD_ID]]
        reject_records = add_integration_status_columns(
            reject_records,
            status=REDCAP_REJECTED_PERSON,
            reason="Case already submitted by a Case Investigator.",
        )
        import_records_to_redcap(reject_records, redcap_api_url, redcap_api_key)
    logger.info("Done.")
    return accept_records


def split_complete_and_incomplete_records(df):
    """
    Splits the DataFrame into two - 'complete' meaning all rows with no
    column values missing, and 'incomplete' is the remainder.
    """
    # Drop columns where all rows are missing data.
    df = df.dropna(axis=1, how="all")
    # Drop rows where any values are missing from columns.
    complete_records = df.dropna()
    # The inverse; select rows where any values are missing from columns.
    incomplete_records = df[df.isna().any(axis=1)]
    return complete_records, incomplete_records


def upload_complete_records(
    complete_records, commcare_api_key, commcare_project_name, commcare_user_name
):
    """Uploads the given DataFrame to CommCare if there are any rows."""
    logger.info(
        f"Uploading {len(complete_records.index)} found patients (cases) "
        f"with complete records to CommCare..."
    )
    if len(complete_records.index) > 0:
        upload_data_to_commcare(
            complete_records,
            commcare_project_name,
            "patient",
            EXTERNAL_ID,
            commcare_user_name,
            commcare_api_key,
            create_new_cases="off",
            search_field=EXTERNAL_ID,
        )


def upload_incomplete_records(
    incomplete_records, commcare_api_key, commcare_project_name, commcare_user_name
):
    """
    To avoid overwriting existing data in CommCare with blank values,
    iterate over the incomplete records one by one, drop any blank/null values
    before uploading to CommCare.
    **Note that iterating over rows of a DataFrame is slow and not recommended for
    most use cases!
    """
    logger.info(
        f"Uploading {len(incomplete_records.index)} found patients (cases) "
        f"with incomplete records to CommCare..."
    )
    for index, row in incomplete_records.iterrows():
        # Drops any values in this Series with missing/NA values,
        # and converts it back to a DataFrame.
        # **Note that iterrows does not preserve the type of a cell.
        data = row.dropna().to_frame().transpose()
        upload_data_to_commcare(
            data,
            commcare_project_name,
            "patient",
            EXTERNAL_ID,
            commcare_user_name,
            commcare_api_key,
            create_new_cases="off",
            search_field=EXTERNAL_ID,
        )
