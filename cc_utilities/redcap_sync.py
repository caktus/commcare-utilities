from collections import defaultdict
from datetime import datetime
from functools import partial

import pandas as pd
import redcap
from sqlalchemy import MetaData, Table, and_, create_engine, or_, select

from .common import upload_data_to_commcare
from .constants import (
    DOB_FIELD,
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
        # replace N/A values with empty string before normalizing
        df[col_name] = (
            df[col_name]
            .fillna("")
            .apply(partial(normalize_phone_number, col_name=col_name))
        )
    return df


def set_external_id_column(df, external_id_col):
    """
    For the given external_id_col, drop any rows with no value and
    copy to a new column named "external_id"
    """
    df = df.copy()
    df = df.dropna(subset=[external_id_col])
    df["external_id"] = df[external_id_col]
    return df


def get_matching_cdms_patients(df, db_url, external_id_col, table_name="patient"):
    """
    Look up records by CDMS ID and DOB. This will be used to reject records
    that do not match, to avoid overwriting existing patient records with
    another patient's data.

    Returns a list of matching rows, as dictionaries with external_id_col values.
    """
    # Load table
    engine = create_engine(db_url)
    meta = MetaData(bind=engine)
    table = Table(table_name, meta, autoload=True, autoload_with=engine)

    # Validate columns
    column_names = [col.name for col in table.columns]
    assert DOB_FIELD in column_names, f"{DOB_FIELD} not in {table_name} table"
    assert (
        external_id_col in column_names
    ), f"{external_id_col} not in {table_name} table"

    # Define the query
    wheres = []
    for record in df.itertuples():
        dob = record.dob
        external_id = getattr(record, external_id_col)
        wheres.append(
            [
                getattr(table.c, external_id_col) == external_id,
                getattr(table.c, DOB_FIELD) == dob,
            ]
        )
    query = select([getattr(table.c, external_id_col)]).where(
        or_(*[and_(*where) for where in wheres])
    )

    # Execute
    conn = engine.connect()
    try:
        result = conn.execute(query)
        return [dict(row) for row in result.fetchall()]
    finally:
        conn.close()


def select_records_by_cdms_matches(
    df, redcap_records, matched_external_ids, external_id_col
):
    """
    Given the subset of external IDs in matched_external_ids,
    return two DataFrames; one with those matched patients and one without.
    """
    matched_external_ids = [m[external_id_col] for m in matched_external_ids]
    unmatched_records = redcap_records.where(
        ~redcap_records[external_id_col].isin(matched_external_ids)
    ).dropna(subset=[external_id_col])
    matched_records = df.where(df[external_id_col].isin(matched_external_ids)).dropna(
        subset=[external_id_col]
    )
    logger.info(
        f"{len(matched_records.index)} were matched in CDMS by DOB and CDMS ID, "
        f"and {len(unmatched_records.index)} records were not found."
    )
    return matched_records, unmatched_records


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


def handle_cdms_matching(
    df, redcap_records, db_url, external_id_col, redcap_api_url, redcap_api_key
):
    """
    Query the CommCare SQL mirror to match these records by ID and DOB,
    reject / send any non-matching records back to REDCap with error columns,
    and return the matched records that can be sent off to CommCare.
    """
    # Drop records missing DOB; these will also get sent back to REDCap.
    logger.info(
        f"Checking CommCare DB mirror for DOB and ID matches on {len(df.index)} records."
    )
    df = df.dropna(subset=[DOB_FIELD])
    matching_ids = get_matching_cdms_patients(df, db_url, external_id_col)
    matched_records, unmatched_records = select_records_by_cdms_matches(
        df, redcap_records, matching_ids, external_id_col
    )
    if len(unmatched_records) > 0:
        # Select only the 'record_id' column to identify records in REDCap
        # and only update the integration status columns added below.
        unmatched_records = unmatched_records[[REDCAP_RECORD_ID]]
        unmatched_records = add_integration_status_columns(
            unmatched_records,
            status=REDCAP_REJECTED_PERSON,
            reason=f"mismatched {DOB_FIELD} and {external_id_col}",
        )
        import_records_to_redcap(
            unmatched_records,
            redcap_api_url=redcap_api_url,
            redcap_api_key=redcap_api_key,
        )
    return matched_records


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
            "external_id",
            commcare_user_name,
            commcare_api_key,
            create_new_cases="off",
            search_field="external_id",
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
            "external_id",
            commcare_user_name,
            commcare_api_key,
            create_new_cases="off",
            search_field="external_id",
        )
