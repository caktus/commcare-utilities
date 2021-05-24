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
    df = df.dropna(subset=[external_id_col])
    df.loc[:, "external_id"] = df[external_id_col]
    return df


def get_matching_cdms_patients(df, db_url, external_id_col, table_name="patient"):
    """
    Look up records by CDMS ID and DOB. This will be used to reject records
    that do not match, to avoid overwriting existing patient records with
    another patient's data.

    returns a list of matching rows, as dictionaries with external_id_col values.
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
    For unmatched patients, select from the original redcap_records dataframe
    because these will be sent back to REDCap and should not be sent back with
    CommCare-specific transformations and columns.
    """
    matched_external_ids = [m[external_id_col] for m in matched_external_ids]
    unmatched_records = redcap_records.where(
        -df[external_id_col].isin(matched_external_ids)
    ).dropna(subset=[external_id_col])
    matched_records = df.where(df[external_id_col].isin(matched_external_ids)).dropna(
        subset=[external_id_col]
    )
    return matched_records, unmatched_records


def add_reject_status_columns(reject_records, external_id_col):
    df = reject_records.copy()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_columns_and_values = {
        "integration_status": "rejected_person_mismatch",
        "integration_status_timestamp": timestamp,
        "integration_status_reason": f"mismatched {DOB_FIELD} and {external_id_col}",
    }
    for col_name, value in status_columns_and_values.items():
        df[col_name] = value
    return df


def send_back_to_redcap(df, redcap_api_url, redcap_api_key):
    logger.info(
        f"{len(df.index)} records were not found in CDMS, sending back to REDCap "
        f"with error status columns."
    )
    redcap_project = redcap.Project(redcap_api_url, redcap_api_key)
    response = redcap_project.import_records(
        to_import=df, overwrite="normal",  # Default, ignores blank values.
    )
    logger.info(f"Successfully sent back {response.get('count')} records.")
    return response


def handle_cdms_matching(
    df, redcap_records, db_url, external_id_col, redcap_api_url, redcap_api_key
):
    """
    Query the CDMS SQL mirror to match these records by ID and DOB,
    reject / send any non-matching records back to REDCap with error columns,
    and return the matched records that can be sent off to CommCare.
    """
    # Drop records missing DOB; these will also get sent back to REDCap.
    df = df.dropna(subset=[DOB_FIELD])
    matching_ids = get_matching_cdms_patients(df, db_url, external_id_col)
    matched_records, unmatched_records = select_records_by_cdms_matches(
        df, redcap_records, matching_ids, external_id_col
    )
    unmatched_records = add_reject_status_columns(unmatched_records, external_id_col)
    send_back_to_redcap(
        unmatched_records, redcap_api_url=redcap_api_url, redcap_api_key=redcap_api_key
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
