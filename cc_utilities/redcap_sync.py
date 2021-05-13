from collections import defaultdict
from functools import partial

import pandas as pd

from .common import upload_data_to_commcare
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
    For the given external_id_col, copy to a new column named "external_id"
    """
    df.dropna(subset=[external_id_col], inplace=True)
    df["external_id"] = df[external_id_col]
    return df


def split_cases_and_contacts(df, external_id_col):
    """
    FIXME - this is unused and will need updating (the expected columns
     redcap_repeat_instrument and redcap_repeat_instance don't exist).
    Splits a single dataframe of cases and contacts in two, based on the values in the
    "redcap_repeat_instrument" column, and assigns the columns necessary for import
    to CommCare. `external_id_col` is the name of the REDCap column that should be assigned
    to the external_id property in CommCare.
    """
    required_cols = [
        external_id_col,
        "redcap_repeat_instrument",
        "redcap_repeat_instance",
        "record_id",
    ]
    for col_name in required_cols:
        if col_name not in df.columns:
            raise ValueError(f"Column {col_name} not found in REDCap")
    # Rows with null value in "redcap_repeat_instrument" column and columns that contain
    # only missing values removed.
    cases_df = df.loc[df["redcap_repeat_instrument"].isnull()].dropna(axis=1, how="all")
    cases_df["external_id"] = cases_df[external_id_col]
    # Rows with "close_contacts" in "redcap_repeat_instrument" column and columns that
    # contain only missing values removed.
    contacts_df = df.loc[df["redcap_repeat_instrument"] == "close_contacts"].dropna(
        axis=1, how="all"
    )
    if len(contacts_df.index) > 0:
        contacts_df["parent_type"] = "patient"
        contacts_df["parent_external_id"] = contacts_df.apply(
            lambda row: cases_df.loc[
                cases_df["record_id"] == row["record_id"], "external_id"
            ].values[0],
            axis=1,
        )
        contacts_df["external_id"] = contacts_df.apply(
            lambda row: (
                f"{row['parent_external_id']}:"
                "redcap_repeat_instance:"
                f"{row['redcap_repeat_instance']}"
            ),
            axis=1,
        )
        contacts_df = contacts_df.drop(
            columns=["redcap_repeat_instrument", "redcap_repeat_instance"]
        )
    return cases_df, contacts_df


def upload_complete_records(
    cases_df, commcare_api_key, commcare_project_name, commcare_user_name
):
    """Drops all rows with any missing values and uploads the remainder to CommCare."""
    complete_records = cases_df.dropna()
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
    cases_df, commcare_api_key, commcare_project_name, commcare_user_name
):
    """
    To avoid overwriting existing data in CommCare with blank values,
    iterate over the incomplete records one by one, drop any blank/null values
    before uploading to CommCare.
    """
    incomplete_records = cases_df[cases_df.isna().any(axis=1)]
    for index, row in incomplete_records.iterrows():
        # Drops any values in this Series with missing/NA values,
        # and converts it back to a DataFrame.
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
