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
    For the given external_id_col, drop any rows with no value and
    copy to a new column named "external_id"
    """
    df = df.dropna(subset=[external_id_col])
    df.loc[:, "external_id"] = df[external_id_col]
    return df


def split_complete_and_incomplete_records(cases_df):
    """
    Splits the DataFrame into two - 'complete' meaning all rows with no
    column values missing, and 'incomplete' is the remainder.
    """
    # Drop columns where all rows are missing data.
    cases_df = cases_df.dropna(axis=1, how="all")
    # Drop rows where any values are missing from columns.
    complete_records = cases_df.dropna()
    # The inverse; select rows where any values are missing from columns.
    incomplete_records = cases_df[cases_df.isna().any(axis=1)]
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
