import logging
from collections import defaultdict

import pandas as pd

logger = logging.getLogger(__name__)


def collapse_checkbox_columns(df):
    """
    Converts REDCap-style checkbox columns ("box1___yellow", "box1___green") to CommCare-style
    checkbox files ("box1": "yellow green"). See test for details.
    """
    df = df.copy()
    # First, collect the columns that need to be collapsed, in case they don't
    # all appear together.
    cc_properties = defaultdict(list)
    for redcap_col in [col for col in df.columns if "___" in col]:
        cc_property, cc_value = redcap_col.split("___", 1)
        cc_properties[cc_property].append((redcap_col, cc_value))
    # Add new column with the checkbox values collapsed into a single column
    for cc_property, source_val_list in cc_properties.items():
        logger.info(f"Adding column {cc_property} to df")
        df[cc_property] = df.apply(
            lambda row: " ".join(
                # If REDCap value is not NaN or None and is truthy, return cc_value,
                # else return False (to be filtered out by filter()).
                filter(
                    bool,
                    [
                        cc_value
                        if pd.notnull(row[redcap_col]) and row[redcap_col]
                        else False
                        for redcap_col, cc_value in source_val_list
                    ],
                )
            ),
            axis=1,
        )
        # Remove the obsolete columns
        for redcap_col, _ in source_val_list:
            logger.info(f"Dropping column {redcap_col} from df")
            df.drop(columns=redcap_col, inplace=True)
    return df


def split_cases_and_contacts(df):
    """
    Splits a single dataframe of cases and contacts in two, based on the values in the
    "redcap_repeat_instrument" column, and assigns the columns necessary for import
    to CommCare.
    """
    # Rows with null value in "redcap_repeat_instrument" column and columns that contain
    # only missing values removed.
    cases_df = df.loc[df["redcap_repeat_instrument"].isnull()].dropna(axis=1, how="all")
    if "cdms_id" in cases_df.columns:
        cases_df["external_id"] = cases_df["cdms_id"]
    else:
        # No cdms_id column added in REDCap yet; generate one for testing purposes.
        logger.warning(
            "Using REDCap record_id for external_id! Future uploads may duplicate cases."
        )
        cases_df["external_id"] = cases_df.apply(
            lambda row: f"REDCAP-{row['record_id']}", axis=1
        )
    # Rows with "close_contacts" in "redcap_repeat_instrument" column and columns that
    # contain only missing values removed.
    contacts_df = df.loc[df["redcap_repeat_instrument"] == "close_contacts"].dropna(
        axis=1, how="all"
    )
    contacts_df["parent_type"] = "patient"
    contacts_df["parent_external_id"] = contacts_df.apply(
        lambda row: cases_df.loc[
            cases_df["record_id"] == row["record_id"], "external_id"
        ].values[0],
        axis=1,
    )
    contacts_df["external_id"] = contacts_df.apply(
        lambda row: f"REDCAP-{row['record_id']}-{row['redcap_repeat_instance']}", axis=1
    )
    contacts_df.drop(
        ["redcap_repeat_instrument", "redcap_repeat_instance"], axis=1, inplace=True
    )
    return cases_df, contacts_df
