from collections import defaultdict

import pandas as pd

from .logger import logger


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
                # If REDCap value is equal to "1", return cc_value,
                # else return False (to be filtered out by filter()).
                filter(
                    bool,
                    [
                        cc_value
                        if pd.notnull(row[redcap_col])
                        and row[redcap_col].strip() == "1"
                        else False
                        for redcap_col, cc_value in source_val_list
                    ],
                )
            ),
            axis=1,
        )
        # Remove the obsolete columns
        redcap_cols = [col for col, _ in source_val_list]
        logger.info(f"Dropping columns {redcap_cols} from df")
        df.drop(columns=redcap_cols, inplace=True)
    return df


def split_cases_and_contacts(df, external_id_col):
    """
    Splits a single dataframe of cases and contacts in two, based on the values in the
    "redcap_repeat_instrument" column, and assigns the columns necessary for import
    to CommCare. `external_id_col` is the name of the REDCap column that should be assigned
    to the external_id property in CommCare.
    """
    required_cols = [
        external_id_col,
        "redcap_repeat_instrument",
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
    contacts_df["parent_type"] = "patient"
    contacts_df["parent_external_id"] = contacts_df.apply(
        lambda row: cases_df.loc[
            cases_df["record_id"] == row["record_id"], "external_id"
        ].values[0],
        axis=1,
    )
    contacts_df["external_id"] = contacts_df.apply(
        lambda row: f"{row['parent_external_id']}:redcap_repeat_instance:{row['redcap_repeat_instance']}",
        axis=1,
    )
    contacts_df.drop(
        columns=["redcap_repeat_instrument", "redcap_repeat_instance"], inplace=True
    )
    return cases_df, contacts_df
