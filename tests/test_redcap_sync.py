import numpy as np
import pandas as pd

from cc_utilities.redcap_sync import collapse_checkbox_columns, split_cases_and_contacts


def test_collapse_checkbox_columns():
    df = pd.DataFrame(
        {
            "box1___yellow": [1, None, 1],
            "box1___green": [None, 1, 1],
            "box1___blue": [None, None, None],
            "box1___other": [1, None, None],
            "box1__other": ["test", "", ""],
        }
    )
    expected_df = pd.DataFrame(
        {
            "box1__other": ["test", "", ""],
            "box1": ["yellow other", "green", "yellow green"],
        }
    )
    new_df = collapse_checkbox_columns(df)
    pd.testing.assert_frame_equal(expected_df, new_df)


def test_split_cases_and_contacts():
    df = pd.DataFrame(
        {
            "record_id": [1, 2, 2, 3, 3],
            "redcap_repeat_instrument": [
                None,
                None,
                "close_contacts",
                np.nan,
                "close_contacts",
            ],
            "redcap_repeat_instance": pd.array([None, None, 1, None, 1], dtype="Int64"),
            # Int64 (capital I) is the nullable integer type:
            # https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
            "cdms_id": pd.array([1234, 1234, None, 1234, None], dtype="Int64"),
            "arbitrary": ["some", "arbitrary", "values", 2, "test"],
            "empty_col": [None, None, None, np.nan, np.nan],
        },
        index=[1, 2, 3, 4, 5],
    )
    expected_cases_df = pd.DataFrame(
        {
            "record_id": [1, 2, 3],
            "cdms_id": pd.array([1234, 1234, 1234], dtype="Int64"),
            "arbitrary": ["some", "arbitrary", 2],
            "external_id": pd.array([1234, 1234, 1234], dtype="Int64"),
        },
        index=[1, 2, 4],
    )
    expected_contacts_df = pd.DataFrame(
        {
            "record_id": [2, 3],
            "arbitrary": ["values", "test"],
            "parent_type": ["patient", "patient"],
            "parent_external_id": [1234, 1234],
            "external_id": ["REDCAP-2-1", "REDCAP-3-1"],
        },
        index=[3, 5],
    )
    cases_df, contacts_df = split_cases_and_contacts(df)
    pd.testing.assert_frame_equal(expected_cases_df, cases_df)
    pd.testing.assert_frame_equal(
        expected_contacts_df, contacts_df, check_index_type=False
    )


def test_split_cases_and_contacts_no_cdms_id():
    df = pd.DataFrame(
        {
            "record_id": [1, 2, 2, 3, 3],
            "redcap_repeat_instrument": [
                None,
                None,
                "close_contacts",
                np.nan,
                "close_contacts",
            ],
            "redcap_repeat_instance": pd.array([None, None, 1, None, 1], dtype="Int64"),
            # cdms_id missing
            "arbitrary": ["some", "arbitrary", "values", 2, "test"],
            "empty_col": [None, None, None, np.nan, np.nan],
        },
        index=[1, 2, 3, 4, 5],
    )
    expected_cases_df = pd.DataFrame(
        {
            "record_id": [1, 2, 3],
            # cdms_id missing
            "arbitrary": ["some", "arbitrary", 2],
            "external_id": ["REDCAP-1", "REDCAP-2", "REDCAP-3"],
        },
        index=[1, 2, 4],
    )
    expected_contacts_df = pd.DataFrame(
        {
            "record_id": [2, 3],
            "arbitrary": ["values", "test"],
            "parent_type": ["patient", "patient"],
            "parent_external_id": ["REDCAP-2", "REDCAP-3"],
            "external_id": ["REDCAP-2-1", "REDCAP-3-1"],
        },
        index=[3, 5],
    )
    cases_df, contacts_df = split_cases_and_contacts(df)
    pd.testing.assert_frame_equal(expected_cases_df, cases_df)
    pd.testing.assert_frame_equal(
        expected_contacts_df, contacts_df, check_index_type=False
    )
