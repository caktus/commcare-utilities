from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from cc_utilities.redcap_sync import (
    collapse_checkbox_columns,
    normalize_phone_cols,
    split_cases_and_contacts,
    upload_complete_records,
    upload_incomplete_records,
)


def test_collapse_checkbox_columns():
    input_df = pd.DataFrame(
        {
            "box1___yellow": ["1", "0", "1"],
            "box1___green": [None, "1", "1"],
            "box1___blue": [None, "0", None],
            "box1___other": ["1", None, "0"],
            "box2___red": ["0", "0", "0"],
            "box1__other": ["test", "", ""],
        }
    )
    expected_output_df = pd.DataFrame(
        {
            "box1__other": ["test", "", ""],
            "box1": ["yellow other", "green", "yellow green"],
            "box2": [None, None, None],
        }
    )
    output_df = collapse_checkbox_columns(input_df)
    pd.testing.assert_frame_equal(expected_output_df, output_df)


def test_normalize_phone_cols():
    input_df = pd.DataFrame(
        {
            "phone1": ["(919) 555-1212", "9195551213", float("nan")],
            "phone2": ["(919) 555-1215", "9195551216", float("nan")],
        }
    )
    # phone1 col is normalized and with N/A converted to empty string
    expected_output_df = pd.DataFrame(
        {
            "phone1": ["9195551212", "9195551213", ""],
            "phone2": ["(919) 555-1215", "9195551216", float("nan")],
        }
    )
    output_df = normalize_phone_cols(input_df, ["phone1"])
    pd.testing.assert_frame_equal(expected_output_df, output_df)


@patch("cc_utilities.redcap_sync.upload_data_to_commcare")
def test_upload_complete_records(mock_upload_to_commcare):
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3", "4"],
            "cdms_id": ["1111", "2222", "3333", "4444"],
            "arbitrary": ["some", "arbitrary", None, np.NAN],
            "stuff": ["some", "more", "values", "test"],
            "external_id": ["1111", "2222", "3333", "4444"],
        },
        index=[1, 2, 3, 4],
    )
    expected_complete_records = pd.DataFrame(
        {
            "record_id": ["1", "2"],
            "cdms_id": ["1111", "2222"],
            "arbitrary": ["some", "arbitrary"],
            "stuff": ["some", "more"],
            "external_id": ["1111", "2222"],
        },
        index=[1, 2],
    )
    upload_complete_records(input_df, "api-key", "project-name", "username")
    uploaded_dataframe = mock_upload_to_commcare.call_args[0][0]
    pd.testing.assert_frame_equal(uploaded_dataframe, expected_complete_records)


@patch("cc_utilities.redcap_sync.upload_data_to_commcare")
def test_upload_incomplete_records(mock_upload_to_commcare):
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3", "4"],
            "cdms_id": ["1111", "2222", "3333", "4444"],
            "arbitrary": ["some", "arbitrary", None, np.NAN],
            "stuff": ["some", "more", "values", "test"],
            "external_id": ["1111", "2222", "3333", "4444"],
        },
        index=[1, 2, 3, 4],
    )
    expected_incomplete_records = [
        pd.DataFrame(
            {
                "record_id": ["3"],
                "cdms_id": ["3333"],
                "stuff": ["values"],
                "external_id": ["3333"],
            },
            index=[3],
        ),
        pd.DataFrame(
            {
                "record_id": ["4"],
                "cdms_id": ["4444"],
                "stuff": ["test"],
                "external_id": ["4444"],
            },
            index=[4],
        ),
    ]
    upload_incomplete_records(input_df, "api-key", "project-name", "username")
    assert mock_upload_to_commcare.call_count == 2
    pd.testing.assert_frame_equal(
        mock_upload_to_commcare.call_args_list[0][0][0], expected_incomplete_records[0]
    )
    pd.testing.assert_frame_equal(
        mock_upload_to_commcare.call_args_list[1][0][0], expected_incomplete_records[1]
    )


@pytest.mark.skip(
    reason="split_cases_and_contacts is unused and relies on old/outdated data"
)
def test_split_cases_and_contacts():
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "2", "3", "3"],
            "redcap_repeat_instrument": [
                None,
                None,
                "close_contacts",
                np.nan,
                "close_contacts",
            ],
            "redcap_repeat_instance": [None, None, "1", None, "1"],
            # Int64 (capital I) is the nullable integer type:
            # https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
            "cdms_id": ["1234", "1234", None, "1234", None],
            "arbitrary": ["some", "arbitrary", "values", "2", "test"],
            "empty_col": [None, None, None, np.nan, np.nan],
        },
        index=[1, 2, 3, 4, 5],
    )
    expected_output_cases_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "cdms_id": ["1234", "1234", "1234"],
            "arbitrary": ["some", "arbitrary", "2"],
            "external_id": ["1234", "1234", "1234"],
        },
        index=[1, 2, 4],
    )
    expected_output_contacts_df = pd.DataFrame(
        {
            "record_id": ["2", "3"],
            "arbitrary": ["values", "test"],
            "parent_type": ["patient", "patient"],
            "parent_external_id": ["1234", "1234"],
            "external_id": [
                "1234:redcap_repeat_instance:1",
                "1234:redcap_repeat_instance:1",
            ],
        },
        index=[3, 5],
    )
    cases_output_df, contacts_output_df = split_cases_and_contacts(
        input_df, external_id_col="cdms_id"
    )
    pd.testing.assert_frame_equal(expected_output_cases_df, cases_output_df)
    pd.testing.assert_frame_equal(expected_output_contacts_df, contacts_output_df)


@pytest.mark.skip(
    reason="split_cases_and_contacts is unused and relies on old/outdated data"
)
def test_split_cases_and_contacts_no_cdms_id():
    with pytest.raises(ValueError):
        split_cases_and_contacts(pd.DataFrame({}), external_id_col="cdms_id")


@pytest.mark.skip(
    reason="split_cases_and_contacts is unused and relies on old/outdated data"
)
def test_split_cases_and_contacts_no_contacts():
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "redcap_repeat_instrument": pd.Series([None, None, np.nan], dtype="object"),
            "redcap_repeat_instance": pd.Series([None, None, None], dtype="object"),
            "cdms_id": ["1234", "1234", "1234"],
            "arbitrary": ["some", "arbitrary", "2"],
            "empty_col": pd.Series([None, None, np.nan], dtype="object"),
        },
        index=[1, 2, 3],
    )
    expected_output_cases_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "cdms_id": ["1234", "1234", "1234"],
            "arbitrary": ["some", "arbitrary", "2"],
            "external_id": ["1234", "1234", "1234"],
        },
        index=[1, 2, 3],
    )
    expected_output_contacts_df = pd.DataFrame(
        {}, index=pd.Int64Index([], dtype="int64"),
    )
    cases_output_df, contacts_output_df = split_cases_and_contacts(
        input_df, external_id_col="cdms_id"
    )
    pd.testing.assert_frame_equal(expected_output_cases_df, cases_output_df)
    pd.testing.assert_frame_equal(expected_output_contacts_df, contacts_output_df)
