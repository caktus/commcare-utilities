from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from cc_utilities.redcap_sync import (
    collapse_checkbox_columns,
    normalize_phone_cols,
    set_external_id_column,
    split_complete_and_incomplete_records, upload_complete_records,
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


def test_split_complete_and_incomplete_records():
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3", "4"],
            "cdms_id": ["1111", "2222", "3333", "4444"],
            "arbitrary": ["some", "arbitrary", None, "test"],
            "stuff": ["some", "more", "values", np.NAN],
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
    expected_incomplete_records = pd.DataFrame(
        {
            "record_id": ["3", "4"],
            "cdms_id": ["3333", "4444"],
            "arbitrary": [None, "test"],
            "stuff": ["values", np.NAN],
            "external_id": ["3333", "4444"],
        },
        index=[3, 4],
    )
    complete_records, incomplete_records = split_complete_and_incomplete_records(
        input_df
    )
    pd.testing.assert_frame_equal(complete_records, expected_complete_records)
    pd.testing.assert_frame_equal(incomplete_records, expected_incomplete_records)


@patch("cc_utilities.redcap_sync.upload_data_to_commcare")
def test_upload_complete_records(mock_upload_to_commcare):
    input_df = pd.DataFrame(
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
    mock_upload_to_commcare.assert_called_once()
    uploaded_dataframe = mock_upload_to_commcare.call_args[0][0]
    pd.testing.assert_frame_equal(uploaded_dataframe, input_df)

@patch("cc_utilities.redcap_sync.upload_data_to_commcare")
def test_upload_incomplete_records(mock_upload_to_commcare):
    input_df = pd.DataFrame(
        {
            "record_id": ["3", "4"],
            "cdms_id": ["3333", "4444"],
            "stuff": ["values", "test"],
            "external_id": ["3333", "4444"],
        },
        index=[3, 4],
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


def test_set_external_id_column():
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3", "4"],
            "cdms_id": ["1111", None, "3333", None],
            "arbitrary": ["some", "arbitrary", None, np.NAN],
            "stuff": ["some", "more", "values", "test"],
        },
        index=[1, 2, 3, 4],
    )
    expected_output_df = pd.DataFrame(
        {
            "record_id": ["1", "3"],
            "cdms_id": ["1111", "3333"],
            "arbitrary": ["some", None],
            "stuff": ["some", "values"],
            "external_id": ["1111", "3333"],
        },
        index=[1, 3],
    )
    output = set_external_id_column(input_df, external_id_col="cdms_id")
    pd.testing.assert_frame_equal(expected_output_df, output)
