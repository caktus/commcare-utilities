import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from cc_utilities.constants import (
    REDCAP_INTEGRATION_STATUS,
    REDCAP_INTEGRATION_STATUS_REASON,
    REDCAP_INTEGRATION_STATUS_TIMESTAMP,
    REDCAP_REJECTED_PERSON,
)
from cc_utilities.redcap_sync import (
    add_integration_status_columns,
    collapse_checkbox_columns,
    collapse_housing_fields,
    handle_cdms_matching,
    normalize_phone_cols,
    select_records_by_cdms_matches,
    set_external_id_column,
    split_complete_and_incomplete_records,
    upload_complete_records,
    upload_incomplete_records,
)

FAKE_TIME = datetime.datetime(2020, 3, 14, 15, 9, 26)


@pytest.fixture
def patch_datetime_now(monkeypatch):
    class mock_datetime:
        @classmethod
        def now(cls):
            return FAKE_TIME

    monkeypatch.setattr(datetime, "datetime", mock_datetime)
    monkeypatch.setattr("cc_utilities.redcap_sync.datetime", mock_datetime)


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


def test_collapse_housing_fields():
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3", "4"],
            "cdms_id": ["1111", None, "3333", None],
            "housing_1": ["other", "somethin", None, "value"],
            "housing_2": ["senior", None, None, None],
            "housing": [None, None, None, None],
        },
        index=[1, 2, 3, 4],
    )
    expected_output_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3", "4"],
            "cdms_id": ["1111", None, "3333", None],
            "housing": ["senior", "somethin", None, "value"],
        },
        index=[1, 2, 3, 4],
    )
    output_df = collapse_housing_fields(input_df)
    pd.testing.assert_frame_equal(expected_output_df, output_df)


def test_select_records_by_cdms_matches():
    """
    Given a dictionary containing external IDs from CDMS based on values
    that matched on both the external ID and DOB fields, select_records_by_cdms_matches
    should return two DataFrames by selecting the matched / mismatched rows.
    """
    external_id_col = "cdms_id"
    input_redcap_records = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "cdms_id": ["1111", "2222", "3333"],
            "dob": ["2001-01-01", "1953-03-17", "1933-02-04"],
            "other_stuff": ["some", "more", "values"],
        },
        index=[1, 2, 3],
    )
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "cdms_id": ["1111", "2222", "3333"],
            "external_id": ["1111", "2222", "3333"],
            "dob": ["2001-01-01", "1953-03-17", "1933-02-04"],
            "other_stuff": ["some", "more", "values"],
        },
        index=[1, 2, 3],
    )
    expected_matching_df = pd.DataFrame(
        {
            "record_id": ["1", "3"],
            "cdms_id": ["1111", "3333"],
            "external_id": ["1111", "3333"],
            "dob": ["2001-01-01", "1933-02-04"],
            "other_stuff": ["some", "values"],
        },
        index=[1, 3],
    )
    expected_not_matching_df = pd.DataFrame(
        {
            "record_id": ["2"],
            "cdms_id": ["2222"],
            "dob": ["1953-03-17"],
            "other_stuff": ["more"],
        },
        index=[2],
    )
    matching_ids = [{external_id_col: "1111"}, {external_id_col: "3333"}]
    matching_records, unmatching_records = select_records_by_cdms_matches(
        input_df,
        input_redcap_records,
        matched_external_ids=matching_ids,
        external_id_col=external_id_col,
    )
    pd.testing.assert_frame_equal(matching_records, expected_matching_df)
    pd.testing.assert_frame_equal(unmatching_records, expected_not_matching_df)


def test_add_reject_status_columns(patch_datetime_now):
    dob_field = "dob"
    external_id_col = "cdms_id"
    input_df = pd.DataFrame({"record_id": ["1", "2", "3"]}, index=[1, 2, 3])
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    reason = f"mismatched {dob_field} and {external_id_col}"
    expected_output_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "integration_status": [REDCAP_REJECTED_PERSON for i in range(3)],
            "integration_status_timestamp": [timestamp for i in range(3)],
            "integration_status_reason": [reason for i in range(3)],
        },
        index=[1, 2, 3],
    )
    output_df = add_integration_status_columns(
        input_df, status=REDCAP_REJECTED_PERSON, reason=reason
    )
    pd.testing.assert_frame_equal(expected_output_df, output_df)


def test_handle_cdms_matching(patch_datetime_now):
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "cdms_id": ["1111", "2222", "3333"],
            "external_id": ["1111", "2222", "3333"],
            "dob": [None, "1953-03-17", "1933-02-04"],
            "other_stuff": ["some", "more", "values"],
        },
        index=[1, 2, 3],
    )
    input_redcap_records = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "cdms_id": ["1111", "2222", "3333"],
            "dob": [None, "1953-03-17", "1933-02-04"],
            "other_stuff": ["some", "more", "values"],
        },
        index=[1, 2, 3],
    )

    # Expect handle_cdms_matching to return the matching records.
    expected_matched_cdms_ids = [{"cdms_id": "2222"}]
    expected_output_df = pd.DataFrame(
        {
            "record_id": ["2"],
            "cdms_id": ["2222"],
            "external_id": ["2222"],
            "dob": ["1953-03-17"],
            "other_stuff": ["more"],
        },
        index=[2],
    )

    # Expect import_records_to_redcap to be called with the following data.
    expected_error_status_columns = {
        REDCAP_INTEGRATION_STATUS: [REDCAP_REJECTED_PERSON for i in range(2)],
        REDCAP_INTEGRATION_STATUS_TIMESTAMP: [
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") for i in range(2)
        ],
        REDCAP_INTEGRATION_STATUS_REASON: [
            "mismatched dob and cdms_id" for i in range(2)
        ],
    }
    expected_unmatched_records = pd.DataFrame(
        {"record_id": ["1", "3"], **expected_error_status_columns}, index=[1, 3]
    )

    with patch(
        "cc_utilities.redcap_sync.get_matching_cdms_patients",
        return_value=expected_matched_cdms_ids,
    ) as mock_get_matching_cdms_patients:
        with patch(
            "cc_utilities.redcap_sync.import_records_to_redcap"
        ) as mock_import_records_to_redcap:
            output = handle_cdms_matching(
                input_df,
                input_redcap_records,
                db_url="test",
                external_id_col="cdms_id",
                redcap_api_url="test",
                redcap_api_key="test",
            )

    mock_get_matching_cdms_patients.assert_called_once()
    mock_import_records_to_redcap.assert_called_once()
    pd.testing.assert_frame_equal(
        mock_import_records_to_redcap.call_args[0][0], expected_unmatched_records
    )
    pd.testing.assert_frame_equal(output, expected_output_df)
