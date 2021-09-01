import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from cc_utilities.constants import (
    ACCEPTED_INTERVIEW_DISPOSITION_VALUES,
    REDCAP_INTEGRATION_STATUS,
    REDCAP_INTEGRATION_STATUS_REASON,
    REDCAP_INTEGRATION_STATUS_TIMESTAMP,
    REDCAP_REJECTED_PERSON,
)
from cc_utilities.redcap_sync import (
    add_integration_status_columns,
    collapse_checkbox_columns,
    collapse_housing_fields,
    drop_external_ids_not_in_cdms,
    get_commcare_cases_with_acceptable_interview_dispositions,
    get_records_matching_dob,
    handle_cdms_matching,
    normalize_phone_cols,
    reject_records_already_filled_out_by_case_investigator,
    set_external_id_column,
    split_complete_and_incomplete_records,
    split_records_by_accepted_external_ids,
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
    # phone3 doesn't exist and shouldn't cause a hard failure
    output_df = normalize_phone_cols(input_df, ["phone1", "phone3"])
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


def test_drop_external_ids_not_in_cdms():
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "cdms_id": ["1111", "2222", "3333"],
            "dob": ["1978-10-01", "1953-03-17", "1933-02-04"],
            "other_stuff": ["some", "more", "values"],
        },
        index=[1, 2, 3],
    )
    cdms_patients_data = [
        {"cdms_id": "1111", "dob": "1978-10-01"},
        {"cdms_id": "2222", "dob": "1990-11-08"},
    ]
    expected_output_df = pd.DataFrame(
        {
            "record_id": ["1", "2"],
            "cdms_id": ["1111", "2222"],
            "dob": ["1978-10-01", "1953-03-17"],
            "other_stuff": ["some", "more"],
        },
        index=[1, 2],
    )
    output = drop_external_ids_not_in_cdms(
        input_df, external_id_col="cdms_id", cdms_patients_data=cdms_patients_data
    )
    pd.testing.assert_frame_equal(output, expected_output_df)


def test_get_records_matching_dob():
    external_id_col = "cdms_id"
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3"],
            "cdms_id": ["1111", "2222", "3333"],
            "dob": ["1978-10-01", "1953-03-17", "1933-02-04"],
            "other_stuff": ["some", "more", "values"],
        },
        index=[1, 2, 3],
    )
    cdms_patients_data = [
        {"cdms_id": "1111", "dob": "1978-10-01"},
        {"cdms_id": "2222", "dob": "1990-11-08"},
    ]
    expected_accepted_external_ids = ["1111"]
    accepted_external_ids = get_records_matching_dob(
        df=input_df,
        external_id_col=external_id_col,
        cdms_patients_data=cdms_patients_data,
    )
    assert expected_accepted_external_ids == accepted_external_ids


def test_split_records_by_accepted_external_ids():
    """
    Given a dictionary containing external IDs from CDMS based on values
    that matched on both the external ID and DOB fields, split_records_by_accepted_external_ids
    should return two DataFrames by selecting the matched / mismatched rows.
    """
    external_id_col = "cdms_id"
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
            "external_id": ["2222"],
            "dob": ["1953-03-17"],
            "other_stuff": ["more"],
        },
        index=[2],
    )
    matching_ids = ["1111", "3333"]
    accept_records, reject_records = split_records_by_accepted_external_ids(
        input_df, accepted_external_ids=matching_ids, external_id_col=external_id_col
    )
    pd.testing.assert_frame_equal(accept_records, expected_matching_df)
    pd.testing.assert_frame_equal(reject_records, expected_not_matching_df)


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


@patch("cc_utilities.redcap_sync.import_records_to_redcap")
@patch("cc_utilities.redcap_sync.query_cdms_for_external_ids_and_dobs")
def test_handle_cdms_matching(
    mock_query_cdms_for_external_ids_and_dobs,
    mock_import_records_to_redcap,
    patch_datetime_now,
):
    """
    Given the list of external IDs returned after comparing
    CDMS IDs and DOBs, assert that handle_cdms_matching properly
    handles accepted and rejected records; sending the rejected
    records back to redcap with record_ids and integration status,
    and returns the accepted records.
    """
    mock_cdms_patients_data = [
        {"cdms_id": "2222", "dob": "1953-03-17"},  # a fully matching record
        {"cdms_id": "3333", "dob": "2020-01-01"},  # a mismatch on dob
        # one record left out to be ignored by drop_external_ids_not_in_cdms
    ]
    mock_query_cdms_for_external_ids_and_dobs.return_value = mock_cdms_patients_data

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
    # Expect handle_cdms_matching to return the matching records.
    expected_accepted_records = pd.DataFrame(
        {
            "record_id": ["2"],
            "cdms_id": ["2222"],
            "external_id": ["2222"],
            "dob": ["1953-03-17"],
            "other_stuff": ["more"],
        },
        index=[2],
    )

    # Expect import_records_to_redcap to be called with integration status data.
    expected_error_status_columns = {
        REDCAP_INTEGRATION_STATUS: [REDCAP_REJECTED_PERSON],
        REDCAP_INTEGRATION_STATUS_TIMESTAMP: [
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ],
        REDCAP_INTEGRATION_STATUS_REASON: ["mismatched dob and cdms_id"],
    }
    expected_reject_records = pd.DataFrame(
        {"record_id": ["3"], **expected_error_status_columns}, index=[3]
    )

    output = handle_cdms_matching(
        input_df,
        db_url="test",
        external_id_col="cdms_id",
        redcap_api_url="test",
        redcap_api_key="test",
    )

    mock_query_cdms_for_external_ids_and_dobs.assert_called_once()
    mock_import_records_to_redcap.assert_called_once()
    pd.testing.assert_frame_equal(
        mock_import_records_to_redcap.call_args[0][0], expected_reject_records
    )
    pd.testing.assert_frame_equal(output, expected_accepted_records)


def test_get_commcare_cases_with_acceptable_interview_dispositions():
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3", "4"],
            "cdms_id": ["1111", "2222", "3333", "4444"],
            "external_id": ["1111", "2222", "3333", "4444"],
            "dob": [None, "1953-03-17", "1933-02-04", None],
            "other_stuff": ["some", "more", "values", None],
        },
        index=[1, 2, 3, 4],
    )

    case_mocks = [
        [
            {
                "properties": {
                    "interview_disposition": ACCEPTED_INTERVIEW_DISPOSITION_VALUES[0]
                }
            }
        ],
        [{"properties": {"interview_disposition": "unacceptable"}}],
        None,
        [{"properties": {"other": ""}}],
    ]
    expected_accepted_external_ids = ["1111"]
    with patch(
        "cc_utilities.redcap_sync.get_commcare_cases_by_external_id_with_backoff",
        side_effect=case_mocks,
    ) as mock_get_commcare_cases_by_external_id_with_backoff:
        accepted_external_ids = get_commcare_cases_with_acceptable_interview_dispositions(
            input_df, "cdms_id", "test_key", "test_user_name", "test_project"
        )
    assert mock_get_commcare_cases_by_external_id_with_backoff.call_count == len(
        input_df
    )
    assert accepted_external_ids == expected_accepted_external_ids


def test_reject_records_already_filled_out_by_case_investigator():
    input_df = pd.DataFrame(
        {
            "record_id": ["1", "2", "3", "4"],
            "cdms_id": ["1111", "2222", "3333", "4444"],
            "external_id": ["1111", "2222", "3333", "4444"],
            "dob": [None, "1953-03-17", "1933-02-04", None],
            "other_stuff": ["some", "more", "values", None],
        },
        index=[1, 2, 3, 4],
    )

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    reason = "Case already submitted by a Case Investigator."
    expected_reject_records = pd.DataFrame(
        {
            "record_id": ["3", "4"],
            "integration_status": ["rejected_person" for i in range(2)],
            "integration_status_timestamp": [timestamp for i in range(2)],
            "integration_status_reason": [reason for i in range(2)],
        },
        index=[3, 4],
    )

    mock_accepted_external_ids = ["1111", "2222"]
    expected_accepted_records = pd.DataFrame(
        {
            "record_id": ["1", "2"],
            "cdms_id": ["1111", "2222"],
            "external_id": ["1111", "2222"],
            "dob": [None, "1953-03-17"],
            "other_stuff": ["some", "more"],
        },
        index=[1, 2],
    )
    with patch(
        "cc_utilities.redcap_sync.get_commcare_cases_with_acceptable_interview_dispositions",
        return_value=mock_accepted_external_ids,
    ) as mock_get_commcare_cases_with_acceptable_interview_dispositions:
        with patch(
            "cc_utilities.redcap_sync.import_records_to_redcap"
        ) as mock_import_records_to_redcap:
            accepted_records = reject_records_already_filled_out_by_case_investigator(
                input_df,
                "cdms_id",
                "project_slug",
                "cc_user_name",
                "cc_api_key",
                "redcap_api_url",
                "redcap_api_key",
            )
    mock_get_commcare_cases_with_acceptable_interview_dispositions.assert_called_once()
    mock_import_records_to_redcap.assert_called_once()
    pd.testing.assert_frame_equal(
        mock_import_records_to_redcap.call_args[0][0], expected_reject_records
    )
    pd.testing.assert_frame_equal(accepted_records, expected_accepted_records)
