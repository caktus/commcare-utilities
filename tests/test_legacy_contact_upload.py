import csv
import glob
import random
import tempfile
from pathlib import PurePath
from uuid import uuid4

import pandas as pd
import pytest
from faker import Faker

from cc_utilities.command_line.bulk_upload_legacy_contact_data import (
    FINAL_REPORT_FILE_NAME_PART,
    VALIDATION_REPORT_FILE_NAME_PART,
    main_with_args,
)
from cc_utilities.legacy_upload import (
    MAX_CONTACTS_PER_PARENT_PATIENT,
    create_dummy_patient_case_data,
    generate_commcare_contact_data,
    upload_legacy_contacts_to_commcare,
    validate_case_data_columns,
    validate_legacy_case_data,
)

Faker.seed(0)
fake = Faker("en_US")


CONTACT_DATA_DICT = {
    # fmt: off
    "first_name": {
        "required": True,
        "allowed_values": [],
        "data_type": "plain",
    },
    "dob": {
        "required": False,
        "allowed_values": [],
        "data_type": "date",
    },
    # fmt: on
    "phone_work": {
        "required": False,
        "allowed_values": [],
        "data_type": "phone_number",
    },
    "days_symptoms_lasted": {
        "required": False,
        "allowed_values": [],
        "data_type": "number",
    },
    "current_smoker": {
        "required": False,
        "allowed_values": ["yes", "no", "unknown"],
        "data_type": "select",
    },
    "symptoms_selected": {
        "required": False,
        "allowed_values": ["none", "fever", "chills", "headache"],
        "data_type": "multi_select",
    },
}


def contact_data_dict_to_list_of_dicts(data_dict=CONTACT_DATA_DICT):
    "Convenience function used in testing command line script"
    result = [{**data_dict[k], **{"field": k}} for k in data_dict]
    for row in result:
        row["allowed_values"] = ", ".join(row["allowed_values"])
        row["required"] = str(row["required"])
    return result


def make_valid_contact():
    "Create contact with randomly generated values that validate vs. CONTACT_DATA_DICT"
    return {
        "first_name": fake.first_name(),
        "dob": fake.date_of_birth(minimum_age=18, maximum_age=100).strftime("%Y/%m/%d"),
        "phone_work": fake.phone_number(),
        "days_symptoms_lasted": random.randint(2, 14),
        "current_smoker": random.choice(
            CONTACT_DATA_DICT["current_smoker"]["allowed_values"]
        ),
        "symptoms_selected": ", ".join(
            random.choices(
                CONTACT_DATA_DICT["symptoms_selected"]["allowed_values"], k=2
            )
        ),
    }


def make_legacy_contacts_data(num=10):
    "Create `num` number of valid contacts"
    return [make_valid_contact() for i in range(num)]


def test_create_dummy_patient_case_data():
    """Show that `create_dummy_patient_case_data` returns expected schema.

    This function is responsible for generating specific key/value pairs that
    CommCare needs to see for dummy patients, so we test this function in isolation.
    """
    external_id = "foobarbizzbang"
    expectations = {
        "external_id": external_id,
        "case_id": None,
        "stub": "yes",
        "name": "(no index case)",
        "stub_type": "contact_without_index",
    }
    actual = create_dummy_patient_case_data(external_id)
    assert set(expectations.keys()) == set(actual.keys())
    assert set(expectations.values()) == set(actual.values())


def test_generate_commcare_contact_data():
    """Show that `generate_commcare_contact_data` returns expected schema.

    This function is responsible for generating specific key/value pairs that
    CommCare needs to see for uploaded legacy contacts, so we test this function in
    isolation.
    """
    patient_id = "abcdef123"
    expectation = {
        "parent_type": "patient",
        "parent_id": patient_id,
        "ooj": "no",
        "case_id": None,
    }
    minimal_validated_contact_data = {"contact_id": "someID"}
    actual = generate_commcare_contact_data(minimal_validated_contact_data, patient_id)
    assert set(expectation.keys()).issubset(set(actual.keys()))
    assert all([actual[k] == expectation[k] for k in expectation])


class TestCaseDataValidationLogic:
    """Test logic around validating user-supplied case data"""

    def test_validate_legacy_case_data_happy_path(self):
        "Show that when given valid data, 'is_valid' is True for each row"
        df = validate_legacy_case_data(
            pd.DataFrame(make_legacy_contacts_data()), CONTACT_DATA_DICT
        )
        assert df["is_valid"].all()

    def test_validate_legacy_case_data_invalid_date(self):
        """Show that case data rows with invalid date value are invalid...

        and row["validation_problems"] has info about the offending column(s)
        """
        change_row = 0
        date_col = "dob"
        data = make_legacy_contacts_data()
        data[change_row][date_col] = "this-is-not-a-date"
        df = pd.DataFrame(data)
        df = validate_legacy_case_data(df, CONTACT_DATA_DICT)
        assert ~df.iloc[change_row]["is_valid"]
        assert date_col in df.iloc[change_row]["validation_problems"]

    def test_validate_legacy_case_data_invalid_number(self):
        """Show that case data rows with invalid number value are invalid...

        and row["validation_problems"] has info about the offending column(s)
        """
        change_row = 0
        num_col = "days_symptoms_lasted"
        data = make_legacy_contacts_data()
        data[change_row][num_col] = "this is not a number"
        df = pd.DataFrame(data)
        df = validate_legacy_case_data(df, CONTACT_DATA_DICT)
        assert ~df.iloc[change_row]["is_valid"]
        assert num_col in df.iloc[change_row]["validation_problems"]

    def test_validate_legacy_case_data_invalid_select(self):
        """Show that case data rows with invalid number value are invalid...

        and row["validation_problems"] has info about the offending column(s)
        """
        change_row = 0
        select_col = "current_smoker"
        val = "what did you say?"
        data = make_legacy_contacts_data()
        data[change_row][select_col] = val
        assert val not in CONTACT_DATA_DICT[select_col]["allowed_values"]
        df = pd.DataFrame(data)
        df = validate_legacy_case_data(df, CONTACT_DATA_DICT)
        assert ~df.iloc[change_row]["is_valid"]
        assert select_col in df.iloc[change_row]["validation_problems"]

    def test_validate_legacy_case_data_invalid_multi_select(self):
        """Show that case data rows with invalid number value are invalid...

        and row["validation_problems"] has info about the offending column(s)
        """
        change_row = 0
        multi_select_col = "symptoms_selected"
        val = "floating"
        data = make_legacy_contacts_data()
        data[change_row][multi_select_col] = val
        assert val not in CONTACT_DATA_DICT[multi_select_col]["allowed_values"]
        df = pd.DataFrame(data)
        df = validate_legacy_case_data(df, CONTACT_DATA_DICT)
        assert ~df.iloc[change_row]["is_valid"]
        assert multi_select_col in df.iloc[change_row]["validation_problems"]

    def test_validate_legacy_case_data_missing_required_value(self):
        """Show that case data rows with missing required fields are invalid...

        and row["validation_problems"] has info about the offending column(s)
        """
        change_row_1 = 0
        change_row_2 = 1
        required_col = "first_name"
        assert CONTACT_DATA_DICT[required_col]["required"] is True
        data = make_legacy_contacts_data()
        data[change_row_1][required_col] = None
        data[change_row_2][required_col] = ""
        df = pd.DataFrame(data)
        df = validate_legacy_case_data(df, CONTACT_DATA_DICT)
        assert ~df.iloc[[change_row_1, change_row_2]]["is_valid"].all()
        assert required_col in df.iloc[change_row_1]["validation_problems"]
        assert required_col in df.iloc[change_row_2]["validation_problems"]

    def test_validate_case_data_columns_happy_path(self):
        "When given valid col names, `validate_case_data_columns` is True"
        df = pd.DataFrame(make_legacy_contacts_data())
        assert (
            validate_case_data_columns(df.columns, [col for col in CONTACT_DATA_DICT],)
            is True
        )

    def test_validate_case_data_columns_unexpected_columns(self):
        "When given unexpected col names, `validate_case_data_columns` is False"
        data = make_legacy_contacts_data()
        for row in data:
            row["uNeXpEcTeD"] = True
        df = pd.DataFrame(data)
        assert (
            validate_case_data_columns(df.columns, [col for col in CONTACT_DATA_DICT],)
            is False
        )

    def test_validate_case_data_columns_missing_req_columns(self):
        "When missing required col names, `validate_case_data_columns` is False"
        data = make_legacy_contacts_data()
        required = [
            col
            for col in CONTACT_DATA_DICT
            if CONTACT_DATA_DICT[col]["required"] is True
        ]
        for row in data:
            for col in required:
                del row[col]
        df = pd.DataFrame(data)
        assert len(required) > 0
        assert (
            validate_case_data_columns(
                df.columns,
                [col for col in CONTACT_DATA_DICT],
                required_columns=required,
            )
            is False
        )


class MockCommCareFunctions:
    """Monkeypatching monkey business to mock API responses from CommCare API...

    as called across three functions in cc_utilities.common, across which state needs
    to be simulated that would otherwise be stored on CommCareHQ.
    """

    patients = []
    get_commcare_case_called = 0

    @classmethod
    def upload_data_to_commcare(cls, data, slug, case_type, *args, **kwargs):
        if case_type == "patient" and not kwargs.get("create_new_cases"):
            for patient in data:
                case_id = uuid4().hex
                cls.patients.append(
                    dict(
                        case_id=case_id,
                        external_id=patient["external_id"],
                        child_cases={},
                    )
                )
            return
        if case_type == "patient" and kwargs.get("create_new_cases") == "off":
            return
        if case_type == "contact":
            parent_id = data[0]["parent_id"]
            patient = next(
                patient for patient in cls.patients if patient["case_id"] == parent_id
            )
            for idx, contact in enumerate(data):
                patient["child_cases"][idx] = {
                    "properties": {"contact_id": contact["contact_id"]},
                    "case_id": uuid4().hex,
                }
            return

    @classmethod
    def get_commcare_case(cls, case_id, *args, **kwargs):
        if kwargs.get("error_after_first") is True:
            cls.get_commcare_case_called += 1
        if cls.get_commcare_case_called > 1:
            raise Exception("Simulated server error")
        return next(
            patient for patient in cls.patients if patient["case_id"] == case_id
        )

    @classmethod
    def get_commcare_cases(cls, *args, **kwargs):
        return cls.patients


@pytest.fixture
def mock_upload_to_commcare(monkeypatch):
    def mock(*args, **kwargs):
        return MockCommCareFunctions.upload_data_to_commcare(*args, **kwargs)

    monkeypatch.setattr("cc_utilities.legacy_upload.upload_data_to_commcare", mock)


@pytest.fixture
def mock_get_commcare_cases(monkeypatch):
    def mock(*args, **kwargs):
        return MockCommCareFunctions.get_commcare_cases(*args, **kwargs)

    monkeypatch.setattr("cc_utilities.legacy_upload.get_commcare_cases", mock)


@pytest.fixture
def mock_get_commcare_case(monkeypatch):
    def mock(*args, **kwargs):
        return MockCommCareFunctions.get_commcare_case(*args, **kwargs)

    monkeypatch.setattr("cc_utilities.legacy_upload.get_commcare_case", mock)


@pytest.fixture
def mock_get_commcare_case_exception_after_first_call(monkeypatch):
    def mock(*args, **kwargs):
        return MockCommCareFunctions.get_commcare_case(
            error_after_first=True, *args, **kwargs
        )

    monkeypatch.setattr("cc_utilities.legacy_upload.get_commcare_case", mock)


class TestUploadLegacyContactsToCommCare:
    "Tests of the `upload_legacy_contacts_to_commcare` function"

    def test_happy_path(
        self, mock_upload_to_commcare, mock_get_commcare_case, mock_get_commcare_cases
    ):
        data = make_legacy_contacts_data(num=200)
        for contact in data:
            contact["contact_id"] = uuid4().hex

        result = upload_legacy_contacts_to_commcare(data, "slug", "username", "apikey")
        assert len(result) == len(data)
        assert set([contact["contact_id"] for contact in data]) == set(result.keys())

    def test_unhappy_path_still_returns_contacts_created_so_far(
        self,
        mock_upload_to_commcare,
        mock_get_commcare_case_exception_after_first_call,
        mock_get_commcare_cases,
    ):
        data = make_legacy_contacts_data(num=200)
        for contact in data:
            contact["contact_id"] = uuid4().hex
        result = upload_legacy_contacts_to_commcare(data, "slug", "username", "apikey")
        # our mock class is written such that it will succeed on first contact upload
        # call, but on second will fail. Below proves that it still returns info
        # about contacts uploaded so far.
        assert len(result) == MAX_CONTACTS_PER_PARENT_PATIENT
        assert len(result) + MAX_CONTACTS_PER_PARENT_PATIENT == len(data)


def test_command_line_script_happy_path(
    mock_upload_to_commcare, mock_get_commcare_case, mock_get_commcare_cases,
):
    "Test happy path of the command-line script for uploading legacy contacts"
    data = make_legacy_contacts_data(num=200)
    data_dict = contact_data_dict_to_list_of_dicts()
    ad_hoc_contact_key_vals = {"foo": "bar"}
    with tempfile.TemporaryDirectory() as data_dir, tempfile.TemporaryDirectory() as report_dir:
        data_path = PurePath(data_dir).joinpath("contacts.csv")
        data_dict_path = PurePath(data_dir).joinpath("data_dict.csv")
        with open(data_path, "w") as data_fl:
            field_names = [k for k in data[0]]
            writer = csv.DictWriter(data_fl, fieldnames=field_names)
            writer.writeheader()
            for contact in data:
                writer.writerow(contact)

        with open(data_dict_path, "w") as data_dict_fl:
            field_names = [k for k in data_dict[0]]
            writer = csv.DictWriter(data_dict_fl, fieldnames=field_names)
            writer.writeheader()
            for item in data_dict:
                writer.writerow(item)

        main_with_args(
            "user_name",
            "api_key",
            "my_project",
            data_path,
            data_dict_path,
            report_dir,
            prompt_user=False,
            **ad_hoc_contact_key_vals,
        )
        validation_report_path = next(
            fl
            for fl in glob.glob(
                str(
                    PurePath(report_dir).joinpath(
                        f"*{VALIDATION_REPORT_FILE_NAME_PART}*"
                    )
                )
            )
        )
        validation_df = pd.read_excel(validation_report_path)
        assert set(("is_valid", "validation_problems")).issubset(
            set(validation_df.columns)
        )

        final_report_path = next(
            fl
            for fl in glob.glob(
                str(PurePath(report_dir).joinpath(f"*{FINAL_REPORT_FILE_NAME_PART}*"))
            )
        )
        final_report_df = pd.read_excel(final_report_path)
        assert set(("contact_creation_success", "commcare_contact_case_url")).issubset(
            set(final_report_df.columns)
        )
