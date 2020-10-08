import random
from uuid import uuid4

import pandas as pd
from faker import Faker

from cc_utilities.legacy_upload import (
    create_dummy_patient_case_data,
    generate_commcare_contact_data,
    generate_commcare_external_id,
    validate_case_data_columns,
    validate_legacy_case_data,
)

Faker.seed(0)
fake = Faker("en_US")


CONTACT_DATA_DICT = {
    "first_name": {
        "group": None,
        "required": True,
        "accepted_values": [],
        "data_type": "plain",
    },
    "dob": {
        "group": None,
        "required": False,
        "accepted_values": [],
        "data_type": "date",
    },
    "phone_work": {
        "group": None,
        "required": False,
        "accepted_values": [],
        "data_type": "phone_number",
    },
    "days_symptoms_lasted": {
        "group": None,
        "required": False,
        "accepted_values": [],
        "data_type": "number",
    },
    "current_smoker": {
        "group": None,
        "required": False,
        "accepted_values": ["yes", "no", "unknown"],
        "data_type": "select",
    },
    "symptoms_selected": {
        "group": None,
        "required": False,
        "accepted_values": ["none", "fever", "chills", "headache"],
        "data_type": "multi_select",
    },
}


def make_valid_contact():
    return {
        "first_name": fake.first_name(),
        "dob": fake.date_of_birth(minimum_age=18, maximum_age=100).strftime("%Y/%m/%d"),
        "phone_work": fake.phone_number(),
        "days_symptoms_lasted": random.randint(2, 14),
        "current_smoker": random.choice(
            CONTACT_DATA_DICT["current_smoker"]["accepted_values"]
        ),
        "symptoms_selected": ", ".join(
            random.choices(
                CONTACT_DATA_DICT["symptoms_selected"]["accepted_values"], k=2
            )
        ),
    }


def make_legacy_contacts_data(num=10):
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
        assert val not in CONTACT_DATA_DICT[select_col]["accepted_values"]
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
        assert val not in CONTACT_DATA_DICT[multi_select_col]["accepted_values"]
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


class MockCommCareUploadFunctionsForUploadContacts:
    def __init__(self):
        super().__init__(MockCommCareUploadFunctionsForUploadContacts)
        self.parent_id_contact_map = {}
        self.parent_id_case_id_map = {}

    def mock_upload_data_to_commcare(
        self, data, project_slug, case_type, *args, **kwargs
    ):
        if case_type == "contact":
            for contact in data:
                if contact["parent_id"] in self.parent_id_contact_map:
                    self.parent_id_contact_map[
                        contact["parent_id"][contact["contact_id"]]
                    ] = None
                else:
                    self.parent_id_contact_map[contact["parent_id"]] = {
                        contact["contact_id"]: uuid4()
                    }
        if case_type == "patient":
            for patient in data:
                if patient["external_id"] in self.parent_id_case_id_map:
                    continue
                else:
                    self.parent_id_case_id_map[patient["external_id"]] = str(uuid4())

    def mock_get_commcare_case(self, parent_id, *args, **kwargs):
        case = self.parent_id_case_id_map[parent_id]
        return case


def mock_generate_cc_dummy_patient_cases(
    project_slug, cc_user_name, cc_api_key, num_dummies=1
):
    return [generate_commcare_external_id() for i in range(num_dummies)]


class TestUploadLegacyContactsToCommCare:
    """Test logic around validating user-supplied case data"""

    def test_happy_path(self, monkeypatch):
        monkeypatch.setattr(
            "cc_utilities.legacy_upload.generate_cc_dummy_patient_cases",
            mock_generate_cc_dummy_patient_cases,
        )
        # data = make_legacy_contacts_data()
        # return data

    def test_unhappy_path_still_returns_contacts_created_so_far(self):
        pass
