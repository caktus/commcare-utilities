import uuid

from faker import Faker

from cc_utilities import twilio_lookup
from cc_utilities.constants import (
    COMMCARE_CAN_SMS_LABEL,
    COMMCARE_CANNOT_SMS_LABEL,
    COMMCARE_PHONE_FIELD,
    TWILIO_INVALID_NUMBER_FOR_REGION_CODE,
    TWILIO_LANDLINE_CODE,
    TWILIO_VOIP_CODE,
)
from cc_utilities.twilio_lookup import (
    COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME,
    TWILIO_MOBILE_CODE,
    add_bad_ids,
    get_bad_ids,
    process_records,
)


class MockTwilioPhoneTypeMobileResponse:
    "Used to mock expected requests.response from Twilio when valid US mobile number"

    @property
    def ok(self):
        return True

    @staticmethod
    def json():
        return {"carrier": {"type": TWILIO_MOBILE_CODE}}


class MockTwilioPhoneTypeVoipResponse:
    "Used to mock expected requests.response from Twilio when valid US VOIP number"

    @property
    def ok(self):
        return True

    @staticmethod
    def json():
        return {"carrier": {"type": TWILIO_VOIP_CODE}}


class MockTwilioPhoneTypeLandlineResponse:
    "Used to mock expected requests.response from Twilio when valid US landline number"

    @property
    def ok(self):
        return True

    @staticmethod
    def json():
        return {"carrier": {"type": TWILIO_LANDLINE_CODE}}


class MockTwilioPhoneNumberNonUsRegion:
    "Used to mock expected requests.response from Twilio when valid non-US number"

    @property
    def ok(self):
        return False

    @property
    def status_code(self):
        return TWILIO_INVALID_NUMBER_FOR_REGION_CODE


class TestBulkProcessNumbersCanReceiveSms:
    """Test business logic around determining if numbers can receive SMS

    `cc_utilities.twilio_lookup.process_records` contains the primary business logic
    for determining if numbers can receive SMS or not. In this test suite, we show
    the expected behavior for a range of expected phone number inputs.

    This functionality is ultimately used by
    `scripts/process-numbers-for-sms-capabilty` which in addition to calling
    `process_records` is also responsible for retrieving unprocessed records from
    db-land, and ultimately propogating results of `process_records` back up to
    CommCare. We only narrowly test the business logic around `process_records`.
    """

    def test_valid_mobile_us_number_can_receive_sms(self, monkeypatch):
        """Valid US mobile numbers should have `True` for SMS capability"""

        def mock_get(*args, **kwargs):
            return MockTwilioPhoneTypeMobileResponse()

        monkeypatch.setattr(twilio_lookup, "twilio_http_request", mock_get)
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        fake = Faker("en_US")
        data = [
            {search_column: uuid.uuid1(), COMMCARE_PHONE_FIELD: fake.phone_number()}
            for _ in range(5)
        ]
        processed = process_records(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert item[COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME] == COMMCARE_CAN_SMS_LABEL

    def test_valid_voip_us_number_cant_receive_sms(self, monkeypatch):
        """Valid US VOIP numbers should have `False` for SMS capability"""

        def mock_get(*args, **kwargs):
            return MockTwilioPhoneTypeVoipResponse()

        monkeypatch.setattr(twilio_lookup, "twilio_http_request", mock_get)
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        fake = Faker("en_US")
        data = [
            {search_column: uuid.uuid1(), COMMCARE_PHONE_FIELD: fake.phone_number()}
            for _ in range(5)
        ]
        processed = process_records(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert (
                item[COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME] == COMMCARE_CANNOT_SMS_LABEL
            )

    def test_valid_landline_us_number_cant_receive_sms(self, monkeypatch):
        """Valid US landline numbers should have `False` for SMS capability"""

        def mock_get(*args, **kwargs):
            return MockTwilioPhoneTypeLandlineResponse()

        monkeypatch.setattr(twilio_lookup, "twilio_http_request", mock_get)
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        fake = Faker("en_US")
        data = [
            {search_column: uuid.uuid1(), COMMCARE_PHONE_FIELD: fake.phone_number()}
            for _ in range(5)
        ]
        processed = process_records(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert (
                item[COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME] == COMMCARE_CANNOT_SMS_LABEL
            )

    def test_valid_non_us_number_cant_receive_sms(self, monkeypatch):
        """Valid non-US numbers should have `False` for SMS capability

        This is because our present use case is US-focused, and Twilio requires setting
        a country code when using the lookup API.
        """

        def mock_get(*args, **kwargs):
            return MockTwilioPhoneNumberNonUsRegion()

        monkeypatch.setattr(twilio_lookup, "twilio_http_request", mock_get)
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        fake = Faker("en_CA")
        data = [
            {search_column: uuid.uuid1(), COMMCARE_PHONE_FIELD: fake.phone_number()}
            for _ in range(5)
        ]
        processed = process_records(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert (
                item[COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME] == COMMCARE_CANNOT_SMS_LABEL
            )

    def test_unformattable_phone_number_cant_receive_sms(self):
        """Invalid phonenumbers should have `False` for SMS capability"""
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        data = [
            {search_column: uuid.uuid1(), COMMCARE_PHONE_FIELD: "abcdefg"},
            {search_column: uuid.uuid1(), COMMCARE_PHONE_FIELD: ""},
            {search_column: uuid.uuid1(), COMMCARE_PHONE_FIELD: " "},
        ]
        processed = process_records(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert (
                item[COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME] == COMMCARE_CANNOT_SMS_LABEL
            )

    def test_bad_ids_state(self):
        """Bad IDs are set and returned for a the given case_type."""
        bad_ids = [
            "ep6GwxUbI2mGBB3JiFWOJ2q0l1kGdsAO",
            "r9RzBRhrSD8Tz4huHTbJ0AANybm7uMUl",
            "Nsb5JcTTDN0JrdYWWk99IfScRl0FwwQo",
        ]
        add_bad_ids("contact", bad_ids)
        assert set(get_bad_ids("contact")) == set(bad_ids)
        # IDs are specific to case_type
        assert get_bad_ids("other") == []
