import uuid

import requests
from faker import Faker

from cc_utilities.constants import (
    TWILIO_INVALID_NUMBER_FOR_REGION_CODE,
    TWILIO_LANDLINE_CODE,
    TWILIO_VOIP_CODE,
)
from cc_utilities.twilio_lookup import (
    COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME,
    TWILIO_MOBILE_CODE,
    process_contacts,
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

    `cc_utilities.twilio_lookup.process_contacts` contains the primary business logic
    for determining if numbers can receive SMS or not. In this test suite, we show
    the expected behavior for a range of expected phone number inputs.

    This functionality is ultimately used by
    `scripts/batch-process-contacts-for-can-receive-sms` which in addition to calling
    `process_contacts` is also responsible for retrieving unprocessed contacts from
    db-land, and ultimately propogating results of `process_contacts` back up to
    CommCare. We only narrowly test the business logic around `process_contacts`.
    """

    def test_valid_mobile_us_number_can_receive_sms(self, monkeypatch):
        """Valid US mobile numbers should have `True` for SMS capability"""

        def mock_get(*args, **kwargs):
            return MockTwilioPhoneTypeMobileResponse()

        monkeypatch.setattr(requests, "get", mock_get)
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        fake = Faker("en_US")
        data = [
            {search_column: uuid.uuid1(), "contact_phone_number": fake.phone_number()}
            for _ in range(5)
        ]
        processed = process_contacts(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert item[COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME] is True

    def test_valid_voip_us_number_cant_receive_sms(self, monkeypatch):
        """Valid US VOIP numbers should have `False` for SMS capability"""

        def mock_get(*args, **kwargs):
            return MockTwilioPhoneTypeVoipResponse()

        monkeypatch.setattr(requests, "get", mock_get)
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        fake = Faker("en_US")
        data = [
            {search_column: uuid.uuid1(), "contact_phone_number": fake.phone_number()}
            for _ in range(5)
        ]
        processed = process_contacts(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert item[COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME] is False

    def test_valid_landline_us_number_cant_receive_sms(self, monkeypatch):
        """Valid US landline numbers should have `False` for SMS capability"""

        def mock_get(*args, **kwargs):
            return MockTwilioPhoneTypeLandlineResponse()

        monkeypatch.setattr(requests, "get", mock_get)
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        fake = Faker("en_US")
        data = [
            {search_column: uuid.uuid1(), "contact_phone_number": fake.phone_number()}
            for _ in range(5)
        ]
        processed = process_contacts(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert item[COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME] is False

    def test_valid_non_us_number_cant_receive_sms(self, monkeypatch):
        """Valid non-US numbers should have `False` for SMS capability

        This is because our present use case is US-focused, and Twilio requires setting
        a country code when using the lookup API.
        """

        def mock_get(*args, **kwargs):
            return MockTwilioPhoneNumberNonUsRegion()

        monkeypatch.setattr(requests, "get", mock_get)
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        fake = Faker("en_CA")
        data = [
            {search_column: uuid.uuid1(), "contact_phone_number": fake.phone_number()}
            for _ in range(5)
        ]
        processed = process_contacts(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert item[COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME] is False

    def test_unformattable_phone_number_cant_receive_sms(self):
        """Invalid phonenumbers should have `False` for SMS capability"""
        search_column = "id"
        twilio_sid = twilio_token = "xxxxxx"
        data = [
            {search_column: uuid.uuid1(), "contact_phone_number": "abcdefg"},
            {search_column: uuid.uuid1(), "contact_phone_number": ""},
            {search_column: uuid.uuid1(), "contact_phone_number": " "},
        ]
        processed = process_contacts(data, search_column, twilio_sid, twilio_token)
        for item in processed:
            assert item[COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME] is False
