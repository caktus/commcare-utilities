BULK_UPLOAD_URL = "https://www.commcarehq.org/a/{}/importer/excel/bulk_upload_api/"
COMMCARE_CAN_RECIEVE_SMS_FIELD_NAME = "contact_phone_can_receive_sms"
COMMCARE_UPLOAD_STATES = dict(missing=-1, not_started=0, started=1, success=2, failed=3)
TWILIO_INVALID_NUMBER_FOR_REGION_CODE = 404
TWILIO_LANDLINE_CODE = "landline"
TWILIO_LOOKUP_URL = "https://lookups.twilio.com/v1/PhoneNumbers"
TWILIO_MOBILE_CODE = "mobile"
TWILIO_VOIP_CODE = "voip"
WHITE_LISTED_TWILIO_CODES = [
    TWILIO_INVALID_NUMBER_FOR_REGION_CODE,
]
