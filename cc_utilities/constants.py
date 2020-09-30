BULK_UPLOAD_URL = "https://www.commcarehq.org/a/{}/importer/excel/bulk_upload_api/"
LIST_CASES_URL = "https://www.commcarehq.org/a/{}/api/v0.5/case/"
COMMCARE_CONTACT_PHONE_FIELD = "phone_home"
COMMCARE_CAN_RECEIVE_SMS_FIELD_NAME = "api_phone_home_sms_verification"
COMMCARE_UPLOAD_STATES = dict(missing=-1, not_started=0, started=1, success=2, failed=3)
COMMCARE_UNSET_CAN_SMS_LABEL = "pending"
COMMCARE_CANNOT_SMS_LABEL = "no"
COMMCARE_CAN_SMS_LABEL = "yes"
TWILIO_INVALID_NUMBER_FOR_REGION_CODE = 404
TWILIO_LANDLINE_CODE = "landline"
TWILIO_LOOKUP_URL = "https://lookups.twilio.com/v1/PhoneNumbers"
TWILIO_MOBILE_CODE = "mobile"
TWILIO_VOIP_CODE = "voip"
WHITE_LISTED_TWILIO_CODES = [
    TWILIO_INVALID_NUMBER_FOR_REGION_CODE,
]
