BULK_UPLOAD_URL = "https://www.commcarehq.org/a/{}/importer/excel/bulk_upload_api/"
LIST_CASES_URL = "https://www.commcarehq.org/a/{}/api/v0.5/case/"
CASE_REPORT_URL = "https://www.commcarehq.org/a/{}/reports/case_data/"
APPLICATION_STRUCTURE_URL = "https://www.commcarehq.org/a/{}/api/v0.5/application/"
APPLICATION_STRUCTURE_DEFAULT_TIMEOUT = 4 * 60  # seconds
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

# case fields that all cases will have but that will not be returned by the Application
# Structure API endpoint, which is the source of the case type's properties past and
# present.
COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS = {
    # Form is (<source>, <target>)
    ("case_id", "case_id"),
    ("closed", "closed"),
    ("date_closed", "date_closed"),
    ("date_modified", "date_modified"),
    ("domain", "domain"),
    ("id", "id"),
    ("indexed_on", "indexed_on"),
    ("opened_by", "opened_by"),
    ("properties.case_type", "case_type"),
    ("properties.closed_by", "closed_by"),
    ("properties.closed_on", "closed_on"),
    ("properties.date_opened", "date_opened"),
    ("properties.doc_type", "doc_type"),
    ("properties.external_id", "external_id"),
    ("properties.indices.patient", "indices.patient"),
    ("properties.modified_on", "modified_on"),
    ("properties.number", "number"),
    ("properties.owner_id", "owner_id"),
    ("resource_uri", "resource_uri"),
    ("server_date_modified", "server_date_modified"),
    ("server_date_opened", "server_date_opened"),
    ("user_id", "user_id"),
}
