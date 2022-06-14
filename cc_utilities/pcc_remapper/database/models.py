import re
from datetime import datetime

from peewee import (
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    IntegerField,
    Model,
    SqliteDatabase,
    TextField,
)

from cc_utilities import constants
from cc_utilities.logger import logger


class DatabaseConnection:
    PCC_REMAPPER_DB = SqliteDatabase(constants.DB_NAME)

    def __init__(self) -> None:
        logger.info("Connecting to database")
        logger.info(f"DB_LOCATION: {constants.DB_NAME}")
        self.PCC_REMAPPER_DB.connect()
        self.PCC_REMAPPER_DB.create_tables([BaseMessage, Message])

    def close(self):
        logger.info("Disconnecting from the database")
        self.PCC_REMAPPER_DB.close()


class BaseMessage(Model):
    created = DateTimeField(default=datetime.now(), formats=constants.DATE_TIME_FORMATS)
    modified = DateTimeField(formats=constants.DATE_TIME_FORMATS, null=True)
    transmitted = BooleanField(default=False)
    can_sms = BooleanField(default=False)
    error = BooleanField(default=False)
    errors = TextField(null=True)

    class Meta:
        database = DatabaseConnection.PCC_REMAPPER_DB


class Message(BaseMessage):
    case_id = IntegerField(unique=True, index=True)  # CDMS_ID
    unid = IntegerField(null=True)  # EXTERNAL_ID
    first_name = CharField(null=True)
    last_name = CharField(null=True)
    street1 = CharField(null=True)
    street2 = CharField(null=True)
    city = CharField(null=True)
    postal_code = CharField()
    mobile_phone = CharField(max_length=32)
    email = CharField(null=True)
    home_phone = CharField(max_length=32)
    age_years = DecimalField(null=True)
    create_date = CharField(null=True)
    state = CharField(null=True)
    birth_date = DateField(null=True, formats=constants.DATE_FORMATS)
    test = CharField(null=True)  # TEST_TYPE
    race = CharField(null=True)
    result = CharField(null=True)
    gender = CharField(null=True)
    result_date = DateField(null=True, formats=constants.DATE_FORMATS)
    result_value = CharField(null=True)  # This, currently, is not mapped in Rhapsody
    facility = CharField(null=True)
    facility_other = CharField(null=True)
    order_facility = CharField(null=True)
    specimen_date = DateField(null=True, formats=constants.DATE_FORMATS)
    order_provider = CharField(null=True)
    hispanic = CharField(null=True)
    commcare_id = IntegerField(null=True)

    @staticmethod
    def get_representation(message):
        values = {}
        for field, value in Message._meta.__dict__.get("fields").items():
            if val := message.find(field):
                values[field] = val.text
        return values

    def patient_record(self):
        return {
            "external_id": self.case_id,
            "cdms_id": self.case_id,
            "owner_name": "dev_health_department",
            "disease_status": self.disease_status,
            "name": f"{self.first_name}",
            "full_name": f"{self.first_name} {self.last_name}",
            "age": self.get_age(),
            "address": self.full_address(),  # We might want to do some normalization here
            "address_complete": "yes" if self.street1 else "no",
            "address_street": self.street1,
            "address_city": self.city,
            "address_county": self.city,
            "address_state": self.state,
            "address_zip": self.postal_code[:5],
            "specimen_collection_date": self.specimen_date,
            "analysis_date": self.result_date,
            "close_base_date": self.result_date
            if self.result_date
            else self.specimen_date,
            "interview_initiated": "no",
            "interview_disposition": "no_attempt",
            "patient_type": "confirmed",
            "dob": self.birth_date,
            "dob_known": "yes" if self.birth_date else "no",
            "ethnicity": self.get_ethnicity(),
            "race": constants.RACE_MAP.get(self.race, None),
            "gender": constants.GENDER_MAP.get(self.gender, None),
            "first_name": self.first_name,
            "last_name": self.last_name,
            "phone_home": self.get_phone(),
            "contact_phone_number": f"{self.get_phone(True)}",
            "has_phone_number": "yes" if self.has_phone else "no",
            "received_heads_up_sms": "no",
            "current_status": "open",
            "case_import_date": self.create_date,
        }

    def lab_record(self):
        return {
            "parent_external_id": self.case_id,
            "parent_type": "patient",
            "parent_relationship_type": "extension",
            "parent_identifier": "parent",
            "external_id": self.unid,
            "name": self.lab_record_name,
            "cdms_id": self.case_id,
            "cdms_create_date": self.create_date,
            "specimen_collection_date": self.specimen_date,
            "analysis_date": self.result_date,
            "lab_facility": self.get_facility(constants.FacilityTypes.LAB),
            "ordering_facility": self.get_facility(constants.FacilityTypes.ORDERING),
            "ordering_provider": self.order_provider,
            "lab_result": "positive",
            "test_type": constants.TEST_RESULTS_ALL_MAP.get(self.test, None),
        }

    @property
    def lab_record_name(self):
        prefix = f"{self.case_id}-"
        if self.result_date:
            return f"{prefix}{str(self.result_date).replace('-', '_')}"
        return f"{prefix}{str(self.create_date).replace('-', '_')}"

    @property
    def has_phone(self):
        if phone := self.get_phone():
            if len(phone) == 10 and phone != "9999999999":
                return True

    @property
    def send(self):
        if (
            self.result in constants.RESULT_VALUES_SEND
            and self.result not in constants.TEST_RESULTS_NO_SEND
        ):
            return True

    @property
    def disease_status(self):
        if self.test in constants.TEST_RESULTS_CONFIRMED:
            return "confirmed"
        if self.test in constants.TEST_RESULTS_PROBABLE:
            return "probable"

    @property
    def sms(self):
        if self.can_sms:
            return constants.COMMCARE_CAN_SMS_LABEL
        return constants.COMMCARE_CANNOT_SMS_LABEL

    def get_ltcf(self, words):
        return " ".join([x.capitalize() for x in str(words).split(" ")])

    def full_address(self):
        return f"{self.get_ltcf(self.street1)}, {self.get_ltcf(str(self.city))}, {self.state}, {self.postal_code}"

    def get_age(self):
        return str(self.age_years).split(".")[0]

    def get_facility(self, facility_type):
        facility = None
        if facility_type == constants.FacilityTypes.LAB:
            facility = self.facility.split(",")[0]
        if facility_type == constants.FacilityTypes.ORDERING:
            facility = self.order_facility.split(",")[0]
        return facility

    def get_phone(self, contact=False):
        phone = None
        clean = re.compile(r"[(,),\s,-]")
        if self.mobile_phone:
            phone = re.sub(clean, "", self.mobile_phone)
        if self.home_phone:
            phone = re.sub(clean, "", self.home_phone)

        if phone:
            return phone if not contact else f"1{phone}"

    def get_ethnicity(self):
        ethnicity = "UNKNOWN"
        if self.hispanic == "Yes":
            ethnicity = "HISPANIC"
        if self.hispanic == "No":
            ethnicity = "NOT_HISPANIC"
        return ethnicity
