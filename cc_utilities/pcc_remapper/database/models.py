import csv
import re
from datetime import datetime
from io import StringIO

from bs4 import BeautifulSoup
from peewee import (
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    IntegerField,
    IntegrityError,
    Model,
    SqliteDatabase,
    TextField,
)

from cc_utilities import constants


class DatabaseConnection:
    PCC_REMAPPER_DB = SqliteDatabase(constants.DB_NAME)

    def __init__(self) -> None:
        print("Connecting to database")
        self.PCC_REMAPPER_DB.connect()
        self.PCC_REMAPPER_DB.create_tables([BaseMessage, Message])

    def close(self):
        print("Disconnecting from the database")
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
            "age": int(self.get_age()),
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
            "has_phone": "yes" if self.has_phone else "no",
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


class PccDBRecord(DatabaseConnection):
    """PccDBRecord
    A class that manages the receipt, creation, and conversion of Message database objects
    from xml source, and to csv for consumption by the CommCare Bulk API.

    The class takes two parameters that represent the form of the incoming message.

    :param soup: An xml document that has already been converted by BeautifulSoup
    :param raw: An xml document from Rhapsody that has not been converted by BeautifulSoup
    """

    def __init__(self, soup=None, raw=None) -> None:
        self.soup = soup
        self.raw = raw
        self.message_count = 0
        self.messages = []
        self._prep_records()
        super().__init__()

    def _prep_records(self):
        if self.soup:
            self.messages = self.soup.find_all("newstatement")
            self.message_count = len(self.messages)
        elif self.raw:
            self.soup = BeautifulSoup(self.raw)
            self.messages = self.soup.find_all("newstatement")
            self.message_count = len(self.messages)
        else:
            self.set_untransmitted_messages()

    def message_from_xml(self):
        """message_from_xml

        Generator that yields a single message at a time.
        """
        for message in self.messages:
            yield Message.get_representation(message)

    def messages_from_xml(self):
        """messages_from_xml
        A helper function to allow for bulk inserts into the db.
        """
        messages = []
        for message in self.messages:
            messages.append(Message.get_representation(message))
        return messages

    def write_bulk(self):
        if messages := self.messages_from_xml():
            with DatabaseConnection.PCC_REMAPPER_DB.atomic():
                Message.insert_many(messages).execute()

    def set_current_to_transmitted(self):
        for message in self.messages:
            message.transmitted = True
        with DatabaseConnection.PCC_REMAPPER_DB.atomic():
            Message.bulk_update(self.messages, fields=["transmitted"], batch_size=100)

    def write_each(self):
        """write_each

        Inserts a message at a time in order to capture and handle any collisions.
        """
        for message in self.messages_from_xml():
            with DatabaseConnection.PCC_REMAPPER_DB.atomic():
                try:
                    Message.create(**message)
                except IntegrityError:
                    # Do something when we have a collision
                    print(f"Collided with CDMS_ID: {message['case_id']}")

    def set_messages(self):
        self.messages = Message.select().where(Message.transmitted == False)

    def messages_to_csv(self):
        patient_csv = StringIO()
        lab_csv = StringIO()
        patient_writer = csv.DictWriter(patient_csv, constants.PATIENT_HEADERS)
        patient_writer.writeheader()
        lab_writer = csv.DictWriter(lab_csv, constants.LAB_HEADERS)
        lab_writer.writeheader()
        for message in self.messages:
            patient_writer.writerow(message.patient_record())
            lab_writer.writerow(message.lab_record())
        return patient_csv, lab_csv
