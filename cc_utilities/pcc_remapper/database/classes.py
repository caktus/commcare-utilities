import csv
from io import StringIO

from bs4 import BeautifulSoup
from peewee import IntegrityError

from cc_utilities import constants
from cc_utilities.logger import logger

from .models import DatabaseConnection, Message


class PccDBRecord(DatabaseConnection):
    """PccDBRecord
    A class that manages the receipt, creation, and conversion of Message database objects
    from xml or BeautifulSoup source to csv for consumption by the CommCare Bulk API.

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
        if not self.soup:
            self.soup = BeautifulSoup(self.raw)
        self.messages = self.soup.find_all("newstatement")
        self.message_count = len(self.messages)

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
                    logger.info(f"Collided with CDMS_ID: {message['case_id']}")

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
