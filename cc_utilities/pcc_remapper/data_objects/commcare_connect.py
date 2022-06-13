import json
import os
import tempfile
from pathlib import Path

import requests

from cc_utilities import constants
from cc_utilities.logger import logger
from cc_utilities.pcc_remapper.database.classes import PccDBRecord


class CommCareConnect:
    COMMCARE_HEADERS = {
        "Authorization": f"ApiKey {os.getenv('COMMCARE_USERNAME')}:{os.getenv('COMMCARE_API_KEY')}"
    }

    def __init__(self, testing=True) -> None:
        self.url = (
            constants.BULK_UPLOAD_URL.format(constants.COMMCARE_TEST_SLUG)
            if testing
            else constants.BULK_UPLOAD_URL.format(constants.COMMCARE_PROD_SLUG)
        )
        self.pcc_records = PccDBRecord()
        self.status = ""
        self.status_message = ""
        self.status_url = ""
        self.response_json = {}
        self.patients, self.labs = self.pcc_records.messages_to_csv()

    def _send(self, case_type):
        values = {
            "case_type": case_type,
            "search_column": "external_id",
            "search_field": "external_id",
            "create_new_cases": "on",
        }
        value = self.patients.getvalue()
        if case_type == constants.CaseTypes.LAB_RESULT:
            value = self.labs.getvalue()
        tempf = tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False)
        tempf.write(value)
        tempf.close()
        with Path(tempf.name).open("r", encoding="utf-8") as fh:
            file = {"file": (f"{case_type}.csv", fh, "application/csv; charset=UTF-8")}
            response = requests.post(
                self.url,
                files=file,
                data=values,
                headers=CommCareConnect.COMMCARE_HEADERS,
            )
            if response.status_code == 200:
                self.response_json = json.loads(response.content)
                self.status_message = self.response_json.get("message")
                self.status_url = self.response_json.get("status_url")
                logger.info(f"{case_type} Send: success.")
            if response.status_code == 402:
                logger.info(f"{case_type} Send: failed.")
            if response.status_code == 500:
                logger.info(f"{case_type} Send: failed.")
                pass
        Path(tempf.name).unlink()

    def set_status(self):
        response = requests.get(self.status_url)
        if response.status_code == 200:
            self.status = json.loads(response.content)

    def send(self):
        self._send(str(constants.CaseTypes.PATIENT))
        cc.set_status()
        self._send(str(constants.CaseTypes.LAB_RESULT))
        cc.set_status()


if __name__ == "__main__":
    from cc_utilities.pcc_remapper.factories.message_factory import (
        generate_message_file,
    )

    generate_message_file()
    cc = CommCareConnect()
    cc.send()
