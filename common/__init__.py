import time

import pandas as pd
import requests

from .logger import logger

BULK_UPLOAD_URL = "https://www.commcarehq.org/a/{}/importer/excel/bulk_upload_api/"
COMMCARE_UPLOAD_STATES = dict(missing=-1, not_started=0, started=1, success=2, failed=3)


class CommCareUtilitiesError(Exception):
    def __init__(self, message, info):
        super(CommCareUtilitiesError, self).__init__(message)
        self.info = info


def upload_data_to_commcare(
    data,
    project_slug,
    case_type,
    search_column,
    cc_username,
    cc_api_key,
    create_new_cases="on",
    search_field="case_id",
):
    url = BULK_UPLOAD_URL.format(project_slug)
    headers = {
        "Authorization": f"ApiKey {cc_username}:{cc_api_key}",
    }
    fieldnames = data[0].keys()
    assert (
        search_column in fieldnames
    ), f"Data items must have property '{search_column}'"
    df = pd.DataFrame(data)
    files = {
        "file": (
            f"{case_type}.csv",
            df.to_csv(index=False),
            "application/csv",
            {"charset": "UTF-8"},
        )
    }
    body = dict(
        case_type=case_type,
        search_column=search_column,
        search_field=search_field,
        create_new_cases=create_new_cases,
    )

    response = requests.post(url, headers=headers, files=files, data=body, timeout=5)
    if not response.ok:
        message = "Something went wrong uploading data to CommCare"
        info = {
            "commcare_response_status_code": response.status_code,
            "commcare_response_text": response.text,
        }
        logger.error(message)
        raise CommCareUtilitiesError(message, info)
    while True:
        seconds = 2
        logger.info(f"Sleeping {seconds} seconds and checking upload status...")
        time.sleep(seconds)
        response_ = requests.get(
            response.json()["status_url"], headers=headers, timeout=2
        )
        if response_.json()["state"] == COMMCARE_UPLOAD_STATES["success"]:
            logger.info("Succesfully uploaded. All done.")
            break
