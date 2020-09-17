import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from .constants import BULK_UPLOAD_URL, COMMCARE_UPLOAD_STATES
from .logger import logger


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
    request_timeout=30,
):
    retry_strategy = Retry(
        total=3, backoff_factor=6, status_forcelist=[500], method_whitelist=["POST"]
    )
    headers = {
        "Authorization": f"ApiKey {cc_username}:{cc_api_key}",
    }
    adapter = HTTPAdapter(max_retries=retry_strategy)
    url = BULK_UPLOAD_URL.format(project_slug)
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
    with requests.Session() as session:
        session.headers.update(headers)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        req = requests.Request("POST", url, data=body, files=files)
        prepped = session.prepare_request(req)
        response = session.send(prepped, timeout=request_timeout)
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
            response.json()["status_url"], headers=headers, timeout=request_timeout
        )
        if response_.json()["state"] == COMMCARE_UPLOAD_STATES["success"]:
            logger.info("Succesfully uploaded. All done.")
            break
