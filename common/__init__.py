import time

import pandas as pd
import requests


BULK_UPLOAD_URL = "https://www.comcarehq.org/a/{}/importer/excel/bulk_upload_api/"


def upload_data_to_commcare(
    data,
    project_slug,
    case_type,
    match_column,
    cc_username,
    cc_api_key,
    create_new_cases="on",
):
    url = BULK_UPLOAD_URL.format(project_slug)
    headers = {
        "Authorization": f"ApiKey {cc_username}:{cc_api_key}",
    }
    fieldnames = data[0].keys()
    assert (
        match_column in fieldnames
    ), f"Data items must have an '{match_column}' property"
    df = pd.DataFrame(data)
    files = {"file": (f"{case_type}.csv", df.to_csv(index=False))}
    body = dict(
        case_type=case_type,
        search_column=match_column,
        search_field=match_column,
        create_new_cases=create_new_cases,
        name_column="name",
    )
    response = requests.post(url, headers=headers, files=files, data=body)
    for line in response.text.splitlines():
        print(line)
    while True:
        seconds = 2
        print(f"Sleeping {seconds} seconds and checking upload status...")
        time.sleep(seconds)
        response_ = requests.get(response.json()["status_url"], headers=headers)
        for line in response_.text.splitlines():
            print(line)
        if response_.json()["state"]["commcare_upload_state"] == "success":
            print("Succesfully uploaded. All done")
            break
