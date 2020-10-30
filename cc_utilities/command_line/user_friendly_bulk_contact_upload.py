import csv
import os
import subprocess

from openpyxl import load_workbook

CONTACT_SHEET_NAME = "contacts"

# i.e., column "M", which has a formula for LPHA facing data validation
# TODO: LINK TO THE ASSET ON s3 here
DELETE_COLUMN_INDEX = 13


def preprocess_linelist(path_to_wb):
    wb = load_workbook(path_to_wb, guess_types=False)
    ws = wb.get_sheet_by_name(CONTACT_SHEET_NAME)
    ws.delete_cols(DELETE_COLUMN_INDEX)
    wb.save(path_to_wb)


def lookup_owner_id_for_project(project_slug, agency_owner_lookup_path):
    with open(agency_owner_lookup_path) as fl:
        reader = csv.DictReader(fl)
        row = next([row for row in reader if row["project_slug"] == project_slug])
    return row["owner_id"]


if __name__ == "__main__":
    vars = {
        "COMMCARE_USER_NAME": os.environ.get("COMMCARE_USER_NAME"),
        "COMMCARE_API_KEY": os.environ.get("COMMCARE_API_KEY"),
        "COMMCARE_CONTACT_DATA_DICT_CSV": os.environ.get(
            "COMMCARE_CONTACT_DATA_DICT_CSV"
        ),
        "PROJECT_AGENCY_OWNER_LOOKUP_PATH": os.environ.get(
            "PROJECT_AGENCY_OWNER_LOOKUP_PATH"
        ),
    }

    missing = [k for k in vars if not vars[k]]
    if missing:
        print(f"The following env var(s) need(s) to be set: {', '.join(missing)}")

    while True:
        file_path = input("Enter the path to the contact data Excel file to upload:  ")
        if file_path:
            break

    while True:
        reporting_path = input(
            "Enter the path the folder to store reporting data in:  "
        )
        if reporting_path:
            break

    while True:
        project_slug = "Enter the project slug: "
        if project_slug:
            break

    owner_id = lookup_owner_id_for_project(
        project_slug, vars["PROJECT_AGENCY_OWNER_LOOKUP_PATH"]
    )

    commands = (
        f"bulk-upload-legacy-contact-data "
        f"--username {vars['COMMCARE_USER_NAME']} "
        f"--apikey {vars['COMMCARE_API_KEY']} "
        f"--project {project_slug} "
        f"--dataDictPath {vars['COMMCARE_CONTACT_DATA_DICT_CSV']} "
        f"--reportingPath {reporting_path} "
        f"--contactKeyValDict "
        f"""'{{"owner_id": {owner_id}}}' """
        f"--caseDataPath {file_path}"
    )
    commands = commands.split(" ")
    import pdb

    pdb.set_trace()
    subprocess.run(commands)
