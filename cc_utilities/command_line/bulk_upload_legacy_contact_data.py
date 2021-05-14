import argparse
import csv
import io
import json
import sys
from datetime import datetime
from pathlib import Path, PurePath

import pandas as pd
from openpyxl import load_workbook

from cc_utilities.legacy_upload import (
    LegacyUploadError,
    clean_raw_case_data_df,
    generate_commcare_case_report_url,
    generate_commcare_external_id,
    load_data_dict,
    normalize_legacy_case_data,
    upload_legacy_contacts_to_commcare,
    validate_case_data_columns,
    validate_legacy_case_data,
)
from cc_utilities.logger import logger

VALIDATION_REPORT_FILE_NAME_PART = "validation_report"
FINAL_REPORT_FILE_NAME_PART = "final_report"
WB_CONTACT_SHEET_NAME = "contacts"


def convert_xl_wb_to_csv_string_io(wb_path, sheet_name=WB_CONTACT_SHEET_NAME):
    """Used to accomodate Excel workbook inputs
    Converts an Excel workbook into a string IO representing the data as a CSV.

    NB: We use this approach rather than using pd.read_excel because we need all values
    to be treated as text and read_excel can lead to inferred data types we don't want
    """
    path = Path(wb_path).expanduser()
    wb = load_workbook(path)
    ws = wb[sheet_name]
    output = io.StringIO()
    writer = csv.writer(output)
    for row in ws.rows:
        # only want non-empty rows
        if any([cell.value for cell in row]):
            writer.writerow([cell.value for cell in row])
    output.seek(0)
    return output


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    legacy_case_data_path,
    data_dictionary_path,
    reporting_path,
    reject_all_if_any_invalid_rows=True,
    prompt_user=True,
    rename_columns=None,
    required_one_ofs=None,
    **contact_kwargs,
):
    """The main routine. Create CommCare contacts based on legacy contact data.

    Attempt to create contact cases for each legacy contact provided. The columns
    of the legacy case data CSV are validated, along with row values based on the values
    found in the data dictionary at `data_dictionary_path`.

    In addition to creating contacts on CommCare, this function creates two Excel files
    along the way that can be shared with end users. Both are based on the original
    input legacy case data CSV. In addition to all rows and columns that appear in that
    CSV, the validation report provides feedback on which rows were valid, and any
    validation problems encountered. In the final report, the original rows from the
    CSV appear, along with an indication of whether or not CommCare upload was
    successful, and if so, there will be a link to view the uploaded contact in CommCare.


    Args:
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be imported
        legacy_case_data_path (str): Path to an Excel file or CSV containing contacts
            to be imported
        data_dictionary_path (str): Path to a CSV of a data dict used to validate user-
            supplied contact data. Note that this asset is based on but distinct from
            the data dict provided in the CommCare dashboard. It should have the
            following columns: field, group, allowed_values, data_type, and required
        reporting_path (str): A folder to output reports indicating what happened.
        reject_all_if_any_invalid_rows (bool): If `True`, if any rows in case data CSV
            contain invalid data, the entire routine will be halted.
        prompt_user (bool): If true, user will be prompted to affirm moving forward
            after data validation and normalization has succeeded. In testing, we need
            this behavior to be suppressed, so this param is to support that use case.
        rename_columns (dict): Optional. Keys are original column names, values are new
            names.
        required_one_ofs (list): Optional. A list of columns from which at least one
            must have a valid, non-null value per row
        contact_kwargs (dict): Optional key/value pairs that will be added to each
            generated contact.
    """
    required_one_ofs = required_one_ofs if required_one_ofs else []

    logger.info(f"Loading data dictionary at {data_dictionary_path}")
    data_dict = load_data_dict(data_dictionary_path)
    assert all(
        [field in data_dict for field in ("first_name", "last_name")]
    ), "Data dict must contain `first_name` and `last_name` for contact upload"
    # these must be present in order to enable creation of `name` property, which
    # needs to be generated if not supplied in upload data
    data_dict["first_name"]["required"] = True
    data_dict["last_name"]["required"] = True

    logger.info(f"Loading legacy contact data at {legacy_case_data_path}")

    # Pandas infers data types, and that's not helpful in this context. For validation
    # and normalization purposes, we need all inputs to be strings.
    # The main script wants a CSV file, but users may supply Excel wbs. If wb supplied
    # we convert the `contacts` sheet into a CSV string IO
    case_data_file = (
        legacy_case_data_path
        if legacy_case_data_path.endswith(".csv")
        else convert_xl_wb_to_csv_string_io(legacy_case_data_path)
    )

    # avoid unexpected data type conversions. we just treat everything as string.
    raw_case_data_df = pd.read_csv(case_data_file, keep_default_na=False, dtype=str)
    if rename_columns:
        col_map_string = ", ".join([f"{k} -> {v}" for (k, v) in rename_columns.items()])
        logger.info(f"Renaming columns: {col_map_string}")
        raw_case_data_df.rename(columns=rename_columns, inplace=True)

    logger.info("Validating columns in legacy contact data CSV against data dictionary")
    if (
        validate_case_data_columns(
            raw_case_data_df.columns,
            data_dict.keys(),
            [
                key
                for key in data_dict
                if data_dict[key]["required"]
                in ("true", "True", "TRUE", True, 1, "1", "yes", "Yes", "YES", "y", "Y")
            ],
        )
        is False
    ):
        msg = (
            "Columns in case data were invalid, either because of unexpected column "
            "names or because required column names were missing. Compare column "
            "names to data dict expectations"
        )
        raise LegacyUploadError(msg)

    # Some pre-validation cleanup is required
    cleaned_case_data_df = clean_raw_case_data_df(raw_case_data_df, data_dict)

    case_data_df = validate_legacy_case_data(
        cleaned_case_data_df, data_dict, required_one_ofs=required_one_ofs
    )

    logger.info(
        "Validating row values in legacy contact data CSV against data dictionary"
    )
    now_string = datetime.now().strftime("%m-%d-%Y_%H-%M")
    validation_report_name = (
        f"{Path(legacy_case_data_path).stem}_{VALIDATION_REPORT_FILE_NAME_PART}_"
        f"{now_string}.xlsx"
    )
    validation_report_path = PurePath(reporting_path).joinpath(validation_report_name)
    report_df = raw_case_data_df.merge(
        case_data_df[["is_valid", "validation_problems"]],
        left_index=True,
        right_index=True,
    )
    logger.info(f"Generating validation report at {validation_report_path}")
    report_df.to_excel(
        validation_report_path, index=False, sheet_name=WB_CONTACT_SHEET_NAME
    )
    num_invalid = len(case_data_df[~case_data_df["is_valid"]])
    if not case_data_df["is_valid"].all() and reject_all_if_any_invalid_rows:
        msg = (
            f"{num_invalid} rows were invalid and `reject_all_if_any_invalid_rows` "
            f"is True. No case data will be uploaded. See details in the validation "
            f"report at {validation_report_path}."
        )
        logger.error(msg)
        raise LegacyUploadError(msg)

    # We generate this value in this context as it allows us to match up our original
    # data with results from the CommCare API and produce a report in which we
    # provide links to created contacts
    case_data_df["contact_id"] = case_data_df.apply(
        lambda row: generate_commcare_external_id(), axis=1
    )
    valid_df = case_data_df[case_data_df["is_valid"]].drop(
        ["is_valid", "validation_problems"], axis=1
    )
    logger.info("Normalizing legacy case data")
    normalized_case_data_df = normalize_legacy_case_data(
        valid_df, data_dict, ignore_columns=["contact_id"]
    )

    if "name" not in normalized_case_data_df.columns:
        logger.info(
            "Generating `name` property from `first_name`, `last_name`, and "
            "`contact_id`"
        )
        normalized_case_data_df["name"] = normalized_case_data_df.apply(
            lambda row: f"{row['first_name']} {row['last_name']} ({row['contact_id']})",
            axis=1,
        )
    if prompt_user:
        while True:
            keep_going = input(
                "Data validated and normalized. Do you want to continue to upload [y/n]"
            )
            if keep_going.lower() in ("y", "yes"):
                break
            if keep_going.lower() in ("n", "no"):
                logger.info("Terminating `bulk_upload_contact_data`")
                sys.exit(0)

    logger.info("Attempting to upload contacts to CommCare")
    created_contacts_dict = upload_legacy_contacts_to_commcare(
        normalized_case_data_df.to_dict(orient="records"),
        commcare_project_name,
        commcare_user_name,
        commcare_api_key,
        **contact_kwargs,
    )
    normalized_case_data_df["contact_creation_success"] = normalized_case_data_df[
        "contact_id"
    ].apply(lambda val: val in created_contacts_dict.keys())

    # this would be a lambda but didn't readably fit in one line
    def _generate_contact_case_url_col_values(row, created_contacts_dict):
        if row["contact_creation_success"] is False:
            return ""
        else:
            return generate_commcare_case_report_url(
                created_contacts_dict[row["contact_id"]], commcare_project_name
            )

    normalized_case_data_df[
        "commcare_contact_case_url"
    ] = normalized_case_data_df.apply(
        _generate_contact_case_url_col_values,
        created_contacts_dict=created_contacts_dict,
        axis=1,
    )
    # generate a final frame that combines original contact data along with
    # columns we generated indicating if upload was successful and url to CommCare
    # contact.
    final_df = case_data_df.merge(
        normalized_case_data_df[
            ["contact_creation_success", "commcare_contact_case_url", "contact_id"]
        ],
        how="left",
        on="contact_id",
    )
    final_report_name = (
        f"{Path(legacy_case_data_path).stem}_{FINAL_REPORT_FILE_NAME_PART}_"
        f"{now_string}.xlsx"
    )
    final_report_path = PurePath(reporting_path).joinpath(final_report_name)
    final_df.drop(["contact_id"], inplace=True, axis=1)
    logger.info(f"Generating a final report at {final_report_path}")
    final_df.to_excel(final_report_path, index=False, sheet_name=WB_CONTACT_SHEET_NAME)
    logger.info("I am quite done now.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--username",
        help="The Commcare username (email address)",
        dest="commcare_user_name",
    )
    parser.add_argument("--apikey", help="A Commcare API key", dest="commcare_api_key")
    parser.add_argument(
        "--project", help="The Commcare project name", dest="commcare_project_name"
    )
    parser.add_argument(
        "--caseDataPath",
        help="The path where the case data csv file is located",
        dest="legacy_case_data_path",
    )
    parser.add_argument(
        "--dataDictPath",
        help="The path where the data dictionary CSV is located",
        dest="data_dictionary_path",
    )
    parser.add_argument(
        "--reportingPath",
        help="The path where to output reports on what happened to",
        dest="reporting_path",
    )
    parser.add_argument(
        "--requiredOneOfs",
        help=(
            "Space-separated list of columns for which at least one value must be "
            "valid and non-null for a row"
        ),
        dest="required_one_ofs",
        nargs="+",
    )
    parser.add_argument(
        "--contactKeyValDict",
        help=(
            "Additional key/value pairs to add to all uploaded contacts, supplied as "
            "a JSON string. For instance `--contactKeyValDict \"{'key1': 'value1'}\""
        ),
        dest="contact_kwargs",
        type=json.loads,
    )
    args = parser.parse_args()
    try:
        main_with_args(
            args.commcare_user_name,
            args.commcare_api_key,
            args.commcare_project_name,
            args.legacy_case_data_path,
            args.data_dictionary_path,
            args.reporting_path,
            required_one_ofs=args.required_one_ofs,
            **args.contact_kwargs,
        )
    except Exception:
        logger.exception("[main] Something went wrong")
        sys.exit(1)
    sys.exit(0)
