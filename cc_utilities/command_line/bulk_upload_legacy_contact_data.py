import argparse
import json
import sys
from datetime import datetime
from pathlib import Path, PurePath

import pandas as pd

from cc_utilities.legacy_upload import (
    generate_commcare_case_report_url,
    generate_commcare_external_id,
    load_data_dict,
    normalize_legacy_case_data,
    upload_legacy_contacts_to_commcare,
    validate_case_data_columns,
    validate_legacy_case_data,
)
from cc_utilities.logger import logger


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    legacy_case_data_path,
    data_dictionary_path,
    reporting_path,
    reject_all_if_any_invalid_rows=True,
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
        commcare_project_name (str): The Commcare project being exported from
        legacy_case_data_path (str): Path to a CSV containing contacts to be imported
        data_dictionary_path (str): Path to a CSV of a data dict used to validate user-
            supplied contact data. Note that this asset is based on but distinct from
            the data dict provided in the CommCare dashboard. It should have the
            following columns: field, group, allowed_values, data_type, and required
        reporting_path (str): A folder to output reports indicating what happened.
        reject_all_if_any_invalid_rows (bool): If `True`, if any rows in case data CSV
            contain invalid data, the entire routine will be halted.
        contact_kwargs (dict): Optional key/value pairs that will be added to each
            generated contact.
    """
    logger.info(f"Loading data dictionary at {data_dictionary_path}")
    data_dict = load_data_dict(data_dictionary_path)
    # Pandas infers data types, and that's not helpful in this context. For validation
    # and normalization purposes, we need all inputs to be strings.
    logger.info(f"Loading legacy contact data at {validate_legacy_case_data}")
    raw_case_data_df = pd.read_csv(legacy_case_data_path, keep_default_na=False).astype(
        "string"
    )
    logger.info("Validating columns in legacy contact data CSV against data dictionary")
    if (
        validate_case_data_columns(
            raw_case_data_df.columns,
            data_dict.keys(),
            [
                key
                for key in data_dict
                if data_dict[key]["required"] in ("true", "True", "TRUE", True, 1, "1")
            ],
        )
        is False
    ):
        logger.error(
            "Columns in case data were invalid, either because of unexpected column "
            "names or because required column names were missing. Compare column "
            "names to data dict expectations"
        )
        sys.exit(1)

    # Even though we converted to string above, there will still be NA values
    # so here we convert those to empty string when we pass in to the validation
    # function.
    logger.info(
        "Validating row values in legacy contact data CSV against data dictionary"
    )
    case_data_df = validate_legacy_case_data(raw_case_data_df.fillna(""), data_dict)
    validation_report_name = (
        f"{Path(legacy_case_data_path).stem}_validation_report_"
        f"{datetime.now().strftime('%m-%d-%Y_%H-%M')}.xlsx"
    )
    validation_report_path = PurePath(reporting_path).joinpath(validation_report_name)
    report_df = raw_case_data_df.merge(
        case_data_df[["is_valid", "validation_problems"]],
        left_index=True,
        right_index=True,
    )
    logger.info(f"Generating validation report at {validation_report_path}")
    report_df.to_excel(validation_report_path, index=False)
    num_invalid = len(case_data_df[~case_data_df["is_valid"]])
    if case_data_df["is_valid"].all() is False and reject_all_if_any_invalid_rows:
        msg = (
            f"{num_invalid} rows were invalid and `reject_all_if_an_invalid_rows` "
            f"is True. No case data will be uploaded. See details in the validation "
            f"report at {validation_report_path}."
        )
        logger.warn(msg)
        sys.exit(1)

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
        f"{Path(legacy_case_data_path).stem}_final_report_"
        f"{datetime.now().strftime('%m-%d-%Y_%H-%M')}.xlsx"
    )
    final_report_path = PurePath(reporting_path).joinpath(final_report_name)
    final_df.drop(["contact_id"], inplace=True, axis=1)
    logger.info(f"Generating a final report at {final_report_path}")
    final_df.to_excel(final_report_path, index=False)
    logger.info("I am quite done now.")
    sys.exit(0)


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
        "--contactKeyValDict",
        help=(
            "Additional key/value pairs to add to all uploaded contacts, supplied as "
            "a JSON string. For instance `--contactKeyValDict \"{'key1': 'value1'}\""
        ),
        dest="contact_kwargs",
        type=json.loads,
    )
    args = parser.parse_args()
    main_with_args(
        args.commcare_user_name,
        args.commcare_api_key,
        args.commcare_project_name,
        args.legacy_case_data_path,
        args.data_dictionary_path,
        args.reporting_path,
        **args.contact_kwargs,
    )
