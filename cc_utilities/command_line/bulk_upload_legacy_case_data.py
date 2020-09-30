import argparse
import sys
from datetime import datetime
from pathlib import Path, PurePath

import pandas as pd

# from cc_utilities.common import upload_data_to_commcare
from cc_utilities.legacy_upload import (  # normalize_legacy_case_data,
    LegacyUploadError,
    get_lookup_ids_for_project_slug,
    load_data_dict,
    validate_case_data_columns,
    validate_legacy_case_data,
)
from cc_utilities.logger import logger


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    project_slug,
    legacy_case_data_path,
    legacy_case_data_tab_name,
    project_lookup_table_path,
    project_derived_ids,
    data_dictionary_path,
    reporting_path,
    reject_all_if_any_invalid_rows=True,
):
    """The main routine

    Args:
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project being exported from
    """
    required_lookup_ids = get_lookup_ids_for_project_slug(
        project_slug, project_lookup_table_path, project_derived_ids
    )

    if any(val is None for val in required_lookup_ids.values()):
        missing = [k for (k, v) in required_lookup_ids.items()]
        message = (
            f"One or more required project_derived_ids was missing, specifically: "
            f"{', '.join(missing)}"
        )
        logger.error(message)
        raise LegacyUploadError(message)

    data_dict = load_data_dict(data_dictionary_path)

    case_data_df = pd.read_excel(
        legacy_case_data_path, sheet_name=legacy_case_data_tab_name
    )

    validate_case_data_columns(
        case_data_df.columns,
        data_dict.keys(),
        [
            key
            for key in data_dict
            if data_dict[key]["required"] in ("true", "True", "TRUE", True, 1, "1")
        ],
    )

    case_data_df = validate_legacy_case_data(case_data_df, data_dict)

    validation_report_name = (
        f"{Path(legacy_case_data_path).stem}_validation_report_"
        f"{datetime.now().strftime('%m-%d-%Y_%H-%M')}.xlsx"
    )
    validation_report_path = PurePath(reporting_path).join(validation_report_name)
    case_data_df.to_excel(validation_report_path)

    num_invalid = len(case_data_df[~case_data_df["is_valid"]])
    if case_data_df["is_valid"].all() is False and reject_all_if_any_invalid_rows:
        msg = (
            f"{num_invalid} rows were invalid and `reject_all_if_an_invalid_rows` "
            f"is True. No case data will be uploaded. See details in the validation "
            f"report at {validation_report_path}."
        )
        logger.warn(msg)
        sys.exit(0)

    # validated_case_data_df = case_data_df[case_data_df["is_valid"]]
    # normalized_case_data_df = normalize_legacy_case_data(
    #     validated_case_data_df, data_dict
    # )
    import pdb

    pdb.set_trace()
    # upload_legacy_contacts_to_commcare(
    #     normalized_case_data_df,
    #     project_slug,
    #     commcare_user_name,
    #     commcare_api_key,
    # )


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
        help="The path where the case data Excel file is located",
        dest="legacy_case_data_path",
    )
    parser.add_argument(
        "--caseDataTab",
        help="The name of the Excel sheet in the case data workbook",
        dest="legacy_case_data_tab_name",
    )
    parser.add_argument(
        "--projectLookupPath",
        help="The path where the CSV mapping projects to owner_id etc. lives",
        dest="project_lookup_table_path",
    )
    parser.add_argument(
        "--projectIdsList",
        help=(
            "Comma-separated list of column names to look up by project name in "
            "the project lookup table"
        ),
        dest="project_derived_ids",
        type=str,
    )
    parser.add_argument(
        "--dataDictPath",
        help="The path where the data dictionary CSV is located",
        dest="project_lookup_table_path",
    )
    parser.add_argument(
        "--reportingPath",
        help="The path where to output reports on what happened to",
        dest="reporting_path",
    )
    args = parser.parse_args()
    project_derived_ids = [item.strip() for item in args.project_derived_ids.split(",")]
    main_with_args(
        args.commcare_user_name,
        args.commcare_api_key,
        args.commcare_project_name,
        args.legacy_case_data_path,
        args.legacy_case_data_tab_name,
        args.project_lookup_table_path,
        project_derived_ids,
        args.data_dictionary_path,
        args.reporting_path,
    )
