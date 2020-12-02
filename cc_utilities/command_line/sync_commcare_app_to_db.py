import argparse
import json
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import PurePath

from cc_utilities.common import (
    get_application_structure,
    make_commcare_export_sync_xl_wb,
    make_sql_friendly,
)
from cc_utilities.constants import (
    APPLICATION_STRUCTURE_DEFAULT_TIMEOUT,
    COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS,
)
from cc_utilities.logger import get_full_log_file_path, logger

PARENT_PROPERTY_PREFIX = "parent/"


def do_commcare_export_to_db(
    database_url_string,
    commcare_project_name,
    wb_file_path,
    commcare_user_name,
    commcare_api_key,
    commcare_export_script_options,
    commcare_export_script_flags,
):
    """Run `commcare-export` as subprocess to export data to SQL db

    Args:
        database_url_string (str): Full db url to export to
        commcare_project_name (str): The Commcare project being exported from
        wb_file_path (str): Where the workbook with source-column mappings lives
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_export_script_options (dict): A dict of additional args to get passed
            to the `commcare-export` subprocess as command line options.
        commcare_export_script_flags (list): A list of command line flags (with no args)
            to pass to `commcare-export` subprocess.
    """
    commcare_export_script_options = (
        commcare_export_script_options if commcare_export_script_options else {}
    )
    commcare_export_script_flags = (
        commcare_export_script_flags if commcare_export_script_flags else []
    )
    commands = (
        f"commcare-export --output-format sql "
        f"--output {database_url_string} --project {commcare_project_name} "
        f"--query {wb_file_path} --username {commcare_user_name} "
        f"--auth-mode apikey --password {commcare_api_key}"
    )

    additional_options = " ".join(
        [f"--{k} {v}" for (k, v) in commcare_export_script_options.items()]
    )
    additional_flags = " ".join([f"--{flag}" for flag in commcare_export_script_flags])
    commands = " ".join(
        [i for i in (commands, additional_options, additional_flags) if i]
    ).strip()
    commands = commands.split(" ")
    log_file_path = get_full_log_file_path()
    if log_file_path:
        with open(log_file_path, "a") as fl:
            subprocess.run(commands, stderr=fl, stdout=fl)
    else:
        subprocess.run(commands)


def normalize_application_structure_response_data(response_json):
    """The application structure API returns some data that is not helpful for the
    overall use case for this script. This function cleans up the JSON so only relevant
    data remains. Additionally, this function collects the total set of properties
    found

    Args:
        response_json (dict): The Python representation of the JSON returned by the
            Application Structure API.

    Returns:
        dict: The keys are case types, and the values are lists of property names
            associated with the case type. For instance {
                "patient": ["first_name", "last_name", "etc.],
                "contact": ["first_name", "last_name", "phone_number", "etc".]
            }
    """
    normalized = {}
    for module in response_json["modules"]:
        # oddly, there are module types that appear that don't have a case type
        if not module["case_type"]:
            continue
        # the same case type will appear in multiple modules (each version of the app)
        # so we build up the total set of case types...
        if not normalized.get(module["case_type"]):
            normalized[module["case_type"]] = set(
                [
                    prop
                    for prop in module["case_properties"]
                    if not prop.startswith(PARENT_PROPERTY_PREFIX)
                ]
            )
        else:
            normalized[module["case_type"]] = normalized[module["case_type"]].union(
                set(
                    [
                        prop
                        for prop in module["case_properties"]
                        # the app structure api returns a long list of properties
                        # of parents of the given case type. we ignore these
                        if not prop.startswith(PARENT_PROPERTY_PREFIX)
                    ]
                )
            )
    # convert the sets to lists at the end
    return {k: list(v) for (k, v) in normalized.items()}


def save_app_structure_json(structure, save_folder):
    """Save the app structure as a JSON file, twice over.

    The file will be saved once with a date+time based name, and a second time with
    "latest" in the file name. So, for instance, "app_structure_10_20_20_11-43.json"
    and "app_structure_latest.json".

    Args:
        structure (dict): The dictionary to be saved as JSON
        save_folder (str): The folder where the files will be saved.
    """
    date_file_name = f"app_structure_{datetime.now().strftime('%m_%d_%Y_%H-%M')}.json"
    latest_file_name = "app_structure_latest.json"
    for file_name in (date_file_name, latest_file_name):
        full_path = PurePath(save_folder).joinpath(file_name)
        logger.info(f"Saving normalized application structure data at {full_path}")
        with open(full_path, "w") as fl:
            json.dump(structure, fl)


def get_app_case_types_with_properties_from_api(
    commcare_project_name,
    commcare_user_name,
    commcare_api_key,
    commcare_app_id,
    app_structure_api_timeout,
    app_structure_json_save_folder_path=None,
):
    """Get data about each case type and its known properties (historical and current)
        from the Application Structure API.

    Args:

        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be
            imported
        commcare_app_id (str): The ID of the Commcare app.
        app_structure_api_timeout (int): Optional. If provided will override default
            timeout for the call to Application Structure API (which tends to take a
            while)
        app_structure_json_save_folder_path (str): Optional. If provided, the JSON
            returned by the call to the Application Structure API will be saved as a
            JSON file in this folder.
    Returns:
        dict: Whose keys are case types and whose values are lists of property names.
            For instance {
                "patient": ["first_name", "last_name", "etc.],
                "contact": ["first_name", "last_name", "phone_number", "etc".]
            }
    """
    logger.info(
        f"Retrieving application structure for {commcare_project_name} with ID: "
        f"{commcare_app_id} from API. This may take a while."
    )
    app_structure = get_application_structure(
        commcare_project_name,
        commcare_user_name,
        commcare_api_key,
        commcare_app_id,
        app_structure_api_timeout,
    )
    normalized_structure = normalize_application_structure_response_data(app_structure)
    if app_structure_json_save_folder_path:
        save_app_structure_json(
            normalized_structure, app_structure_json_save_folder_path
        )
    return normalized_structure


def load_app_case_types_with_properties_from_json(file_path):
    """Load JSON file with data stored from the application structure API endpoint

    Args:
        file_path (str): the path to a JSON file containing the data
    """
    logger.info(f"Loading application structure data from {file_path}")
    with open(file_path) as fl:
        return json.load(fl)


def generate_source_field_to_target_column_mappings(case_types_with_properties):
    """Generate source property to target column mappings for each case type in...

    ...`case_types_with_properties`. Note that this function also adds a set of default
    mappings that are shared by all case types but that are not included in the data
    returned by the Application Structure API.

    Args:

        case_types_with_properties (dict): Keys are CommCare case types. Values are
            lists of strings of properties for the case type.

    Returns:
        dict: A dictionary whose keys are CommCare case types and whose values are
            lists of tuples, where the first item is the source property (formatted
            in a manner required by the `commcare-export` script, which this script
            calls as a subprocess in a parent scope). For instance:

            {
                "contact": [
                    ("properties.first_name", "first_name"),
                    ("date_close", "date_closed"),
                ],
                "patient": [
                    ("etc", "etc...."),
                ]
            }

    """
    source_to_target_mappings = {}
    for case_type in case_types_with_properties:
        source_to_target_mappings[make_sql_friendly(case_type)] = list(
            set(
                [
                    *COMMCARE_DEFAULT_HIDDEN_FIELD_MAPPINGS,
                    *[
                        (f"properties.{item}", make_sql_friendly(item))
                        for item in sorted(case_types_with_properties[case_type])
                    ],
                ]
            )
        )
    return source_to_target_mappings


def main_with_args(
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    commcare_app_id,
    db_url,
    case_types=None,
    existing_app_structure_json=None,
    app_structure_json_save_folder_path=None,
    app_structure_api_timeout=None,
    commcare_export_script_options=None,
    commcare_export_script_flags=None,
):
    """The main routine.

    Args:
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project to which contacts will be
            imported
        commcare_app_id (str): The ID of the Commcare app.
        db_url (str): Connection string for the db
        case_types (list): Optional. List of case types. If provided, only the provided
            case types will be synced.
        existing_app_structure_json (str): Optional. Path to a JSON blob storing data
            returned by the CommCare Application Structure API endpoint.
        app_structure_json_save_folder_path (str): Optional. If provided, the JSON blob
            saved by a call to the Application Structure API will be saved in a file
            here.
        app_structure_api_timeout (int): Optional. If provided will override default
            timeout for the call to Application Structure API (
            which tends to take a while)
        commcare_export_script_options (dict): Optional. A dict of additional args to
            get passed to the `commcare-export` subprocess as command line options.
        commcare_export_script_flags (list): Optional. A list of command line flags
            (with no args) to pass to `commcare-export` subprocess.
    """
    case_types = case_types if case_types else []

    all_case_types_with_properties = (
        load_app_case_types_with_properties_from_json(existing_app_structure_json)
        if existing_app_structure_json
        # NB: This API call can take a long time: ~2-3 minutes
        else get_app_case_types_with_properties_from_api(
            commcare_project_name,
            commcare_user_name,
            commcare_api_key,
            commcare_app_id,
            app_structure_api_timeout,
            app_structure_json_save_folder_path,
        )
    )
    # if person running script used the `--case-types` property and some of the ones
    # they asked for weren't avaiable, we'll use this to notify them in the logs
    unfound_requested_case_types = list(
        set(case_types).difference(
            set([k for k in all_case_types_with_properties.keys()])
        )
    )
    if case_types and len(unfound_requested_case_types) == len(case_types):
        logger.warn("None of the case types you requested were found")
        return
    if unfound_requested_case_types:
        logger.warn(
            f"Some case types were not found: {', '.join(unfound_requested_case_types)}"
        )
        logger.info("Will continue processing the other requested case types")
    # we'll try to sync the requested case types minus the unfound ones if subset
    # requested, and if no subset requested, we'll sync all found case types
    to_sync_case_types = (
        list(set(case_types).difference(set(unfound_requested_case_types)))
        if case_types
        else list(set([k for k in all_case_types_with_properties.keys()]))
    )
    # filter `all_case_types_with_properties` down to only ones that are in our
    # list of `to_sync_case_types`
    to_sync_case_types_with_properties = {
        k: v
        for (k, v) in all_case_types_with_properties.items()
        if k in to_sync_case_types
    }

    mappings = generate_source_field_to_target_column_mappings(
        to_sync_case_types_with_properties
    )
    # this excel wb file is required by commcare-export which gets called in subprocess
    # by do_commcare_export_to_db
    wb = make_commcare_export_sync_xl_wb(mappings)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_file_path = PurePath(tmpdir).joinpath("mapping.xlsx")
        wb.save(tmp_file_path)
        logger.info("Attempting to sync to db")
        do_commcare_export_to_db(
            db_url,
            commcare_project_name,
            tmp_file_path,
            commcare_user_name,
            commcare_api_key,
            commcare_export_script_options,
            commcare_export_script_flags,
        )
    logger.info("I am quite done now.")


def extract_options_and_flags_to_pass_to_commcare_export(args):
    """Extract optional arguments and flags from args supplied at run time...

    These are meant to be passed to the subprocess call to `commcare-export` that
    causes the db sync to ultimately happen. That script optional args that will be
    useful in some cases.

    Args:
        args (object): The object returned by argparser's `parser.parse_args()`

    Returns:
        A tuple whose first item is key/val pairs of options and whose second item
        is a list of flags
    """
    commcare_export_script_options = {
        "since": args.since,
        "until": args.until,
    }
    # get rid items that are not set and are therefore `None`
    commcare_export_script_options = {
        k: v for (k, v) in commcare_export_script_options.items() if v
    }
    commcare_export_script_flags = []
    # these ones are optional flags that don't have a value.
    # we check to see if they are included when script is called
    for arg in ["verbose", "users", "locations", "with_organization"]:
        if args.__dict__[arg]:
            commcare_export_script_flags.append(arg.replace("_", "-"))
    return commcare_export_script_options, commcare_export_script_flags


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--username",
        help="The Commcare username (email address)",
        dest="commcare_user_name",
    )
    parser.add_argument("--api-key", help="A Commcare API key", dest="commcare_api_key")
    parser.add_argument(
        "--project", help="The Commcare project name", dest="commcare_project_name"
    )
    parser.add_argument(
        "--app-id", help="The ID of the CommCare app", dest="application_id"
    )
    parser.add_argument(
        "--db-url", help="The URL string of the db to sync to",
    )
    parser.add_argument(
        "--case-types",
        help="Optional. Comma-separated list of case types to sync",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "--existing-app-structure-json",
        help=(
            "Optional. Path to a JSON blob storing data returned by the CommCare "
            "Application Structure API endpoint"
        ),
    )
    parser.add_argument(
        "--app-structure-json-save-folder-path",
        help=(
            "Optional. If provided, the JSON blob saved by a call to the Application "
            "Structure API will be saved in a file here."
        ),
    )
    parser.add_argument(
        "--app-structure-api-timeout",
        help="Optional. Seconds for timeout for request to application structure API",
        type=int,
        default=APPLICATION_STRUCTURE_DEFAULT_TIMEOUT,
    )
    parser.add_argument(
        "--since", help="Optional. Export all data after this date. Format YYYY-MM-DD",
    )
    parser.add_argument(
        "--until", help="Optional. Export all up until this date. Format YYYY-MM-DD",
    )
    parser.add_argument(
        "--batch-size",
        help=(
            "Optional. Integer. If included, records will be streamed to the SQL "
            "db in batches of this size"
        ),
        default=5000,
    )
    parser.add_argument(
        "--verbose",
        help="If flag included, logs of the db sync will be verbose.",
        action="store_true",
    )
    parser.add_argument(
        "--users",
        help="If flag included, export table with data about project's mobile workers",
        action="store_true",
    )
    parser.add_argument(
        "--locations",
        help="If flag included, export table with data about project's locations",
        action="store_true",
    )
    parser.add_argument(
        "--with-organization",
        help=(
            "If flag included, export tables containing mobile worker data and "
            "location data and add a commcare_userid field to any exported form or "
            "case"
        ),
        action="store_true",
    )
    args = parser.parse_args()
    try:
        (
            additional_cc_export_options,
            additional_cc_export_flags,
        ) = extract_options_and_flags_to_pass_to_commcare_export(args)

        main_with_args(
            args.commcare_user_name,
            args.commcare_api_key,
            args.commcare_project_name,
            args.application_id,
            args.db_url,
            args.case_types,
            args.existing_app_structure_json,
            args.app_structure_json_save_folder_path,
            args.app_structure_api_timeout,
            commcare_export_script_options=additional_cc_export_options,
            commcare_export_script_flags=additional_cc_export_flags,
        )
    except Exception:
        logger.exception("[sync_commcare_app_to_db.main] Something went wrong")
        sys.exit(1)
    sys.exit(0)
