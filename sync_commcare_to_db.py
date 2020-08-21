import argparse
import json
import os
import subprocess
from datetime import datetime

import requests
from openpyxl import Workbook


def extract_property_names_from_case_data(
    feed_url, commcare_user_name, commcare_api_key
):
    """Get unique property names that appear in OData feed JSON objects

    This function iterates over each case data item in the feed
    and gathers unique property names.

    Args:
        feed_url (str): URL of Commcare data feed to retrieve
        commcare_user_name (str): Email address associated with commcare
        commcare_api_key (str): API keyfor the email address

    Returns:
        list: List of strings of property names
    """
    response = requests.get(feed_url, auth=(commcare_user_name, commcare_api_key))
    cases = response.json()["value"]
    property_names = set()
    for case in cases:
        property_names.update(set(case.keys()))
    return list(property_names)


def get_unseen_property_names(old_names, new_names):
    """Get list of previously unseen property names

    Args:
        old_names (list): List of previously seen property names
        new_names (list): List of property names from new data

    Returns:
        list: List of strings of new property names
    """
    harmonized_old_names = [item.split("properties.")[-1] for item in old_names]
    return list(set(new_names).difference(set(harmonized_old_names)))


def get_source_and_target_mapping(source_column_name):
    """Map source column name to target column name.
    """

    # Some source names unpredictably have form "properties.properties" (and further?)
    # so we split on "properties." and take the final item in list after that split
    # to get at any deeply nested properties when forming target column name. This
    # prevents column names such as "properties.foo" and also keeps column names
    # at 63 or fewer characters, which is Postgres' limit. Postgres on its own
    # will truncate > 63, but the commcare-export script raises error and enforces
    # the limit of the db it's syncing to.
    if source_column_name in ("caseid", "case_id"):
        return (source_column_name, "id")
    if source_column_name == "closed":
        return (source_column_name, source_column_name)
    else:
        return (
            f"properties.{source_column_name}",
            source_column_name.split("properties.")[-1],
        )


def generate_source_target_mappings(
    source_columns, transform_function=get_source_and_target_mapping
):
    """Generate mapping of source column names to target column names for db

    Args:
        source_columns (list): List of source column names
        transform_function (fn): Function to apply to each source column name to
            derive target column name. Defaults to `transform_source_to_target`.

    Returns:
        list: List of tuples where item[0] is source name, and item[1] is target name
    """
    return [transform_function(source_col) for source_col in sorted(source_columns)]


def make_commcare_export_sync_xl_wb(source_target_mappings, filter_value):
    """Create an Excel workbook in format required for commcare-export script

    NB: This does not save the workbook, and will need to call wb.save() on object
    returned by this function in order to persist.


    Args:
        source_target_mappings (list): List of tuples of form
            ("source_name", "target_name)
        filter_value (str): This is the case type, and gets added as the "Filter value"
            in the workbook, as well as the worksheet name.

    Returns:
        obj: An Openpyxl workbook
    """
    sheet_headers = [
        "Data Source",
        "Filter Name",
        "Filter Value",
        "",
        "Field",
        "Source Field",
        "Alternate Source Field 1",
    ]
    wb = Workbook()
    ws = wb.active
    ws.title = filter_value
    ws.append(sheet_headers)
    ws["A2"] = "case"
    ws["B2"] = "type"
    ws["C2"] = filter_value

    row_offset = 3
    # this is the only column name that we need to list Alternate Source Field 1 for
    # at the moment
    ws["E2"], ws["F2"], ws["G2"] = ("id", "caseid", "case_id")

    mappings = [
        item for item in source_target_mappings if item[0] not in ("caseid", "case_id")
    ]
    for idx, item in enumerate(mappings):
        row_num = idx + row_offset
        ws[f"F{row_num}"], ws[f"E{row_num}"] = item

    return wb


def save_column_state(save_path, filter_value, mappings):
    """Save the state of source-target column mappings in a JSON file

    Args:
        save_path (str): Path that the file will be saved to
        filter_value (str): This is the commcare case type
        mappings (list): List of tuples of form
            ("source_name", "target_name)

    Returns: No return, but saves json file to save_path
    """
    state = {
        "filter_value": filter_value,
        "column_mappings": mappings,
        "as_of": datetime.now().strftime("%Y_%m_%d-%H_%M_%S"),
    }
    with open(save_path, "w") as f:
        json.dump(state, f)


def do_commcare_export_to_db(
    database_url_string,
    commcare_project_name,
    wb_file_path,
    commcare_user_name,
    commcare_api_key,
):
    """Run `commcare-export` as subprocess to export data to SQL db

    Args:
        database_url_string (str): Full db url to export to
        commcare_project_name (str): The Commcare project being exported from
        wb_file_path (str): Where the workbook with source-column mappings lives
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
    """
    commands = (
        f"commcare-export --output-format sql "
        f"--output {database_url_string} --project {commcare_project_name} "
        f"--query {wb_file_path} --username {commcare_user_name} "
        f"--auth-mode apikey --password {commcare_api_key}"
    ).split(" ")
    subprocess.run(commands)


def main(
    feed_url,
    filter_value,
    database_url_string,
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    previous_column_state_path=None,
    wb_file_path=None,
):
    """Do everything required to export and report on commcare export

    Args:
        feed_url (str): URL of Commcare OData feed
        filter_value (str): The type of case (i.e, "contact", "lab_result, etc.)
        database_url_string (str): Full db url to export to
        commcare_user_name (str): The Commcare username (email address)
        commcare_api_key (str): A Commcare API key for the user
        commcare_project_name (str): The Commcare project being exported from
        previous_column_state_path (str): the path to a previous column state report
        wb_file_path (str): Where the workbook with source-column mappings lives

    """
    print(f"Retrieving data from {feed_url} and extracting column names")
    column_names = extract_property_names_from_case_data(
        feed_url, commcare_user_name, commcare_api_key
    )
    state_path = (
        previous_column_state_path
        if previous_column_state_path
        else os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "sync_commcare_assets",
            f"{filter_value}-column-state.json",
        )
    )
    with open(state_path) as f:
        state = json.load(f)
        previously_seen_columns = [item[0] for item in state["column_mappings"]]
    new = get_unseen_property_names(previously_seen_columns, column_names)
    new_set = set(new)
    new_set.update(set(previously_seen_columns))

    mappings = generate_source_target_mappings(list(new_set))
    if len(new) > 0:
        print(f"Saving new column state in the directory: {state_path}")
        save_column_state(state_path, filter_value, mappings)

    wb_file_path = (
        wb_file_path
        if wb_file_path
        else os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "sync_commcare_assets",
            f"{filter_value}-source-target-mappings.xlsx",
        )
    )
    print(
        f"Generating a temporary Excel workbook for {filter_value} "
        f"to the directory {wb_file_path}"
    )
    wb = make_commcare_export_sync_xl_wb(mappings, filter_value)
    wb.save(wb_file_path)

    print(f"Syncing data from Commcare to db at {database_url_string}")
    do_commcare_export_to_db(
        database_url_string,
        commcare_project_name,
        wb_file_path,
        commcare_user_name,
        commcare_api_key,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--feed-url", help="URL of Commcare OData feed", dest="feed_url"
    )
    parser.add_argument(
        "--case-type",
        help='The type of case (i.e, "contact", "lab_result, etc.)',
        dest="filter_value",
    )
    parser.add_argument(
        "--db",
        help="The db url string of the db that data will be exported to",
        dest="database_url_string",
    )
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
        "--previous-report",
        help="The path to a previous column state report",
        dest="previous_column_state_path",
    )
    args = parser.parse_args()
    main(
        args.feed_url,
        args.filter_value,
        args.database_url_string,
        args.commcare_user_name,
        args.commcare_api_key,
        args.commcare_project_name,
        args.previous_column_state_path,
    )
