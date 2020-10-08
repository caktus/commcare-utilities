import argparse
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from openpyxl import Workbook, load_workbook

from cc_utilities.logger import logger

VALID_PROPERTY_NAME = re.compile(r"[\w-]+")
INVALID_SQL_CHARS = re.compile(r"[^\w]")

# For some reason, the same values have different labels in different places in
# CommCare. This list is taken from running a Case Data API query for a single
# case and removing any field names that start with "properties" or "xforms".
# The properties are app specific and are determined via the Case Summary
# Excel file, and I'm not sure what the xforms are.
# https://confluence.dimagi.com/display/commcarepublic/Case+Data
STATIC_CASE_FIELDS = [
    "case_id",
    "closed",
    "closed_by",
    "date_closed",
    "date_modified",
    "domain",
    "id",
    "indexed_on",
    "indices.parent.case_id",
    "indices.parent.case_type",
    "indices.parent.relationship",
    "opened_by",
    "resource_uri",
    "server_date_modified",
    "server_date_opened",
    "user_id",
]

# The CommCare API documentation also includes several "special case properties"
# which appear to be standard for cases, but do require the "properties." prefix.
# https://confluence.dimagi.com/display/commcarepublic/Case+Data
SPECIAL_CASE_PROPERTIES = [
    "owner_id",
    "case_name",
    "external_id",
    "case_type",
    "date_opened",
]


def make_sql_friendly(value):
    """Some CommCare properties and case types include dashes, which make for
    bothersome SQL queries. Remove any non-alphanumeric or underscore
    characters, then return resulting value."""
    # Do not substitute "-" with "_" because in at least once instance, that would
    # result in a duplicate property name ("date_opened").
    return INVALID_SQL_CHARS.sub("", value)


def extract_property_names(case_summary_file, case_types):
    """Get unique property names that appear in the Case Summary Excel file.
    This function iterates over each property on the All Case Properties tab
    and gathers unique property names.
    Args:
        case_summary_file (str): File path to CommCare app case summary
        case_types (list): Case types to extract from the file
    Returns:
        iterator: Iterator of strings of property names
    """
    wb = load_workbook(filename=case_summary_file)
    # iterate through columns A & B, by row
    rows = zip(*wb["All Case Properties"]["A:B"])
    # read the first (header) row
    header_a, header_b = next(rows)
    assert header_a.value == "case_type"
    assert header_b.value == "case_property"
    # read the remaining rows, filtering out any case types we're not looking for
    # and case properties that are not valid XML entities (they are likely
    # "calculated properties" without a property name in the CommCare app)
    properties_by_type = defaultdict(list)
    for case_type, case_property in rows:
        case_type = case_type.value.strip()
        case_property = case_property.value.strip()
        if case_type in case_types and VALID_PROPERTY_NAME.fullmatch(case_property):
            properties_by_type[case_type].append(case_property)
    return properties_by_type


def generate_source_target_mappings(source_columns):
    """Generate mapping of source column names to target column names for db. Prepends
    STATIC_CASE_FIELDS and SPECIAL_CASE_PROPERTIES (defined above) to the output.

    Args:
        source_columns (list): List of source column names

    Returns:
        list: List of tuples where item[0] is source name, and item[1] is target name
    """
    property_names = SPECIAL_CASE_PROPERTIES + sorted(
        prop for prop in source_columns if prop not in SPECIAL_CASE_PROPERTIES
    )
    return [(source_col, source_col) for source_col in STATIC_CASE_FIELDS] + [
        (f"properties.{source_col}", make_sql_friendly(source_col))
        for source_col in property_names
        if source_col not in STATIC_CASE_FIELDS
    ]


def make_commcare_export_sync_xl_wb(mapping):
    """Create an Excel workbook in format required for commcare-export script

    NB: This does not save the workbook, and will need to call wb.save() on object
    returned by this function in order to persist.


    Args:
        mapping (dict): Dictionary of lists of tuples of form
            {"case_type": [("source_name", "target_name)]}

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
    ]
    wb = Workbook()
    for i, (case_type, source_target_mappings) in enumerate(mapping.items()):
        # Some case types have dashes in them, which makes SQL queries more
        # bothersome to write.
        ws_title = make_sql_friendly(case_type)
        if i == 0:
            ws = wb.active
            ws.title = ws_title
        else:
            ws = wb.create_sheet(ws_title)
        ws.append(sheet_headers)
        ws["A2"] = "case"
        ws["B2"] = "type"
        ws["C2"] = case_type

        row_offset = 2
        for idx, item in enumerate(source_target_mappings):
            row_num = idx + row_offset
            # NOTE: Columns F, E are not in the order they appear in Excel (E, F)
            ws[f"F{row_num}"], ws[f"E{row_num}"] = item
    return wb


def get_previous_mapping(state_dir, case_type):
    """Get the previous state of source-target column mappings from a JSON file
    Args:
        state_dir (str): Path to the directory where JSON files were stored
        case_type (str): This is the commcare case type
    Returns:
        mappings (list): List of tuples of form
            ("source_name", "target_name)
    """
    state_path = Path(state_dir).joinpath(f"{case_type}-column-state.json")
    if state_path.exists():
        with open(state_path) as f:
            state = json.load(f)
            return [
                # json will read these as lists, but they need to be tuples for
                # hashing / comparison purposes later on (this is also how the
                # mapping is generated by get_source_and_target_mapping()).
                tuple(x)
                for x in state["column_mappings"]
            ]
    else:
        # Allow state files to be re-generated if needed.
        return []


def save_column_state(state_dir, case_type, mappings):
    """Save the state of source-target column mappings in a JSON file
    Args:
        state_dir (str): Directory where the state file will be saved
        case_type (str): This is the commcare case type
        mappings (list): List of tuples of form
            ("source_name", "target_name)
    Returns: No return, but saves json file to save_path
    """
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir.joinpath(f"{case_type}-column-state.json")
    state = {
        "case_type": case_type,
        "column_mappings": mappings,
        "as_of": datetime.now().strftime("%Y_%m_%d-%H_%M_%S"),
    }
    with open(state_path, "w") as f:
        json.dump(state, f, sort_keys=True, indent=2)
        # Add missing newline at end of file.
        f.write("\n")


def main_with_args(case_summary_file, case_types, output_file_path, state_dir):
    """Do everything required to export and report on commcare export
    Args:
        case_summary_file (str): File path to CommCare app case summary
        case_types (list): The case types (i.e, "contact", "lab_result, etc.)
        output_file_path (str): Where the workbook with source-column mappings lives
    """
    logger.info(f"Retrieving data from {case_summary_file} and extracting column names")
    properties_by_type = extract_property_names(case_summary_file, case_types)

    new_mappings = {
        case_type: generate_source_target_mappings(properties)
        for case_type, properties in properties_by_type.items()
    }

    if state_dir:
        for case_type, new_mapping in new_mappings.items():
            previous_mapping = get_previous_mapping(state_dir, case_type)
            if set(previous_mapping) != set(new_mapping):
                save_column_state(state_dir, case_type, new_mapping)

    logger.info(f"Generating a temporary Excel workbook to {output_file_path}")
    wb = make_commcare_export_sync_xl_wb(new_mappings)
    wb.save(output_file_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--case-summary-file",
        help="File path to CommCare app case summary",
        dest="case_summary_file",
    )
    parser.add_argument(
        "--case-types",
        help='Space-separated list case types (e.g., "patient lab_result contact investigation")',
        dest="case_types",
        nargs="+",
    )
    parser.add_argument(
        "--state-dir",
        help="The directory where a JSON representation of the column state should "
        "be read/saved for each case type (optional)",
        dest="state_dir",
    )
    parser.add_argument(
        "--output",
        help="The file path to the Excel query file output that will be created",
        dest="output_file_path",
    )
    args = parser.parse_args()
    main_with_args(
        args.case_summary_file, args.case_types, args.output_file_path, args.state_dir
    )