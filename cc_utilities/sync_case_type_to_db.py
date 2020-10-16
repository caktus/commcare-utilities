import subprocess

from cc_utilities.common import get_application_structure
from cc_utilities.logger import logger


def get_case_property_names_from_application_data(
    case_type,
    commcare_user_name,
    commcare_api_key,
    commcare_project_name,
    commcare_app_id,
    pinned_version=None,
):
    """Get unique properties that appear in Application Data API endpoint data.

    Unless a `pinned_version` is included, this function iterates over each version
    of the app returned by the API, and collects the set of all property names across
    versions. If called with a pinned_version, it will only take properties for

    Args:
        case_type (str): The case type to retrieve properties for
        commcare_user_name (str): Email address associated with commcare
        commcare_api_key (str): API keyfor the email address
        commcare_project_name (str): The name of the Commcare project space
        commcare_app_id (str): The ID of the app
        pinned_version (str): Defaults to `None`. If included, this function will only
            retrieve the properties found on the specified version of the app from the
            data. If `pinned_version` is included, and that app version is not found,
            this function raises an error.

    Returns:
        list: List of strings of property names
    """
    data = get_application_structure(
        commcare_project_name, commcare_user_name, commcare_api_key, commcare_app_id
    )
    import pdb

    pdb.set_trace()
    # handled pinned version and case type
    property_names = set()
    return list(property_names)


# def get_unseen_property_names(old_names, new_names):
#     """Get list of previously unseen property names

#     Args:
#         old_names (list): List of previously seen property names
#         new_names (list): List of property names from new data

#     Returns:
#         list: List of strings of new property names
#     """
#     harmonized_old_names = [item.split("properties.")[-1] for item in old_names]
#     return list(set(new_names).difference(set(harmonized_old_names)))


# def get_source_and_target_mapping(source_column_name):
#     """Map source column name to target column name."""

#     # The case ID field is special and is mapped to an "id" column in the SQL
#     # database, which will be used as the primary key.
#     if source_column_name in ("caseid", "case_id"):
#         return (source_column_name, "id")
#     # The "closed" attribute doesn't seem to be a "property" as far as CommCare is
#     # concerned, but rather, a common attribute on all cases. (There may be others,
#     # but we haven't found them yet.)
#     if source_column_name == "closed":
#         return (source_column_name, source_column_name)
#     # All other field names are assumed to be "properties" in CommCare and need to be
#     # prefixed with "properties.".
#     return (f"properties.{source_column_name}", source_column_name)


# def generate_source_target_mappings(
#     source_columns, transform_function=get_source_and_target_mapping
# ):
#     """Generate mapping of source column names to target column names for db

#     Args:
#         source_columns (list): List of source column names
#         transform_function (fn): Function to apply to each source column name to
#             derive target column name. Defaults to `transform_source_to_target`.

#     Returns:
#         list: List of tuples where item[0] is source name, and item[1] is target name
#     """
#     return [transform_function(source_col) for source_col in sorted(source_columns)]


# def make_commcare_export_sync_xl_wb(source_target_mappings, filter_value):
#     """Create an Excel workbook in format required for commcare-export script

#     NB: This does not save the workbook, and will need to call wb.save() on object
#     returned by this function in order to persist.


#     Args:
#         source_target_mappings (list): List of tuples of form
#             ("source_name", "target_name)
#         filter_value (str): This is the case type, and gets added as the "Filter value"
#             in the workbook, as well as the worksheet name.

#     Returns:
#         obj: An Openpyxl workbook
#     """
#     sheet_headers = [
#         "Data Source",
#         "Filter Name",
#         "Filter Value",
#         "",
#         "Field",
#         "Source Field",
#         "Alternate Source Field 1",
#     ]
#     wb = Workbook()
#     ws = wb.active
#     ws.title = filter_value
#     ws.append(sheet_headers)
#     ws["A2"] = "case"
#     ws["B2"] = "type"
#     ws["C2"] = filter_value

#     row_offset = 3
#     # this is the only column name that we need to list Alternate Source Field 1 for
#     # at the moment
#     ws["E2"], ws["F2"], ws["G2"] = ("id", "caseid", "case_id")

#     mappings = [
#         item for item in source_target_mappings if item[0] not in ("caseid", "case_id")
#     ]
#     for idx, item in enumerate(mappings):
#         row_num = idx + row_offset
#         ws[f"F{row_num}"], ws[f"E{row_num}"] = item

#     return wb


# def save_column_state(save_path, filter_value, mappings):
#     """Save the state of source-target column mappings in a JSON file

#     Args:
#         save_path (str): Path that the file will be saved to
#         filter_value (str): This is the commcare case type
#         mappings (list): List of tuples of form
#             ("source_name", "target_name)

#     Returns: No return, but saves json file to save_path
#     """
#     state = {
#         "filter_value": filter_value,
#         "column_mappings": mappings,
#         "as_of": datetime.now().strftime("%Y_%m_%d-%H_%M_%S"),
#     }
#     with open(save_path, "w") as f:
#         json.dump(state, f, sort_keys=True, indent=2)
#         # Add missing newline at end of file.
#         f.write("\n")


# def do_commcare_export_to_db(
#     database_url_string,
#     commcare_project_name,
#     wb_file_path,
#     commcare_user_name,
#     commcare_api_key,
# ):
#     """Run `commcare-export` as subprocess to export data to SQL db

#     Args:
#         database_url_string (str): Full db url to export to
#         commcare_project_name (str): The Commcare project being exported from
#         wb_file_path (str): Where the workbook with source-column mappings lives
#         commcare_user_name (str): The Commcare username (email address)
#         commcare_api_key (str): A Commcare API key for the user
#     """
#     commands = (
#         f"commcare-export --output-format sql "
#         f"--output {database_url_string} --project {commcare_project_name} "
#         f"--query {wb_file_path} --username {commcare_user_name} "
#         f"--auth-mode apikey --password {commcare_api_key}"
#     ).split(" ")
#     subprocess.run(commands)

# def sync_case_type_to_db(db_url, ):
#     pass