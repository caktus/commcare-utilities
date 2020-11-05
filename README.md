# commcare-utilities

<!-- markdownlint-disable no-inline-html -->
<p align="left">
  <a href="https://github.com/caktus/commcare-utilities/actions?query=workflow%3A%22Test%22"><img alt="test status badge" src="https://github.com/caktus/commcare-utilities/workflows/Test/badge.svg"></a>
</p>
<!-- markdownlint-enable no-inline-html -->

This repo is for an assortment of scripts for developers working with Commcare.

- [commcare-utilities](#commcare-utilities)
  - [Setup](#setup)
  - [Tests](#tests)
  - [Scripts](#scripts)
    - [`sync-commcare-app-to-db`](#sync-commcare-app-to-db)
    - [`generate-case-export-query-file`](#generate-case-export-query-file)
    - [`process-numbers-for-sms-capability`](#process-numbers-for-sms-capability)
    - [`bulk-upload-legacy-contact-data`](#bulk-upload-legacy-contact-data)
      - [The workflow](#the-workflow)
      - [Creating the data dict](#creating-the-data-dict)
      - [Running the script](#running-the-script)
    - [`sync-redcap-to-commcare`](#sync-redcap-to-commcare)
  - [Logging](#logging)

## Setup

1. Create and source a virtual environment.
2. `pip3 install -e .`
3. Check if there are additional dependencies required for any of the scripts you wish to run (see below), and install them.
4. Enjoy.

## Tests

To run tests, from root of repo, do:

```bash
tox
```

## Scripts

### `sync-commcare-app-to-db`

This script allows a user to automatically backup one or more case types from a CommCareHQ project into a SQL database. It does so by calling the Application Structure API to retrieve data about all cases and their properties (past and present). Based on this data, it generates an Excel file mapping observed property names to target SQL db column names before calling `commcare-export` (a separate Python package maintained by Dimagi) as a subprocess with this Excel file as a parameter. During this final step, there will be logs indicating any new table-column combinations that were added to the database.

While additional options are available, it's worth calling out two important, related command line arguments here: `--app-structure-json-save-folder-path` and `--existing-app-structure-json`. By providing an appropriate value for the `--app-structure-json-save-folder-path` option, the script will save a JSON blob of (cleaned up) data returned by the application structure API. Calls to this API endpoint are time consuming, so in subsequent runs of the script, this file can be referenced via the `--existing-app-structure-json` option. In this case, the script will not make a call to the Application Structure API and will instead rely on the saved JSON data. Keep in mind that that the database will not learn about any new properties that have been added to a case type if it doesn't call the API.

Available command line arguments and flags:

- `--username`: Mandatory. The Commcare username (email address)
- `--api-key`: Mandatory. An API key associated with username
- `--project`: Mandatory. The Commcare projecct name
- `--app-id`: Mandatory. The ID of the Commcare app
- `--db-url`: Mandatory. The URL string of the db to sync to
- `--case-types`: Optional. Space-separated list of case types to sync. If not included, all available case types will be synced.
- `--app-structure-json-save-folder-path`: Optional. Path to a folder in which to save (normalized) JSON data returned by a call to the Application Structure API.
- `--existing-app-structure-json`: Optional. Path to JSON file containing normalized application structure data. If included, the script will not make a call to the Application Structure API and will instead use the data contained in this file.
- `--app-structure-api-timeout` - Optional. Seconds for timeout for request to application structure API. Defaults to value stored in `constants.APPLICATION_STRUCTURE_DEFAULT_TIMEOUT`
- `--since` - Optional. Export all data after (but not including) this date . Format YYYY-MM-DD
- `--until` - Optional. Export all data up until (but not including) this date. Format YYYY-MM-DD
- `--batch-size` - Optional. Integer. If included, records will be streamed to the SQL db in batches of this size
- `--verbose` - If flag included, logs of the db sync will be verbose
- `--users` - If flag included, export table with data about project's mobile workers
- `--locations` - If flag included, export table with data about project's locations
- `--with-organization` - If flag included, export tables containing mobile worker data and location data and add a commcare_userid field to any exported form or case

Also noteworthy: When `commcare-export` encounters properties that do not have >=1 non-empty value in the source data, it will not add a column for that property type to the database. If on subsequent runs at least one case has been added with a non-null for the property, the property will be added as a column.

**Before running the script**:

In order to run this script, you will need to grab the ID for the app your trying to sync. To do this, log into the CommCareHQ dashboard, select your application from the `Applications` dropdown. Your app ID will be in the URL of the resulting page you're taken to:

```text
https://www.commcarehq.org/a/<project-name>/apps/view/<this-is-your-app-id>/
```

Additionally, you'll need to install the correct driver for your SQL database (i.e., `pyodbc` for MSSQL or `pyscogp2` for Postgres), as this script relies on SQLAlchemy behind the scenes, which in turn requires that the correct database driver be supplied.

**Running the script:**

Running the script as follows would sync all discovered case types and save a JSON representing the application structure in the folder specified by `--app-structure-json-save-folder-path`:

```linux
sync-commcare-app-to-db \
  --username $COMMCARE_USER \
  --apikey $COMMCARE_API_KEY \
  --project $COMMCARE_PROJECT_NAME \
  --app-id $APPLICATION_ID \
  --db-url $DB_URL \
  --app-structure-json-save-folder-path $SAVE_FOLDER_PATH
```

To specify only a subset of case types — for instance, contact and patient — you could run:

```linux
sync-commcare-app-to-db \
  --username $COMMCARE_USER \
  --apikey $COMMCARE_API_KEY \
  --project $COMMCARE_PROJECT_NAME \
  --app-id $APPLICATION_ID \
  --db-url $DB_URL \
  --app-structure-json-save-folder-path $SAVE_FOLDER_PATH
  --case-types contact patient
```

To use a pre-existing JSON file and avoid making a request to the Application Structure API, you could run:

```linux
sync-commcare-app-to-db \
  --username $COMMCARE_USER \
  --api_key $COMMCARE_API_KEY \
  --project $COMMCARE_PROJECT_NAME \
  --app-id $APPLICATION_ID \
  --db-url $DB_URL \
  --existing-app-structure-json $JSON_FILE_PATH \
```

### `generate-case-export-query-file`

This script allows a user to generate an Excel query file to facilitate exporting data from CommCare to a SQL database or other local data store. It does by iterating over all records returned in the supplied [Case Summary](https://confluence.dimagi.com/display/commcarepublic/App+Summary#AppSummary-CaseSummary) Excel file to build a list of all observed property names to be turned into column names in a SQL db.

An Excel workbook is created [as required by `commcare-export`](https://confluence.dimagi.com/display/commcarepublic/CommCare+Data+Export+Tool#CommCareDataExportTool-HowtoGenerateanExcelQueryFile), containing source to target column mappings. A separate tab is created for each case type. This same information is stored in a JSON, to make results auditable without requiring Excel. The JSON files are for informational purposes only; they can, for example, be checked into version control in a separate repository to help identify and provide a log of changes to the columns in the database over time. The JSON files are not used as an input to the process, so it is possible for fields to be removed if they are deprecated in the CommCare app.

**Running the script:**

1. Navigate to the [Case Summary](https://confluence.dimagi.com/display/commcarepublic/App+Summary#AppSummary-CaseSummary) page (under App Summary) in the CommCare web interface, and download the corresponding Excel file. It should have an "All Case Properties" tab (this is the only tab that is needed).
2. Run the script, specifying the input file, desired case type(s), and output locations. For instance, to export "patient" and "contact" case records:

  ```linux
   CASE_SUMMARY_FILE="MyApp - All Case Properties.xlsx"
   STATE_DIR="repo/export_query_files/commcare-project-name/"
   OUTPUT_FILE="${STATE_DIR}query_file.xlsx"

   generate-case-export-query-file --case-summary-file "$CASE_SUMMARY_FILE" --case-type patient contact --state-dir $STATE_DIR --output $OUTPUT_FILE
   ```

3. Run the `commcare-export` tool as provided in [its documentation](https://confluence.dimagi.com/display/commcarepublic/CommCare+Data+Export+Tool). Any new columns added to the DB will be noted in the command-line output of the script.

Note the following oddity: When `commcare-export` encounters properties that do not have >=1 non-empty value in the source data, it will not add a column for that property type to the database. If on subsequent runs at least one case has been added with a non-null for the property, the property will be added as a column. This behavior was observed in a Postgres db; other flavors of SQL were not tested. This means that the source-to-target mappings that are indicated in the JSON and Excel files are not a record of what was actually synced to the db, only what was attempted.

### `process-numbers-for-sms-capability`

This script allows a user to run unprocessed contact phone numbers through the [Twilio Lookup API](https://www.twilio.com/docs/lookup/api) in order to determine if contacts can be reached by SMS. To do this, it queries a database for unprocessed contacts, queries the Twilio Lookup API for each number, then uses the [CommCare bulk upload API](https://confluence.dimagi.com/display/commcarepublic/Bulk+Upload+Case+Data) to update the `contact_phone_can_receive_sms` property on these cases.

Note that this script does not update the database it originally queries.

Also note that when this script encounters numbers that either a.) cannot be parsed to generate the standard format required by Twilio, or b.) are not found to be a valid number by the Twilio API, the script marks these numbers as not capable of receiving SMS, and logs a warning to a log file.

Finally, note that this script presently is configured to work with US-based phone numbers and any non-US numbers it encounters will marked as not able to receive SMS.

**Running the script:**

1. Create a Twilio account if you don't already have one.
2. Gather your Twilio SID and auth token.
3. Install the appropriate db engine library for your database. If you're not sure what that is, run the script without doing this, and you'll get a `ModuleNotFoundError` with the name of the required library.
4. Optionally, copy over `sample.env` to `.env` and insert appropriate values. Source those values before the next step.
5. Run the script. Assuming the referenced variables are set: `process-numbers-for-sms-capability --db $DB_URL --username $COMMCARE_USER --apikey $COMMCARE_API_KEY --project $COMMCARE_PROJECT --twilioSID $TWILIO_SID --twilioToken $TWILIO_TOKEN`.
6. Any new columns added to the DB will be noted in the command-line output of the script.


### `bulk-upload-legacy-contact-data`

This script allows a user to bulk upload legacy contact data into a CommCare project. Its input is a CSV of contacts to be imported, where each column in the CSV is a valid CommCare field for the project instance, along with a data dictionary CSV which is used to validate the contact data to be uploaded.


#### The workflow

> **NOTE** This workflow contains the exchange of sensitive PII, so security should be a key conern for all parties involved in preparing, exchanging, procesing this data. The person running this script should, amongst other things, aim to security risks by deleting input data and generated reports as soon as possible after uploading contacts. This person should also be clear with the non-technical stakeholder who is providing the data that security should be a top concern.

At a high-level, this script is intended to support the following workflow:

1. A non-technical stakeholder (for instance a point of contact at a public health agency) creates a CSV file (or an Excel file that will later be transformed into a CSV) containing a row for each legacy contact they want to import. In creating this asset, they should consult the data dictionary in their CommCare instance to determine which fields they would like to upload. Ultimately, the column names in the CSV will need to correspond to non-deprecated field names in their dictionary. For any column/row combination, the values supplied will be validated according to the data type of the column and the user-supplied value.  After producing this asset, the non-technical stakeholder shares it with a technical stakeholder who has API access to the CommCare instance.
2. The technical stakeholder produces a data dictionary CSV, which is based on but modifies the data dictionary export available in CommCare instances. Ultimately, the technical stakeholder will need to create a data dict csv with the following column headers: `field`, `allowed_values`, `data_type` and `required`. Detailed instructions on how to produce this asset are found in the next subsection. Once created, this asset will need to be stored on the same computer that is being used to run this script.
3. The technical stakeholder runs the legacy upload script pointing to the legacy contact data and the data dictionary.
4. The script checks to see that only expected column names were encountered, and whether any required column names were missing. If it encounters problems with the columns, this will be logged. The technical stakeholder will then fix these problems if they are obvious (say a misspelled column name) or else reach out to the non-technical stakeholder who provided the data and ask them to resolve the issue and provide updated data.
5. Assuming the previous validation succeeds, the script next validates each row of data. It does this by cross-referencing the column name of each row value against the data dictionary in order to determine the data type and in the case of select and multi-select data types, the allowed values. The script outputs an Excel file with all of the original data plus 2 new columns: `is_valid` which will contain a boolean indicating whether or not the row validated; and `validation_problems` which will be text describing any validation problems encountered for the row.
6. If row-level validation problems were encountered, the script will exit after creating the validation report. Depending on the problems encountered, the technical user may fix them on their own (say, for instance, for a select field whose values are "yes", "no", and "maybe", there is a row where the typo "yess" appears), or they may return the validation report back to the original user who uploaded the data, asking them to fix the reported issues.
7. If no validation problems are encountered, the script normalizes row values (for instance, converting date columns to the formatting expected by CommCare).
8. The next processes contacts in batches of 100. Legacy contacts must be attached to a parent case, and for this, the script creates a stub patient that gets to attached to a batch of up to 100 contacts. For a given batch of <= 100 contacts, the script creates a stub patient, retrieves it via the API to grab the case_id, reuploads the patient to the API to mark it as closed (this has to be done as a separate step), then uploads the batch of contacts to CommCare, setting the stub patient's case_id as the parent_id field for each of these contacts. Finally, for each batch, the stub patient gets retrieved again along with its children, which in this case are contacts. This data is then used to generate a URL where each newly created contact can be viewed in CommCare.
9. After processing all of the contacts, the script outputs a final report Excel file, which contains all of the originally uploaded data, plus two additional columns: `contact_creation_success` and `commcare_contact_case_url`. The former is a boolean indicating if the row was uploaded, and the latter is a URL where the newly created contact can be viewed in CommCare.
10. After this is all done, the technical stakeholder should share the final report with the non-technical stakeholder and let them know that there contacts have been uploaded.

#### Creating the data dict

When running this script, the user-supplied contact data will be validated against a data dictionary, which the technical stakeholder will need to produce. The rows in this CSV are comprised of the complete set of non-deprecated CommCare contact fields that the non-technical stakeholder might upload.

The following columns must appear in the data dictionary CSV:

- `field`: The name of a CommCare field. This must appear for any given row.
- `data_type`: The datatype of the CommCare field. This is pulled from the data dictionary. The acceptable values are `plain`, `number`, `date`, `phone_number`, `select`, and `multi_select`. Note that in the original CommCare data dictionary that gets downloaded from the dashboard, the `multi_select` data type does not appear as a distinct data type. This value is to be used for rows marked as `select` in the original CommCare data dictionary where the description indicates that more than 1 value can be selected. We break this out as a separate data type for the purposes of this script because different validation rules are required.
- `allowed_values`: For rows with the `select` or `multi_select` data type, this field needs to be supplied. The value should be a comma-space (`, `) separated list of allowed values. For instance, for a select field with the options "yes", "no", and "unknown", this would be rendered as `yes, no, unknown` in the `allowed_values` field for the row.
- `required`: This field is optional. If one of the following values are provided, the field will be treated as required: `True`, `TRUE`, `true`, `1`, `Yes`, `Y`, `YES`, `y`, `yes`. When a field is marked as required, the script will raise an error if the required column is missing in the legacy contact data.

Here are the steps to create this asset:

1. Export the data dictionary from the CommCare instance as an Excel file.
2. Open this asset in a spreadsheet program.
3. Make sure you are in the contact tab of the spreadsheet, as it will also contain any other case types for the CommCare instance.
4. Rename the "Case Property" column header to "field".
5. Delete the column with the header "Group".
6. Get rid of rows that have deprecated case property values. To do this, filter the "Deprecated" column to True values, and then delete those rows. Remove the filter, and delete the "Deprecated" column.
7. For each remaining row, confirm that it has a data type defined. If it does not, reach out to a relevant person at DiMagi to resolve this issue.
8. Create a new column with the header "allowed_values".
9. Rename the column header that currently reads "Data Type" to "data_type".
10. Put a filter on the "data_type" column, and filter down to only rows with the "select" type. For each of these rows, you will need to look at the description field and pull out the allowed values. Reformat these as a comma-space (`, `) separated list int he adjacent "allowed_values" column for that row. For instance, if the allowed values are "yes", "no", and "maybe", you would put `yes, no, maybe` in the "allowed_values" column.  **Additionally, check if the values are indicated to be "multi select" in the description field. In that case, change the data type for this row to `multi_select`, as this will trigger different validation logic in the script.
11. Turn off the filter on the "data_type" column.
12. Delete the "Description" column.
13. Add a column with the header `required`.
14. If you want any properties to be required, mark them as `True` in the `required` column.
15. Export as CSV and save on same computer that will be running the script.

#### Running the script

Assuming you have sourced the appropriate environment variables and that you have the data dictionary CSV and legacy contact data CSV as described above, the following command will run the script:

```linux
bulk-upload-legacy-contact-data --username $COMMCARE_USER --apikey $COMMCARE_API_KEY --project $COMMCARE_PROJECT_NAME --caseDataPath <path-to-contact-data-to-be-uploaded> --dataDictPath <path-to-data-dict> --reportingPath <path-where-reporting-assets-will-be-created> --contactKeyValDict '{"additionalID": "additionalValue"}'
```

Note that the `--contactKeyValDict` is an optional argument. Any key-value pairs provided for this option will be added to all contacts created by the script.

### `sync-redcap-to-commcare`

This script is intended to sync case and contact data from a REDCap project to a corresponding CommCare contact tracing application. It assumes that:

* the project includes `redcap_repeat_instrument` called `"close_contacts"`
* checkbox fields are separated by a triple underscore (`___`) and the part after the triple underscore should be placed in a space-separated string property in CommCare (see `collapse_checkbox_columns()`)
* the column names in REDCap should otherwise be copied as-is to CommCare

To use the script, see its `--help` output:

```
sync-redcap-to-commcare --help
usage: sync-redcap-to-commcare [-h] --username COMMCARE_USER_NAME --apikey
                               COMMCARE_API_KEY --project
                               COMMCARE_PROJECT_NAME --redcap-api-url
                               REDCAP_API_URL --redcap-api-key REDCAP_API_KEY
                               --external-id-col EXTERNAL_ID_COL --state-file
                               STATE_FILE [--sync-all]

optional arguments:
  -h, --help            show this help message and exit
  --username COMMCARE_USER_NAME
                        The Commcare username (email address)
  --apikey COMMCARE_API_KEY
                        A Commcare API key
  --project COMMCARE_PROJECT_NAME
                        The Commcare project name
  --redcap-api-url REDCAP_API_URL
                        The REDCap API URL
  --redcap-api-key REDCAP_API_KEY
                        A REDCap API key
  --external-id-col EXTERNAL_ID_COL
                        Name of column in REDCap that should be used as the
                        external_id in CommCare
  --state-file STATE_FILE
                        The path where state should be read and saved
  --sync-all            If set, ignore the begin date in the state file and
                        sync all records
```

Sample command:

```
sync-redcap-to-commcare --username=$COMMCARE_USERNAME --apikey=$COMMCARE_API_KEY --project=$COMMCARE_PROJECT --redcap-api-url=$REDCAP_API_URL --redcap-api-key=$REDCAP_API_KEY --external-id-col=our_mrs_id --state-file=redcap_test.yaml --sync-all
```

## Logging

By default, this package logs to a .gitignored log file at `logs/cc-utilities.log`. This file is limited to 5MB and beyond that size, the log will be rotated. To log to a non-default location, you can set an env var for `COMMCARE_UTILITIES_LOG_PATH` for a directory in which to save logs.
