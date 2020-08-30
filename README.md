# commcare-utilities

This repo is for an assortment of scripts for developers working with Commcare.

## Setup

1. Create and source a virtual environment.
2. `pip3 install -e .`
3. Check if there are additional dependencies required for any of the scripts you wish to run (see below), and install them.
4. Enjoy.

## Scripts

### `sync_commcare_to_db`

This script allows a user to export data from Commcare to a SQL database. It does by iterating over all records returned in an OData feed for a particular case type to build a list of all observed property names (whether current or deprecated) to be turned into column names in a SQL db. It then calls [`commcare-export`](https://github.com/dimagi/commcare-export) as a subprocess, attempting to all cases with all observed property names to the db. Note that this script attempts to sync both current and deprecated fields to the target DB.

This script will create assets in a .gitignored folder at `assets/<project-name>/`. There are two types of assets that end up in this folder: xlsx and CSV. For each case type (e.g., contact, patient, etc.) that is synced, an Excel workbook is created [as required by `commcare-export`](https://confluence.dimagi.com/display/commcarepublic/CommCare+Data+Export+Tool#CommCareDataExportTool-HowtoGenerateanExcelQueryFile), containing source to target column mappings. This same information is stored in a JSON, to make results auditable without requiring Excel.

For instance, if you were to sync contacts for a project called "my-project", an Excel workbook would be created at `assets/my-project/contact-mappings.xlsx` and and a JSON file would be created at `assets/my-project/contact-column-state.json`. The JSON object contains the property `as_of`, indicating when the file was created or updated. These assets are only updated when new source targets appear.

Note the following oddity: When `commcare-export` encounters properties that do not have >=1 non-empty value in the source data, it will not add a column for that property type to the database. If on subsequent runs at least one case has been added with a non-null for the property, the property will be added as a column. This behavior was observed in a Postgres db; other flavors of SQL were not tested. This means that the source-to-target mappings that are indicated in the JSON and Excel files in `assets` are not a record of what was actually synced to the db, only what was attempted.

**Running the script:**

1. Create an OData feed in Commcare for any case types you want to export, and grab the URL for the feed.
2. Gather your Commcare API key and user name (email address)
3. Install the appropriate db engine library for your database. If you're not sure what that is, run the script without doing this, and you'll get a `ModuleNotFoundError` with the name of the required library.
4. Optionally, copy over `sample.env` to `.env` and insert appropriate values. Source those values before the next step.
5. Run the script. For instance, to export contact case type records: `sync_commcare_to_db --feed-url $CONTACT_FEED_URL --db $DB_URL --username $COMMCARE_USER --apikey $COMMCARE_API_KEY --project $COMMCARE_PROJECT --case-type contact`.
6. Any new columns added to the DB will be noted in the command-line output of the script.

### `lookup_contact_phone_numbers`

This script allows a user to run unprocessed contact phone numbers through the [Twilio Lookup API](https://www.twilio.com/docs/lookup/api) in order to determine if contacts can be reached by SMS. To do this, it queries a database for unprocessed contacts, queries the Twilio Lookup API for each number, then uses the [CommCare bulk upload API](https://confluence.dimagi.com/display/commcarepublic/Bulk+Upload+Case+Data) to update the `contact_phone_can_receive_sms` property on these cases.

Note that this script does not update the database it originally queries.

Also note that when this script encounters numbers that either a.) cannot be parsed to generate the standard format required by Twilio, or b.) are not found to be a valid number by the Twilio API, the script continues and logs a warning to a log file. It's on the user to relay information about such cases back to personel who can update the number in CommCare.

Finally, note that this script presently is configured to work with US-based phone numbers and any non-US numbers it encounters will not yield information about SMS capability.

**Running the script:**

1. Create a Twilio account if you don't already have one.
2. Gather your Twilio SID and auth token.
3. Install the appropriate db engine library for your database. If you're not sure what that is, run the script without doing this, and you'll get a `ModuleNotFoundError` with the name of the required library.
4. Optionally, copy over `sample.env` to `.env` and insert appropriate values. Source those values before the next step.
5. Run the script. Assuming the referenced variables are set: `lookup_contact_phone_numbers --db $DB_URL --username $COMMCARE_USER --apikey $COMMCARE_API_KEY --project $COMMCARE_PROJECT --twilioSID $TWILIO_SID --twilioToken $TWILIO_TOKEN`.
6. Any new columns added to the DB will be noted in the command-line output of the script.

## Logging

By default, this package logs to a .gitignored log file at `logs/cc-utilities.log`. This file is limited to 5MB and beyond that size, the log will be rotated. To log to a non-default location, you can set an env var for `COMMCARE_UTILITIES_LOG_PATH` for a directory in which to save logs.
