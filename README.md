# commcare-db-syncer

This script allows a user to export data from Commcare to a SQL database. It does by iterating over all records returned in an OData feed for a particular case type to build a list of all observed property names (whether current or deprecated) to be turned into column names in a SQL db. It then calls [`commcare-export`](https://github.com/dimagi/commcare-export) as a subprocess, attempting to all cases with all observed property names to the db.

This script presently contains files in `sync_commcare_assets` to support syncing the following case types:

- contact
- investigation
- patient
- lab_result

If you need to add a new case type, create a file with a name of the form `<case-type>-column-state.json` in the `sync_commcare_assets` folder. Copy the contents of `template.json` from that same folder into the new file and save.

## Oddities

- When `commcare-export` encounters properties that do not have >=1 non-empty value in the source data, it will not add a column for that property type to the database. If on subsequent runs at least one case has been added with a non-null for the property, the property will be added as a column. This behavior was observed in a Postgres db; other flavors of SQL were not tested.

## To run it...

1. Create an OData feed in Commcare for any case types you want to export, and grab the URL for the feed.
2. Gather your Commcare API key and user name (email address)
3. Create a virtual environment in this directory.
4. `pip install -r requirements.txt`
5. Install the appropriate db engine library for your database. If you're not sure what that is, run the script without doing this, and you'll get a `ModuleNotFoundError` with the name of the required library.
6. Optionally, copy over `sample.env` to `.env` and insert appropriate values. Source those values before the next step.
7. Run the script. For instance, to export contact case type records: `python sync_commcare_to_db.py --feed-url $CONTACT_FEED_URL --db $DB_URL --username $COMMCARE_USER --apikey $COMMCARE_API_KEY --project $COMMCARE_PROJECT --case-type contact`.
