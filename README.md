# Full Backup Of Airtable Data

Only backs up the record data, table schemas and attachments.

Does not back up interfaces and automations.

## Requirements

To run this you need:

* Python 3
* `python-dotenv` module
* `pyairtable` module
* `requests` module

Installed by running `python -m pip install python-dotenv requests pyairtable`

## Set up

1. Go to [https://airtable.com/create/tokens](https://airtable.com/create/tokens)
2. Create an API token with the scopes:
    * `data.records:read`
    * `schema.bases:read`
3. Create a `.env` file in the base directory
4. Add the line `AIRTABLE_ACCESS_TOKEN=<paste your token in replace of this>`

## Running the backup

Run `python airtable_backup.py` in the root directory.

Once it is done, you can zip and upload your backup to wherever you would like to keep it.
