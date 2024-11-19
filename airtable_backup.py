import pyairtable
import os
import json
from datetime import datetime
import dotenv
import requests
from mimetypes import guess_extension

dotenv.load_dotenv()

backup_parent_dir = os.path.join(os.getcwd(), "backups")
if not os.path.exists(backup_parent_dir):
    os.makedirs(backup_parent_dir)

backup_dir = os.path.join(os.getcwd(), "backups", str(
    datetime.now().strftime("%Y%m%d-%H%M%S")))

os.makedirs(backup_dir)
os.makedirs(os.path.join(backup_dir, "records"))

api = pyairtable.Api(os.environ.get("AIRTABLE_ACCESS_TOKEN"))

bases = api._request("get", "https://api.airtable.com/v0/meta/bases")['bases']

attachments = []

for base in bases:
    print(f"{base['name']}")

    # Create folder for base
    base_folder_name = os.path.join(
        backup_dir, "records", f"{base['id']} ({base['name']})")
    if not os.path.exists(base_folder_name):
        os.makedirs(base_folder_name)

    # Get base schema
    tables = api._request(
        'get', f"https://api.airtable.com/v0/meta/bases/{ base['id'] }/tables")['tables']

    with open(os.path.join(base_folder_name, 'metadata.json'), 'w+') as base_metadata_file:
        # Create metadata file for base
        base_metadata_file.write(json.dumps(tables))

    # Create folder for each table
    for table in tables:
        print(f"{base['name']} > {table['name']}")
        # Get list of attachment fields
        attachment_fields = [f for f in table['fields']
                             if f['type'] == 'multipleAttachments']

        table_folder = os.path.join(
            base_folder_name, f"{table['id']} ({table['name']})")
        if not os.path.exists(table_folder):
            os.makedirs(table_folder)

        # Get all records in each table
        table_records = api.all(base['id'], table['id'])

        # Write file with all records as json
        os.path.join(table_folder, "data.json")
        with open(os.path.join(table_folder, "data.json"), 'w+') as table_records_file:
            table_records_file.write(json.dumps(table_records))

        # * Write metadata file with views and schema
        with open(os.path.join(table_folder, "metadata.json"), 'w+') as table_meta_file:
            table_meta_file.write(json.dumps(table))

        # Extract all attachment objects to list
        # if there are attachment fields, map the attachment fields to new lists, combine lists, remove empties, add to attachments list
        if len(attachment_fields) > 0:
            for row in table_records:
                row_attachments = []
                for f in attachment_fields:
                    if f['name'] in row['fields']:
                        row_attachments.extend(row['fields'][f['name']])
                attachments.extend(row_attachments)

attachments_folder = os.path.join(backup_dir, "attachments")
os.mkdir(attachments_folder)

with open(os.path.join(attachments_folder, "attachments.json"), 'w') as attachments_list_file:
    attachments_list_file.write(json.dumps(attachments))

for attachment in attachments:
    attachment_folder = os.path.join(attachments_folder, attachment['id'])
    if os.path.exists(attachment_folder):
        continue
    os.mkdir(attachment_folder)

    with open(os.path.join(attachment_folder, "metadata.json"), 'w+') as attachment_metadata_file:
        attachment_metadata_file.write(json.dumps(attachment))

    r = requests.get(attachment['url'], stream=True)
    if r.ok:
        attachment_file_ext = guess_extension(attachment['type'])
        attachment_file_name = attachment['filename'] or f"{attachment['id']}.{attachment_file_ext}"
        attachment_path = os.path.join(attachment_folder, attachment_file_name)
        print("saving to", os.path.abspath(attachment_path))
        with open(attachment_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))
