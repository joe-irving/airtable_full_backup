import pyairtable
import os
import json
from datetime import datetime
import dotenv
import requests

dotenv.load_dotenv()

backup_dir = f"./backups/{datetime.now()}"

os.makedirs(backup_dir)
os.makedirs(f"{backup_dir}/records")

api = pyairtable.Api(os.environ.get("AIRTABLE_ACCESS_TOKEN"))

bases = api._request("get", "https://api.airtable.com/v0/meta/bases")['bases']

attachments = []

for base in bases:
    print(f"{base['name']}")

    # Create folder for base
    base_folder_name = f"{backup_dir}/records/{base['id']} ({base['name']})"
    if not os.path.exists(base_folder_name):
        os.makedirs(base_folder_name)

    # Get base schema
    tables = api._request('get', f"https://api.airtable.com/v0/meta/bases/{ base['id'] }/tables")['tables']
    
    with open(f'{base_folder_name}/metadata.json', 'w+') as base_metadata_file:
        # Create metadata file for base
        base_metadata_file.write(json.dumps(tables))
    
    # Create folder for each table
    for table in tables:
        print(f"{base['name']} > {table['name']}")
        # Get list of attachment fields
        attachment_fields = [f for f in table['fields'] if f['type'] == 'multipleAttachments']

        table_folder = f"{base_folder_name}/{table['id']} ({table['name']})"
        if not os.path.exists(table_folder):
            os.makedirs(table_folder)

        # Get all records in each table
        table_records = api.all(base['id'], table['id'])

        # Write file with all records as json
        with open(f"{table_folder}/data.json", 'w+') as table_records_file:
            table_records_file.write(json.dumps(table_records))
       
        # * Write metadata file with views and schema
        with open(f"{table_folder}/metadata.json", 'w+') as table_meta_file:
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

attachments_folder = f"{backup_dir}/attachments"
os.mkdir(attachments_folder)

with open(f"{attachments_folder}/attachments.json", 'w') as attachments_list_file:
    attachments_list_file.write(json.dumps(attachments))

for attachment in attachments:
    attachment_folder = f"{attachments_folder}/{attachment['id']}"
    if os.path.exists(attachment_folder):
        continue
    os.mkdir(attachment_folder)

    with open(f"{attachment_folder}/metadata.json", 'w+') as attachment_metadata_file:
        attachment_metadata_file.write(json.dumps(attachment))
    
    r = requests.get(attachment['url'], stream=True)
    if r.ok:
        print("saving to", os.path.abspath(f"{attachment_folder}/{attachment['filename']}"))
        with open(f"{attachment_folder}/{attachment['filename']}", 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))

