import pyairtable
import os
import json
from datetime import datetime
import dotenv
import requests
from mimetypes import guess_extension

dotenv.load_dotenv()

# Check if they forgot their lunchbox or not
if not "AIRTABLE_ACCESS_TOKEN" in os.environ.keys():
    raise Exception("No Airtable PAT provided. Go to https://airtable.com/create/tokens to make one, and make sure to set it in your environment.")

api = pyairtable.Api(os.environ.get("AIRTABLE_ACCESS_TOKEN"))
print(f"Connecting to Airtable with user {api.whoami()}")

# Generate folder structure for the backup output.
TODAY_FORMATTED = datetime.now().strftime("%Y%m%d-%H%M%S")

BACKUP_PARENT_DIR = os.path.join(os.getcwd(), "backups")
BACKUP_DIR = os.path.join(BACKUP_PARENT_DIR, TODAY_FORMATTED)
RECORDS_DIR = os.path.join(BACKUP_DIR, "records")

os.makedirs(BACKUP_DIR, exist_ok=True)  # this creates ./backups/ and ./backups/[BACKUP_DIR]/
os.makedirs(RECORDS_DIR, exist_ok=True)

bases = api.bases()

def go():
    for base in bases:
        # Create folder for base
        base_folder_dir = os.path.join(RECORDS_DIR, f"{base.id} ({base.name})")
        os.makedirs(base_folder_dir, exist_ok=True)
        
        # Get base tables
        tables = base.tables()

        # Create metadata file for base
        with open(os.path.join(base_folder_dir, 'metadata.json'), 'w+') as base_metadata_file:    
            base_metadata_file.write(json.dumps(str(tables)))

        

        for table in tables:
            table_attachments = []
            table_attachment_fields = [f for f in table.schema().fields if f.type == 'multipleAttachments']
            print(f"{base.name} > {table.name}")

            # creates folder structure for the table
            table_parent_dir = os.path.join(base_folder_dir, f"{table.id} ({table.name})")
            table_attachments_dir = os.path.join(table_parent_dir, 'attachments')
            os.makedirs(table_parent_dir, exist_ok=True)
            os.makedirs(table_attachments_dir, exist_ok=True)

            # Get all records in each table
            table_records = table.all()

            # Write file with all records as json
            with open(os.path.join(table_parent_dir, "data.json"), 'w+') as table_records_file:
                table_records_file.write(json.dumps(str(table_records)))

            # * Write metadata file with views and schema
            with open(os.path.join(table_parent_dir, "metadata.json"), 'w+') as table_meta_file:
                table_meta_file.write(json.dumps(str(table.schema())))

            
            # Extract all attachment objects to list
            # if there are attachment fields, map the attachment fields to new lists, combine lists, remove empties, add to attachments list
            for field in table_attachment_fields:
                for record in table_records:
                    table_attachments.extend(record[field])

            with open(os.path.join(table_attachments_dir, "attachments.json"), 'w') as attachments_list_file:
                attachments_list_file.write(json.dumps(table_attachments))

            for attachment in table_attachments:
                attachment_dir = os.path.join(attachments_folder, attachment.id)

                with open(os.path.join(attachment_dir, "metadata.json"), 'w+') as attachment_metadata_file:
                    attachment_metadata_file.write(json.dumps(str(attachment)))

                r = requests.get(attachment.url, stream=True)
                if r.ok:
                    attachment_file_ext = guess_extension(attachment.type)
                    attachment_file_name = attachment.filename or f"{attachment.id}.{attachment_file_ext}"
                    attachment_path = os.path.join(attachment_dir, attachment_file_name)
                    print("saving to", os.path.abspath(attachment_path))
                
                    with open(attachment_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024 * 8):
                            if chunk:
                                f.write(chunk)
                                f.flush()
                                os.fsync(f.fileno())
                else:  # HTTP status code 4XX/5XX
                    print("Download failed: status code {}\n{}".format(r.status_code, r.text))

if __name__ == '__main__':
    go()

# ⠀⠀⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀  Okay - data export underway!
# ⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀/
# ⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⢸⡿⠖⠒⠲⢦⣄⠀⠀⠀
# ⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⢸⡇⠀⠀⠀⠀⠹⣧⠀⠀
# ⠀⠀⠙⢿⣿⠿⠿⣿⣿⠿⠿⣿⡿⠿⢿⣿⡿⠿⠇⠀⢸⣷⣶⣶⣶⣶⣶⠿⠀⠀
# ⠀⠀⠇⠈⠋⠠⠆⠘⠃⠰⠀⠙⠁⠴⠀⠛⠀⠶⠀⠀⢸⣿⣿⠿⠿⠿⣿⠀⣶⠀
# ⠀⠂⣴⠖⠲⢶⡀⠂⣰⠶⠒⢶⡄⠂⢠⣤⣤⣤⣤⣤⠀⠋⣰⡶⠒⢶⡄⠀⠛⠀
# ⠀⠀⢿⡀⢀⣼⠇⠀⢿⣄⠀⣸⠇⠀⠘⠛⠛⠛⠛⠛⠀⠀⢿⣄⠀⣨⡇⠀⠀⠀
# ⠀⠀⠈⠙⠛⠉⠀⠀⠈⠙⠛⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠛⠋⠀⠀⠀⠀