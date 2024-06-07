import os
import subprocess
from datetime import datetime, timedelta

def generate_dates(start_date, end_date):
    """Generate a list of dates between start_date and end_date."""
    delta = end_date - start_date
    return [(start_date + timedelta(days=i)).strftime('%Y%m%d') for i in range(delta.days + 1)]

def run_azcopy(source_url, dest_url,sas_token):
    """Run AzCopy to copy data from source_url to dest_url."""
    command = f"azcopy copy '{source_url}' '{dest_url}?{sas_token}' --recursive"
    subprocess.run(command, shell=True, check=True)
    
def generate_sas_token(account_name, container_name, account_key):
    """Generate a SAS token for the specified Azure storage container."""
    command = [
        "az", "storage", "container", "generate-sas",
        "--account-name", account_name,
        "--name", container_name,
        "--permissions", "rwdl",
        "--expiry", "2024-10-01",
        "--output", "tsv",
        "--account-key", account_key
    ]
    sas_token = subprocess.check_output(command).decode('utf-8').strip()
    return sas_token


def main():
    # Set the date range
    start_date = datetime.strptime('yyyy-mm-dd', '%Y-%m-%d')
    end_date = datetime.strptime('yyyy-mm-dd', '%Y-%m-%d')
    _container_name = ""
    _account_key = ""
    _account_name =""
    _s3_address= ""
    # Generate dates
    dates = generate_dates(start_date, end_date)
    
    # Set source and destination URLs
    s3_base_url = _s3_address
    azure_base_url = f'https://zarrstorage.blob.core.windows.net/{_container_name}/'
    sas_token =  generate_sas_token(account_name=_account_name, 
                                    container_name=_container_name, 
                                    account_key=_account_key)

    # Process each date
    for date in dates:
        source_url = f"{s3_base_url}{date}/*"
        dest_url = f"{azure_base_url}{date}"
        print(f"Copying data for {date}...")
        try:
            run_azcopy(source_url, dest_url, sas_token)
            print(f"Successfully copied data for {date}.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to copy data for {date}. Error: {e}")

if __name__ == "__main__":
    main()

