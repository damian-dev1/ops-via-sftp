import os
import paramiko
import concurrent.futures
import logging

# Constants
SFTP_HOST = ''
SFTP_USERNAME = ''
SFTP_PASSWORD = ''
REMOTE_DIRECTORY = ''  # Remote directory to fetch files from
LOCAL_DIRECTORY = ''
FILE_EXTENSION = '.csv'  # Process all .csv files
BATCH_SIZE = 50  # Process in batches of 50
MAX_WORKERS = 10  # Number of parallel threads

# Configure logging
logging.basicConfig(
    filename="sftp_fetcher.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Ensure local directory exists
os.makedirs(LOCAL_DIRECTORY, exist_ok=True)


def list_csv_files(sftp, directory):
    """List all .csv files in a directory."""
    try:
        return [
            entry.filename
            for entry in sftp.listdir_attr(directory)
            if entry.filename.endswith(FILE_EXTENSION) and not (entry.st_mode & 0o40000)
        ]
    except Exception as e:
        logging.error(f"Error accessing directory {directory}: {e}")
        return []


def fetch_and_delete_file(sftp, remote_directory, filename):
    """Fetch and delete a single file."""
    remote_path = os.path.join(remote_directory, filename)
    local_path = os.path.join(LOCAL_DIRECTORY, filename)
    try:
        # Fetch the file
        sftp.get(remote_path, local_path)
        logging.info(f"Fetched: {filename}")

        # Delete the file
        sftp.remove(remote_path)
        logging.info(f"Deleted: {filename}")
    except Exception as e:
        logging.error(f"Error processing file {filename}: {e}")


def fetch_and_delete_files_in_parallel(sftp, remote_directory, files):
    """Fetch and delete files in parallel using ThreadPoolExecutor."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(fetch_and_delete_file, sftp, remote_directory, filename)
            for filename in files
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in threaded operation: {e}")


def main():
    """Main function to connect to SFTP and process files."""
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect to the SFTP server
        ssh_client.connect(hostname=SFTP_HOST, username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = ssh_client.open_sftp()

        # List all .csv files
        files = list_csv_files(sftp, REMOTE_DIRECTORY)
        logging.info(f"Found {len(files)} .csv files in {REMOTE_DIRECTORY}")

        # Process files in batches
        for i in range(0, len(files), BATCH_SIZE):
            batch = files[i:i + BATCH_SIZE]
            logging.info(f"Processing batch {i // BATCH_SIZE + 1}: {len(batch)} files")
            fetch_and_delete_files_in_parallel(sftp, REMOTE_DIRECTORY, batch)

        sftp.close()
    except paramiko.AuthenticationException:
        logging.error("Authentication failed. Please check your credentials.")
    except paramiko.SSHException as e:
        logging.error(f"SSH connection error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        ssh_client.close()


if __name__ == "__main__":
    main()
