import os
import paramiko

SFTP_HOST = ''
SFTP_USERNAME = ''
SFTP_PASSWORD = ''
REMOTE_DIRECTORY = ''
LOCAL_DIRECTORY = ''
FILE_EXTENSION = '.csv'
BATCH_SIZE = 50

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
        print(f"Error accessing directory {directory}: {e}")
        return []

def fetch_and_delete_files(sftp, remote_directory, local_directory):
    """Fetch files from the remote directory in batches and delete them after fetching."""
    try:
        files = list_csv_files(sftp, remote_directory)
        print(f"Found {len(files)} .csv files in {remote_directory}")

        for i in range(0, len(files), BATCH_SIZE):
            batch = files[i:i + BATCH_SIZE]
            print(f"Processing batch {i // BATCH_SIZE + 1}: {len(batch)} files")

            for filename in batch:
                remote_path = os.path.join(remote_directory, filename)
                local_path = os.path.join(local_directory, filename)

                try:
                    sftp.get(remote_path, local_path)
                    print(f"Fetched: {filename} -> {local_path}")

                    # Delete file after successful fetch
                    sftp.remove(remote_path)
                    print(f"Deleted: {remote_path}")
                except Exception as e:
                    print(f"Error processing file {filename}: {e}")

    except Exception as e:
        print(f"Error during batch processing: {e}")

def main():
    """Main function to connect to SFTP and process files."""
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh_client.connect(hostname=SFTP_HOST, username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = ssh_client.open_sftp()

        fetch_and_delete_files(sftp, REMOTE_DIRECTORY, LOCAL_DIRECTORY)

        sftp.close()
    except paramiko.AuthenticationException:
        print("Authentication failed. Please check your credentials.")
    except paramiko.SSHException as e:
        print(f"SSH connection error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        ssh_client.close()

if __name__ == "__main__":
    main()
