# SFTP CSV Validator

This application provides a robust solution for fetching CSV files from an SFTP server, performing validations on their content, logging the results, storing them in a local SQLite database, and sending email notifications for completed processes. It features a simple graphical user interface (GUI) for easy interaction.

-----

## Features

  * **SFTP Integration**: Connects to an SFTP server to list and download files.
  * **Recursive File Listing**: Scans specified root directories on the SFTP server, including subdirectories, for relevant files.
  * **File Filtering**: Filters files based on a **keyword** and **file extension** (e.g., `.csv`).
  * **CSV Validation**: Includes a placeholder function for CSV content validation, which can be extended with custom schema checks or data quality rules.
  * **Multi-threaded Processing**: Uses a thread pool to process multiple files concurrently, improving efficiency.
  * **Error Handling & Retries**: Implements retry logic for SFTP operations to enhance reliability.
  * **Detailed Logging**: Records validation outcomes (valid, invalid, error) along with any issues in a dedicated log file.
  * **SQLite Database Storage**: Persists validation results (filename, state, errors, warnings, info) in a local SQLite database for easy querying and reporting.
  * **Email Notifications**: Sends an email summary report with the log file attached upon completion of the validation process.
  * **Graphical User Interface (GUI)**: Provides a user-friendly Tkinter-based interface for configuration and execution.
  * **Configurable**: All key settings are managed through a `config.yaml` file, making it easy to update parameters without modifying the code.

-----

## Prerequisites

Before running the application, ensure you have the following installed:

  * **Python 3.x**: Download from [python.org](https://www.python.org/downloads/).
  * **`paramiko`**: A Python implementation of the SSHv2 protocol, used for SFTP connectivity.
  * **`PyYAML`**: A YAML parser and emitter for Python.

-----

## Installation

1.  **Clone the repository** (if applicable) or download the script files.
    ```bash
    git clone <repository_url>
    cd sftp-csv-validator
    ```
2.  **Install required Python packages**:
    ```bash
    pip install paramiko PyYAML
    ```

-----

## Configuration

The application's settings are managed via a `config.yaml` file. Create this file in the same directory as the Python script (`sftp_csv_validator.py`).

Here's an example `config.yaml` structure:

```yaml
# SFTP connection settings
sftp:
  hostname: "sftp.example.com"
  username: "your_sftp_username"
  password: "your_sftp_password"
  root_directories:
    - "/data/incoming"
    - "/another/sftp/path" # Add more root directories as needed

# Local file and logging settings
local:
  directory: "./downloads" # Local directory to download files to
  log_file: "./validation.log" # Path for the validation log file

# File filtering criteria
filter:
  keyword: "target" # Keyword to look for in file names/paths
  file_extension: ".csv" # File extension to filter by (e.g., .csv, .txt)

# Email notification settings (optional)
email:
  sender: "your_email@example.com" # Sender email address
  receiver: "receiver@example.com" # Recipient email address
  smtp_server: "smtp.example.com" # SMTP server for sending emails
  smtp_port: 465 # SMTP port (e.g., 587 for TLS, 465 for SSL)
  password: "your_email_password" # Password for the sender email account
```

**Make sure to replace the placeholder values with your actual SFTP credentials, paths, and email settings.**

-----

## Usage

### Running the Application with GUI

1.  **Ensure `config.yaml` is set up correctly.**
2.  **Run the Python script**:
    ```bash
    python sftp_csv_validator.py
    ```
3.  A Tkinter GUI window will appear.
4.  You can **browse for a different `config.yaml`** file if needed.
5.  Optionally, specify a **schema file** (JSON or YAML) for more advanced CSV validation and a **CSV Delimiter**.
6.  Click the **"Start Validation"** button to initiate the process.
7.  A progress bar will indicate activity, and a message box will confirm completion or report errors.

-----

## Database Integration

The application creates and uses an SQLite database named `validation_results.db` to store the results of each file validation.

The `results` table has the following schema:

  * `id`: `INTEGER PRIMARY KEY AUTOINCREMENT`
  * `filename`: `TEXT` - The name of the processed file.
  * `state`: `TEXT` - "valid", "invalid", or "error".
  * `errors`: `TEXT` - JSON string of validation errors.
  * `warnings`: `TEXT` - JSON string of validation warnings.
  * `info`: `TEXT` - JSON string of informational messages.
  * `timestamp`: `DATETIME DEFAULT CURRENT_TIMESTAMP` - When the record was created.

You can use any SQLite browser (e.g., DB Browser for SQLite) to view and query the results in this database.

-----

## Logging

All validation activities are logged to the file specified in `config.yaml` (default: `./validation.log`). The log file is a comma-separated value (CSV) file with a header: `File,Validation State,Errors,Warnings,Info`.

-----

## CSV Validation Logic

The `validate_csv` function in `sftp_csv_validator.py` is currently a placeholder. It simply checks if the CSV content is empty.

To implement comprehensive validation, you should modify this function to include:

  * **Schema validation**: Check if columns exist and have the correct data types.
  * **Data integrity checks**: Ensure values are within expected ranges, unique where required, etc.
  * **Custom business rules**.

You can use Python's built-in `csv` module, or more powerful libraries like `pandas`, `jsonschema` (for JSON schemas), or `cerberus` for validation.

-----

## Error Handling and Retries

The script includes a `safe_sftp_operation` function that retries SFTP file downloads up to 3 times in case of transient errors. This helps to make the application more robust against network issues or temporary server unavailability.

-----

## Email Notifications

Upon completion of the validation process, an email notification is sent to the `receiver` email address specified in `config.yaml`. The email includes a summary report and attaches the `validation.log` file. If email settings are incomplete, the email notification will be skipped.

-----

## Contributing

Feel free to fork the repository, make improvements, and submit pull requests.

-----
