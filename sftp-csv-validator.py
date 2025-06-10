import os
import json
import yaml
import sqlite3
import smtplib
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tkinter import Tk, Label, Entry, Button, StringVar, filedialog, messagebox
from tkinter.ttk import Progressbar

# --- CONFIGURATION LOADING ---

def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# Example YAML configuration file (config.yaml)
#
# sftp:
#   hostname: "sftp.example.com"
#   username: "your_username"
#   password: "your_password"
#   root_directories:
#     - "/data/incoming"
# local:
#   directory: "./downloads"
#   log_file: "./validation.log"
# filter:
#   keyword: "target"
#   file_extension: ".csv"
# email:
#   sender: "your_email@example.com"
#   receiver: "receiver@example.com"
#   smtp_server: "smtp.example.com"
#   smtp_port: 465
#   password: "email_password"

CONFIG_PATH = "config.yaml"
config = load_config(CONFIG_PATH)

# SFTP settings
HOSTNAME = config["sftp"]["hostname"]
USERNAME = config["sftp"]["username"]
PASSWORD = config["sftp"]["password"]
ROOT_DIRECTORIES = config["sftp"]["root_directories"]

# Local settings
LOCAL_DIRECTORY = config["local"]["directory"]
LOG_FILE = config["local"]["log_file"]

# File filter settings
KEYWORD = config["filter"]["keyword"]
FILE_EXTENSION = config["filter"]["file_extension"]

# Email settings
EMAIL_SETTINGS = config.get("email", {})

# Ensure local directory exists
os.makedirs(LOCAL_DIRECTORY, exist_ok=True)

# --- DATABASE INTEGRATION ---

DB_FILE = "validation_results.db"

def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            state TEXT,
            errors TEXT,
            warnings TEXT,
            info TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def save_to_database(filename, state, errors, warnings, info):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO results (filename, state, errors, warnings, info)
        VALUES (?, ?, ?, ?, ?)
    """, (filename, state, json.dumps(errors), json.dumps(warnings), json.dumps(info)))
    conn.commit()
    conn.close()

initialize_database()

# --- CSV VALIDATION FUNCTION ---

def validate_csv(csv_content, schema_dict=None, dialect=None):
    """
    Placeholder CSV validation logic.
    Returns (errors, warnings, info) as lists.
    In a real implementation, you might use libraries like `csv`, `pandas`, or custom logic.
    """
    errors = []
    warnings = []
    info = []

    # For example: simply check if CSV content is empty.
    if not csv_content.strip():
        errors.append("CSV content is empty")
    else:
        info.append("CSV read successfully")
    return errors, warnings, info

# --- SFTP UTILS ---
# Using paramiko for SFTP operations
import paramiko

def create_sftp_connection():
    transport = paramiko.Transport((HOSTNAME, 22))
    transport.connect(username=USERNAME, password=PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport

def list_files_recursive(sftp, remote_dir):
    """
    Recursively list files in remote_dir that match the file filter criteria.
    Returns a list of dict objects with 'path' and 'filename'.
    """
    file_list = []

    def _recurse(path):
        try:
            for entry in sftp.listdir_attr(path):
                remote_path = os.path.join(path, entry.filename)
                if paramiko.SFTPAttributes.S_ISDIR(entry.st_mode):
                    _recurse(remote_path)
                else:
                    # Apply file filters
                    if remote_path.endswith(FILE_EXTENSION) and KEYWORD in remote_path:
                        file_list.append({"path": remote_path, "filename": os.path.basename(remote_path)})
        except Exception as e:
            print(f"Error listing {path}: {e}")
    _recurse(remote_dir)
    return file_list

# --- ERROR HANDLING & RETRY ---
def safe_sftp_operation(func, max_retries=3):
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"Attempt {attempt+1} failed with error: {e}")
            if attempt == max_retries - 1:
                raise
    return None

# --- FILE PROCESSING FUNCTION ---
def process_file(sftp, file_info, schema_dict, dialect, log_lock):
    filename = file_info["filename"]
    remote_path = file_info["path"]
    local_path = os.path.join(LOCAL_DIRECTORY, filename)
    try:
        # Retry SFTP file download
        safe_sftp_operation(lambda: sftp.get(remote_path, local_path))
        with open(local_path, "r", encoding="utf-8") as f:
            csv_content = f.read()
        errors, warnings, info = validate_csv(csv_content, schema_dict, dialect)
        validation_state = "invalid" if errors else "valid"
        log_line = f"{filename},{validation_state},{json.dumps(errors)},{json.dumps(warnings)},{json.dumps(info)}\n"
        # Write log with thread-safety
        with log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as log:
                log.write(log_line)
        # Save result to database
        save_to_database(filename, validation_state, errors, warnings, info)
    except Exception as e:
        error_str = str(e)
        log_line = f"{filename},error,{error_str},,\n"
        with log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as log:
                log.write(log_line)
        print(f"Error processing file {filename}: {traceback.format_exc()}")

# --- MAIN VALIDATION PROCESS ---
def fetch_and_validate_sftp_files(schema_dict=None, dialect=None):
    # Initialize log file with header
    with open(LOG_FILE, "w", encoding="utf-8") as log:
        log.write("File,Validation State,Errors,Warnings,Info\n")
    # Create SFTP connection
    sftp, transport = create_sftp_connection()
    log_lock = __import__("threading").Lock()  # Ensure thread-safe log writes
    tasks = []
    try:
        with ThreadPoolExecutor() as executor:
            # Iterate through each root directory
            for root_dir in ROOT_DIRECTORIES:
                files = list_files_recursive(sftp, root_dir)
                for file_info in files:
                    tasks.append(executor.submit(process_file, sftp, file_info, schema_dict, dialect, log_lock))
            # Wait for all tasks to complete
            for task in tasks:
                task.result()
    finally:
        sftp.close()
        transport.close()

# --- EMAIL NOTIFICATIONS ---
def send_email_notification(subject, body, log_file):
    sender_email = EMAIL_SETTINGS.get("sender")
    receiver_email = EMAIL_SETTINGS.get("receiver")
    password = EMAIL_SETTINGS.get("password")
    smtp_server = EMAIL_SETTINGS.get("smtp_server")
    smtp_port = EMAIL_SETTINGS.get("smtp_port", 465)
    if not all([sender_email, receiver_email, password, smtp_server]):
        print("Email settings are incomplete. Skipping email notification.")
        return

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            attachment = MIMEText(f.read())
            attachment.add_header("Content-Disposition", "attachment", filename="validation_results.log")
            msg.attach(attachment)

        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print("Email notification sent.")
    except Exception as e:
        print(f"Failed to send email notification: {e}")

# --- SUMMARY REPORT GENERATION ---
def generate_summary_report(log_file):
    total_files = 0
    valid_files = 0
    invalid_files = 0
    with open(log_file, "r", encoding="utf-8") as log:
        next(log)  # Skip header
        for line in log:
            total_files += 1
            if "valid" in line:
                valid_files += 1
            elif "invalid" in line:
                invalid_files += 1
    summary = (f"Validation Summary:\n"
               f"Total Files: {total_files}\n"
               f"Valid Files: {valid_files}\n"
               f"Invalid Files: {invalid_files}")
    print(summary)
    return summary

# --- GUI INTEGRATION WITH TKINTER ---
class ValidationGUI:
    def __init__(self, master):
        self.master = master
        master.title("SFTP CSV Validation")

        # SFTP Settings (for demonstration, you can add more fields as needed)
        Label(master, text="SFTP Config File:").grid(row=0, column=0, sticky="e")
        self.config_var = StringVar(value=CONFIG_PATH)
        self.config_entry = Entry(master, textvariable=self.config_var, width=40)
        self.config_entry.grid(row=0, column=1)
        Button(master, text="Browse", command=self.browse_config).grid(row=0, column=2)

        # Schema file (optional) and delimiter fields can be added here
        Label(master, text="Schema File (optional):").grid(row=1, column=0, sticky="e")
        self.schema_var = StringVar()
        self.schema_entry = Entry(master, textvariable=self.schema_var, width=40)
        self.schema_entry.grid(row=1, column=1)
        Button(master, text="Browse", command=self.browse_schema).grid(row=1, column=2)

        Label(master, text="CSV Delimiter (optional):").grid(row=2, column=0, sticky="e")
        self.delimiter_var = StringVar(value=",")
        self.delimiter_entry = Entry(master, textvariable=self.delimiter_var, width=40)
        self.delimiter_entry.grid(row=2, column=1)

        # Start button
        Button(master, text="Start Validation", command=self.start_validation).grid(row=3, column=1, pady=10)

        # Progress bar
        self.progress = Progressbar(master, orient="horizontal", length=300, mode="determinate")
        self.progress.grid(row=4, column=0, columnspan=3, pady=10)

    def browse_config(self):
        path = filedialog.askopenfilename(filetypes=[("YAML Files", "*.yaml *.yml")])
        if path:
            self.config_var.set(path)

    def browse_schema(self):
        path = filedialog.askopenfilename(filetypes=[("JSON Files", "*.json"), ("YAML Files", "*.yaml *.yml")])
        if path:
            self.schema_var.set(path)

    def start_validation(self):
        # Optionally load a schema here if provided
        schema_dict = None
        if self.schema_var.get():
            try:
                if self.schema_var.get().endswith((".yaml", ".yml")):
                    with open(self.schema_var.get(), "r") as f:
                        schema_dict = yaml.safe_load(f)
                else:
                    with open(self.schema_var.get(), "r") as f:
                        schema_dict = json.load(f)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load schema: {e}")
                return

        # You can also retrieve and use the delimiter if needed
        delimiter = self.delimiter_var.get()

        # Disable start button to prevent re-entry and update progress bar (simulated)
        self.progress.start(10)
        self.master.update()

        try:
            fetch_and_validate_sftp_files(schema_dict, delimiter)
            summary = generate_summary_report(LOG_FILE)
            send_email_notification("CSV Validation Complete", summary, LOG_FILE)
            messagebox.showinfo("Done", "CSV Validation process complete.\n" + summary)
        except Exception as e:
            messagebox.showerror("Error", f"Validation failed: {e}")
        finally:
            self.progress.stop()

# --- MAIN ENTRY POINT ---
def main():
    # If running from command line without GUI, uncomment below:
    # fetch_and_validate_sftp_files()
    # summary = generate_summary_report(LOG_FILE)
    # send_email_notification("CSV Validation Complete", summary, LOG_FILE)

    # For GUI integration:
    root = Tk()
    gui = ValidationGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
