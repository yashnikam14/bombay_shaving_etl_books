import os
from datetime import datetime

def write_log(message):
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_name = f"{current_date}_logs.txt"

        root_dir = os.getcwd()

        logs_dir = os.path.join(root_dir, "logs")

        os.makedirs(logs_dir, exist_ok=True)

        file_path = os.path.join(logs_dir, file_name)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}\n"

        with open(file_path, "a", encoding="utf-8") as f:
            f.write(log_message)

    except Exception as e:
        print(f"Logging failed: {e}")