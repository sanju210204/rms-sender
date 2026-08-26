import requests
from datetime import datetime, timedelta, timezone
import random
import json
import time as t
import os
import glob
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# TIMEZONE
# ============================================================
# The government portal expects Date/Time in IST (India Standard
# Time), regardless of what timezone the machine running this
# script is actually in. Locally (India) this makes no visible
# difference, but on GitHub Actions -- whose servers run in UTC --
# datetime.now() would otherwise be off by 5 hours 30 minutes,
# which is enough to get the reading rejected as "not today's data".
IST = timezone(timedelta(hours=5, minutes=30))


# ============================================================
# API ENDPOINT
# ============================================================
URL = "https://gis.jharkhand.gov.in/ejalportal/RMS_DATA.asmx/Get_RMS_DATA_NEW"


# ============================================================
# RETRY + THREAD CONFIG
# ============================================================
MAX_RETRIES = 15
REQUEST_TIMEOUT = 120
DELAY_BETWEEN_ROUNDS = 10
MAX_THREADS = 20


# ============================================================
# LOAD DEVICES
# ============================================================
with open("devices.json", "r") as f:
    DEVICES = json.load(f)

os.makedirs("logs", exist_ok=True)


# ============================================================
# DYNAMIC DATA GENERATOR
# ============================================================
def generate_dynamic_data():
    now = datetime.now(IST)
    return {
        "Date": now.strftime("%Y-%m-%d"),
        "Time": now.strftime("%H:%M:%S"),
        "SingleStrength": random.randint(80, 100),
        "MotorSpeed": random.randint(2400, 2600),
        "DV": random.randint(60, 70),
        "DC": random.randint(3, 6),
        "MotorVoltage": random.randint(60, 70),
        "MotorCurrent": random.randint(45, 60),
        "CurrentFlow": random.randint(400, 450),
        "InstPower": random.randint(200, 250),
        "TotalEnrgy": random.randint(6000, 7500),
        "TotalOnTime": random.randint(40, 80),
        "Status": "OFF",
        "FaultStatus": 1,
        "CurrentSensor": 1,
        "FaultCode": 0,
        "TotalFlow": random.randint(200, 300),
        "TotalOffTime": random.randint(5000, 6000),
        "Temp": random.randint(40, 50),
        "SpeedStatus": random.randint(500, 600),
        "OutputPower": random.randint(500, 600),
    }


# ============================================================
# SEND ONE DEVICE (thread worker)
# ============================================================
def send_single_device(device):
    payload = device.copy()
    payload.update(generate_dynamic_data())

    try:
        response = requests.get(URL, params=payload, timeout=REQUEST_TIMEOUT)
        text = response.text.strip()

        return {
            "device": device,
            "DeviceId": payload.get("DeviceId"),
            "IMIS_Scheme_ID": payload.get("IMIS_Scheme_ID"),
            "response": text,
            "success": "Success" in text
        }

    except Exception as e:
        return {
            "device": device,
            "DeviceId": payload.get("DeviceId"),
            "IMIS_Scheme_ID": payload.get("IMIS_Scheme_ID"),
            "response": f"ERROR: {e}",
            "success": False
        }


# ============================================================
# PARALLEL SEND + RETRY ENGINE
# ============================================================
def send_data_with_retries():
    retry_queue = DEVICES[:]
    attempt = 1

    success_log = []
    fail_log = []

    while retry_queue and attempt <= MAX_RETRIES:
        print(f"\nRETRY ROUND {attempt} | Pending Devices: {len(retry_queue)}")

        next_retry = []

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = {executor.submit(send_single_device, dev): dev for dev in retry_queue}

            for future in as_completed(futures):
                result = future.result()

                log_entry = {
                    "IMIS_Scheme_ID": result["IMIS_Scheme_ID"],
                    "DeviceId": result["DeviceId"],
                    "Response": result["response"],
                    "Timestamp": datetime.now(IST).isoformat()
                }

                if result["success"]:
                    print(f"SUCCESS -> {result['DeviceId']}")
                    success_log.append(log_entry)
                else:
                    print(f"FAILED -> {result['DeviceId']}")
                    fail_log.append(log_entry)
                    next_retry.append(result["device"])

        retry_queue = next_retry
        attempt += 1

        if retry_queue:
            print(f"Waiting {DELAY_BETWEEN_ROUNDS}s for next retry...")
            t.sleep(DELAY_BETWEEN_ROUNDS)

    return success_log, fail_log


# ============================================================
# SAVE LOGS
# ============================================================
def save_logs(success, fail):
    timestamp = datetime.now(IST).strftime("%Y-%m-%d_%H-%M-%S")
    success_file = f"logs/success_{timestamp}.json"
    fail_file = f"logs/fail_{timestamp}.json"

    with open(success_file, "w") as f:
        json.dump(success, f, indent=4)

    with open(fail_file, "w") as f:
        json.dump(fail, f, indent=4)

    print(f"Logs saved: {success_file}, {fail_file}")


# ============================================================
# LOG RETENTION (auto-delete logs older than 3 days)
# ============================================================
# NOTE: this uses the timestamp embedded in the filename
# (success_YYYY-MM-DD_HH-MM-SS.json), not the file's modified time.
# That's deliberate -- on GitHub Actions, files are freshly checked
# out from git on every run, so their actual mtime always looks
# "new" regardless of when the log was really created.
LOG_RETENTION_DAYS = 3

def cleanup_old_logs():
    # Compared against naive datetimes parsed from filenames, so this
    # is also kept naive (but still correctly in IST, not UTC).
    cutoff = datetime.now(IST).replace(tzinfo=None) - timedelta(days=LOG_RETENTION_DAYS)
    deleted = 0

    for pattern in ("logs/success_*.json", "logs/fail_*.json"):
        for filepath in glob.glob(pattern):
            filename = os.path.basename(filepath)
            # filename looks like: success_2026-08-24_16-05-33.json
            try:
                timestamp_str = filename.split("_", 1)[1].rsplit(".", 1)[0]
                file_time = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
            except (IndexError, ValueError):
                continue  # skip files that don't match the expected naming pattern

            if file_time < cutoff:
                try:
                    os.remove(filepath)
                    deleted += 1
                except OSError as e:
                    print(f"Could not delete {filepath}: {e}")

    if deleted:
        print(f"Deleted {deleted} log file(s) older than {LOG_RETENTION_DAYS} days.")


# ============================================================
# RUN ONCE AND EXIT (this is the part that changes for GitHub Actions —
# no infinite loop, no internal sleep/scheduling. GitHub Actions calls
# this script once per scheduled run instead.)
# ============================================================
if __name__ == "__main__":
    print("Starting one data-sending cycle...")
    success, fail = send_data_with_retries()
    save_logs(success, fail)
    cleanup_old_logs()

    print(f"\nDone. {len(success)} succeeded, {len(fail)} failed.")

    # Exit with a non-zero code if everything failed, so the GitHub Actions
    # run shows as failed/red and you get notified.
    if DEVICES and not success:
        sys.exit(1)
