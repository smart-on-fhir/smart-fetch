import datetime
import json
import os

from smart_extract import timing


def create_fake_log(folder: str, fhir_url: str, group: str, transaction_time: datetime.datetime):
    timestamp = timing.now().isoformat()
    url = (
        os.path.join(fhir_url, "Group", group, "$export")
        if group
        else os.path.join(fhir_url, "$export")
    )
    with open(f"{folder}/log.ndjson", "w", encoding="utf8") as f:
        json.dump(
            {
                "exportId": "fake-log",
                "timestamp": timestamp,
                "eventId": "kickoff",
                "eventDetail": {
                    "exportUrl": url,
                },
            },
            f,
        )
        f.write("\n")
        json.dump(
            {
                "exportId": "fake-log",
                "timestamp": timestamp,
                "eventId": "status_complete",
                "eventDetail": {
                    "transactionTime": transaction_time.astimezone().isoformat(),
                },
            },
            f,
        )
        f.write("\n")
