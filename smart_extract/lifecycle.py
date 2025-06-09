import contextlib
import datetime
import json
import os

import smart_extract


@contextlib.contextmanager
def skip_or_mark_done(folder: str, tag: str, desc: str):
    """Marks a task as done (or skips it if already done)"""
    done_file = f"{folder}/.{tag}.done"
    if os.path.exists(done_file):
        print(f"Skipping {desc}, already done.")
        return

    # Do the action!
    yield

    with open(done_file, "w", encoding="utf8") as f:
        json.dump(
            {
                "timestamp": datetime.datetime.now(datetime.UTC).astimezone().isoformat(),
                "version": smart_extract.__version__,
            },
            f,
        )
