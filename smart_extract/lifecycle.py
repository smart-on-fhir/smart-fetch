import contextlib
import datetime
import json
import os

import smart_extract


def _basic_metadata() -> dict:
    return {
        "timestamp": datetime.datetime.now(datetime.UTC).astimezone().isoformat(),
        "version": smart_extract.__version__,
    }


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
        json.dump(_basic_metadata, f, indent=2)


@contextlib.contextmanager
def skip_or_mark_done_for_fix(folder: str, fix: str):
    """Marks a task as done (or skips it if already done)"""
    done_file = f"{folder}/.fix.done"
    done = {}
    try:
        with open(done_file, encoding="utf8") as f:
            done = json.load(f)
            if fix not in done.get("fixes", []):
                raise FileNotFoundError
        print(f"Skipping fix {fix}, already done.")
        return
    except FileNotFoundError:
        pass  # expected path - we have not yet done this fix

    # Do the action!
    yield

    done.update(_basic_metadata())
    done.setdefault("fixes", []).append(fix)

    with open(done_file, "w", encoding="utf8") as f:
        json.dump(done, f, indent=2)
