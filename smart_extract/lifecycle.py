import contextlib
import datetime
import json
import os

import smart_extract
from smart_extract import timing


def _atomic_write(path: str, contents: dict) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf8") as f:
        json.dump(contents, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def _basic_metadata() -> dict:
    return {
        "timestamp": timing.now().isoformat(),
        "version": smart_extract.__version__,
    }


def should_skip(folder: str, tag: str) -> bool:
    if tag.startswith("fix:"):
        return should_skip_fix(folder, tag.split(":", 1)[1])
    done_file = f"{folder}/.{tag}.done"
    return os.path.exists(done_file)


@contextlib.contextmanager
def mark_done(folder: str, tag: str):
    """Marks a task as done"""
    started = timing.now()

    if tag.startswith("fix:"):
        with mark_fix_done(folder, tag.split(":", 1)[1]):
            yield started
        return

    # Do the action!
    yield started

    delta = started - timing.now()

    done_file = f"{folder}/.{tag}.done"
    metadata = _basic_metadata()
    metadata["duration"] = delta.total_seconds()
    _atomic_write(done_file, metadata)


def _load_done(filename: str) -> dict:
    try:
        with open(filename, encoding="utf8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def should_skip_fix(folder: str, fix: str) -> bool:
    done_file = f"{folder}/.fix.done"
    done = _load_done(done_file)
    return fix in done.get("fixes", [])


@contextlib.contextmanager
def mark_fix_done(folder: str, fix: str):
    """Marks a fix task as done"""
    yield

    done_file = f"{folder}/.fix.done"
    done = _load_done(done_file)
    done.update(_basic_metadata())
    done.setdefault("fixes", []).append(fix)

    _atomic_write(done_file, done)
