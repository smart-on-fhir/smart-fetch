import datetime


def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).astimezone()
