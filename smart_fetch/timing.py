import datetime


def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).astimezone()


def parse_datetime(value: str | None) -> datetime.datetime | None:
    """
    Convert a FHIR dateTime type to a Python datetime object.

    This is mostly used for comparing dates and timestamps, so sometimes sacrifices a little
    accuracy to get a useful Python comparison object.

    See https://www.hl7.org/fhir/R4/datatypes.html#dateTime for format details.
    """
    if not value:
        return None

    # datetime.fromisoformat() only handles dates at least to the day level.
    # But FHIR allows YYYY and YYYY-MM formats too. So we need to manually handle those.
    # We fill in the missing bits with "01", which is not entirely capturing the intent of the
    # short format (which covers a range), but to work with a datetime we need a date and it's
    # safer to go earlier.
    if len(value) == 4:
        value += "-01-01"
    elif len(value) == 7:
        value += "-01"

    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None

    # Because we are mostly interested in comparing timestamps, we need all dates to be aware (not
    # naive) - so set an arbitrary timezone of UTC. Local timezone wouldn't make any more or less
    # sense because who knows what timezone the EHR is in, but it would make testing more annoying.
    # So UTC it is.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.UTC)

    return parsed
