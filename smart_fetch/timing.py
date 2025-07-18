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

    # Manually ignore leap seconds by clamping the seconds value to 59.
    #
    # Python native times don't support them (at the time of this writing, but also watch
    # https://github.com/python/cpython/issues/67762). For example, the stdlib's
    # datetime.fromtimestamp() also clamps to 59 if the system gives it leap seconds.
    #
    # But FHIR allows leap seconds and says receiving code SHOULD accept them,
    # so we should be graceful enough to at least not throw a ValueError,
    # even though we can't natively represent the most-correct time.
    #
    # :60 shouldn't appear anywhere in a DateTime but seconds.
    value = value.replace(":60", ":59")

    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None

    # Because we are mostly interested in comparing timestamps, we need all dates to be aware (not
    # naive). But to be conservative (and generally allow for transaction times that result in
    # duplicate resources next time we use --since=auto, rather than *missed* resources), we use
    # the most conservative/earliest timezone possible. Really, we kind of hate a lack of timezone.
    # See https://en.wikipedia.org/wiki/UTC+14:00
    #
    # As a (real-world) example of the problem, we might receive an Observation with just a
    # effectivePeriod of {"end": "2025-07-17"} for an export on the 17th. What to do?
    # - If we used partial dates ourselves and later searched for "gt2025-07-17", the server will
    #   pick a timezone for us, and we might miss some resources depending on when we did the
    #   original export on the 17th.
    # - If we pick a timezone ourselves like UTC, we might miss some resources, depending on when
    #   the server local time is.
    # - So we pick the earliest possible timezone for the 17th, and we are unlikely to miss any.
    #
    # (note: servers are allowed a lot of flexibility when comparing timezone dates with non
    # timezone dates in a search context - they can pick utc or local time or whatever. The spec
    # seems to be intentionally vague)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=14)))

    return parsed
