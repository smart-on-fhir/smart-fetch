---
title: Working with Cumulus
parent: SMART Fetch
nav_order: 30
# audience: non-programmers vaguely familiar with the project
# type: how-to
---

# Working with Cumulus

SMART Fetch does not depend on the [Cumulus](https://docs.smarthealthit.org/cumulus/) project,
but it *is* written by the same folks and has some conveniences for that workflow.

Cumulus ETL will look for certain export metadata files in its input folder.
These files are written by SMART Fetch for each export.

As a result, **you should always pass Cumulus ETL the actual export folder**,
rather than the toplevel SMART Fetch folder.

By default, SMART Fetch will create a toplevel folder that holds all the actual export folders.
It might looks like:
```
001.2025-06-26/
    Condition.001.ndjson.gz
    log.ndjson
002.2025-06-27/
    Observation.001.ndjson.gz
    log.ndjson
Condition.001.ndjson.gz -> 001.2025-06-26/Condition.001.ndjson.gz
Observation.001.ndjson.gz -> 002.2025-06-27/Observation.001.ndjson.gz
```

But you can see there that only the subfolders have interesting metadata files like `log.ndjson`.

## What Files Does Cumulus ETL Look For?

- `log.ndjson` holds some metadata about the export (like timestamp and group name) that Cumulus
  ETL uses for its own group metadata tracking purposes. If Cumulus ETL does not find this file,
  it will require you to provide it on the command line.
- SMART Fetch also might write out a `deleted/` folder with resources that the server has deleted,
  which Cumulus ETL reads so that it can delete any matching resources in its output databases.
  * A bulk export will provide us with this info by default, but even in crawl mode, SMART Fetch
  will calculate a list of deleted resources, by comparing any new full exports with previous
  exports to detect IDs that have been deleted.

## Cumulus ETL Arguments

So when feeding Cumulus ETL data from SMART Fetch,
it's best to pass in the export subfolder that SMART Fetch makes, rather than the toplevel folder.

As an example:
```shell
cumulus-etl ./exports/001.2025-06-26/ s3://.../output/ s3://.../phi/
```