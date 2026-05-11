---
title: Case Study: BCH
parent: SMART Fetch
nav_order: 40
# audience: non-programmers already a little familiar with the project
# type: explanation
---

# How BCH Uses SMART Fetch in Production

Boston Children's Hospital maintains SMART Fetch and also uses it internally
to export the data used for the [Cumulus](https://docs.smarthealthit.org/cumulus/) project.

But because real world "in the trenches" usage is always a little different than the ideal case,
this guide will explain we manage the process of exporting data for over 1.4 million patients.

## Context

- This is a snapshot of our workflow. Practices may change over time.
- BCH uses Epic as an EHR. Which means bulk exporting all those patients is not realistic.
  Data is entirely exported via crawling.
- We run against a non-production Epic server, which shadows the prod data.
- We aim for quarterly updates of patient data.

## Cohorts

Since we are crawling, we don't *need* a FHIR Group defined.
And going back and forth with IT to define a registry/group to get it just right
(and updated over time) is friction that we don't need to deal with,
because SMART Fetch can just operate off of MRNs.

1. We start by getting a list of all MRNs from an internal database of patients synced with Epic.
1. We break that list up into groups of ~10k patients (mostly so that we can track progress more
   easily like "we've now exported 5 out of 100 groups")

So now we have a pile of files (144 at the time of writing) with MRNs in them, line by line.
These will define the groups we operate on.

BCH names these group files like `MRN-000` or `MRN-123`.

## Initial Export

We use a little shell script to iterate over the 144 groups, and export each one.
We export 40 groups at the same time using our internal Slurm queue system,
to save time but not so much time that IT complains about us hammering the server.

Each export looks something like:
```shell
smart-fetch export \
  --verbose \
  --nickname initial \
  --id-system urn:oid:1.2.840.114350.xxx \
  --id-file /path/to/MRN-123 \
  /output/root/MRN-123
```

We give it a nickname of `initial` so that all the groups' original export folders
will be easier to identify.

We'll end up with a folder tree like:
```
/output/root/
  MRN-000/
    001.initial/
      log.ndjson
      error/OperationOutcome.ndjson.gz
      Condition.ndjson.gz
      ... (and other resources)
    Condition.001.ndjson.gz -> 001.initial/Condition.ndjson.gz
    ... (and symlinks for the other resources)
  MRN-001/ (similar)
  MRN-002/ (similar)
  ... (and so on)
```

### Archiving

We sync the output folder to S3 to archive it, and make it easier to grant folks fine-grained
access to the archive.

Symlinks don't work on S3, so we just archive the leaf node folders (`001.initial`).

### Running Cumulus ETL

Then we use Cumulus ETL on all the `001.inital` folders.

One by one, running a command like the following for each group:
```shell
docker compose run --rm \
  cumulus-etl \
  --batch-size 800000 \
  s3://archive-bucket/epic/MRN-123/001.initial/ \
  s3://etl-output-bucket/epic/ \
  s3://etl-phi-bucket/epic/
```

### Building Cumulus Libary Studies

Once the ETL is done, we build the basic Cumulus Library studies too.

```shell
cumulus-library build \
  --database cumulus_epic_db \
  --workgroup cumulus \
  --target discovery

cumulus-library build \
  --database cumulus_epic_db \
  --workgroup cumulus \
  --target core
```

## Quarterly Refreshes

### New MRN Groups

We get the list of MRNs again from the database.
New MRNs go into new groups, and we keep existing MRNs in their existing groups.

### Full Exports

Because Epic doesn't provide the `meta.lastUpdated` field,
we choose to do a full server export every quarter rather than try to do an incremental update
with `--since` because that would require the use of `--since-mode=created` (the best-effort
fallback mode when `lastUpdated` is not available).

- We've seen some older data (from an extra few months to a year old) be injected into the server
  between refreshes, seemingly from 3rd party sources.
  If using `--since-mode=created`, we'd miss those older data pieces (because they are injected
  with the created timestamps from the 3rd parties).
- We've seen lots of churn in how Epic maps its internal data structures to FHIR.
  From changing the coding systems to changing which kind of resources get created from an event.
  Because of this, it's nice to make sure we get the current FHIR translation for all our data.

### Exporting

The export process looks the same, except that instead of `--nickname initial`,
we use `--nickname 2026-03` or whatever the month happens to be that we start the export in.
This becomes our internal identifier for the quarterly refresh.

```shell
smart-fetch export \
  --verbose \
  --nickname 2026-03 \
  --id-system urn:oid:1.2.840.114350.xxx \
  --id-file /path/to/MRN-123 \
  /output/root/MRN-123
```

This leaves us with a file layout like:
```
/output/root/
  MRN-000/
    001.initial/*
    002.2026-03/
      log.ndjson
      deleted/* (this is new)
      error/*
      ... (resource files)
  MRN-001/ (similar)
  ... (and so on)
  MRN-145/
    001.2026-03/*
```

Note that the latest MRN group has a subfolder with a slightly different name (001 instead of 002).
This is because this quarter's refresh is the first export for that group.
This is fine, just be careful in any of your own scripting that you're looking for something like
`*.2026-03` instead of hardcoding `002.2026-03`.

### Deleted Resources

You'll also see some `deleted/` folders in the groups that have a previous export.
Normally, if we were bulk-exporting, that would hold a list of all the resources that the server
told us to delete (because the server deleted them and we should get in sync).

SMART Fetch emulates that bulk-export behavior when crawling, by looking at the previous export
and noticing which resources are no longer there.

This is useful when we run the ETL, because Cumulus ETL will look at that folder and delete the
resources from Athena as appropriate.
(Epic can have a lot of ID churn. For example, as it merges and unmerges patients.)

### Processing Refresh Data

Just like with the initial export, we archive the new data, ETL it, and rebuild Library studies.

### Timeline

Because we are doing a full refresh of all 1.4 million patients, it can take a while.

Maybe a week or two for the export.
And then another three to four weeks for the ETL itself.

Because the ETL is the slowest part, we do a couple things to speed it up:
- We give it plenty of memory (32GB) and use large batch sizes.
  Small batch sizes seem to take just as long as large batches when writing to a Delta Lake,
  so you can get a lot more mileage by increasing that batch size.
- We pass `--no-table-optimization` to Cumulus ETL for most groups, only leaving it off when the
  group name ends in zero. This skips an optimization step where Delta Lake files are re-organized
  and unused files are deleted. These speed up queries against the database and are useful, but
  since we're writing a lot of groups in sequence, it isn't necessary to optimize after every
  single group is processed.
