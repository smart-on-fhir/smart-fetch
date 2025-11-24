---
title: Exporting
parent: SMART Fetch
nav_order: 1
# audience: non-programmers vaguely familiar with the project
# type: how-to
---

# Exporting Data in Bulk

By default, SMART Fetch will perform a [bulk export](https://hl7.org/fhir/uv/bulkdata/export.html)
for a wide variety of FHIR resource types.

```shell
smart-fetch export \
  --fhir-url https://bulk-data.smarthealthit.org/fhir \
  /tmp/export
```

## Defining the Cohort

Working with your EHR, you should first define a FHIR Group.
This process will be vendor-specific.

(But as an example for Epic, you have to first define a Registry,
then have your Epic contacts turn that into a FHIR Group.)

Once you have a Group identifier, simply pass it in with `--group ID`.

## Filtering

You can limit which resource types are exported by passing `--type Patient,Condition`.
Type arguments can be specified multiple times.

You can also filter further by adding
[type filters](https://hl7.org/fhir/uv/bulkdata/export.html#_typefilter-experimental-query-parameter)
like `--type-filter=Patient?active=true`.

Adding multiple `--type-filter` arguments will act as a logical OR and find resources that match
any of your type filters.

### Default Observation Filtering

Because Observation typically has the largest volume of resources,
SMART Fetch saves time and space by only exporting Observations with one of the
[nine standard categories](https://www.hl7.org/fhir/R4/valueset-observation-category.html).

You can disable this by passing the `--no-default-filters` flag.

## Crawling

"Crawling" is the process of faking a bulk export by going through every patient in your cohort,
and searching for all their resources one-by-one using traditional
[FHIR search](https://www.hl7.org/fhir/R4/search.html).

This is useful if you don't have permission to start a bulk export or your EHR vendor does not
support bulk exports (or is not very performant when running a bulk export).
But in general, if you don't need to use this approach, stick to actual bulk exports.
They should be faster.

It's easy to start a crawl instead of a bulk export by simply passing `--export-mode=crawl`.

{: .note }
Crawling is the default export mode for Epic servers,
because Epic's current bulk export implementation has limitations
(in terms of both recommended cohort size and performance).
You can override this with `--export-mode=bulk`.

### Custom Cohorts

By default, a crawl will use an initial bulk export to get the list of Patients
from a defined FHIR Group just like the normal bulk export flow.
After that, it will use those exported Patients to do non-bulk searches.

But if you don't even have a FHIR Group at all,
you can also specify a custom cohort with a file of identifier values (for example, MRNs).

Pass in:
- `--id-file` which can be either a text file with one ID per line
  or a CSV file with a column labelled `ID` or `MRN` (case-insensitive)
- `--id-system` which is a code system that your institution/vendor uses to mark
  the patient identifiers (if not set, direct FHIR IDs will be matched)

If you are only working with a small number of IDs,
you can instead pass in `--id-list` to specify them on the command line
(comma separated).

#### Example
Assume there is a Patient resource that looks like this:
```json
{
  "resourceType": "Patient",
  "id": "123",
  "identifier": [
    {
      "system": "uri:oid:1.2.3.4",
      "value": "abc"
    }
  ]
}
```

You can pass in `--id-list=123` to have SMART Fetch use a cohort of just this patient.
(Or instead, pass in both `--id-system=uri:oid:1.2.3.4` and `--id-list=abc`.)

#### Make a Cohort from an Existing Export

Alternatively, if you already have a pile of Patient FHIR data from a previous export,
you can also use it as a cohort. Pass `--source-dir=path/to/fhir/data`
and any Patient resources found there will be used as the cohort.

### New and Deleted Patients

When crawling, SMART Fetch will notice new and deleted patients, just like a bulk export would.

Meaning, historical resources for new and merged patients may be included in the crawl,
even outside the `--since` timespan.
New patients added to the Group or the MRN file will be noticed,
as well as patient records that absorb another record when two patient records merge.
In that case, we'll pull the historical resources for the surviving record,
to make sure we have all the updated resources.

And if a patient is deleted by the EHR (or removed from the Group or MRN file),
that fact will be recorded in a `deleted/` folder, just like it would be for a bulk export.
Downstream tooling might use that information.

This all relies on looking back at previous exports in the export folder.
Noticing new and deleted patients should automatically happen on the server side for a bulk export,
but SMART Fetch has to do them manually for a crawl.

## Resuming

If your bulk export (or crawl) gets interrupted, just run the SMART Fetch command again with the
same arguments, and it will be resumed.

## New Exports

Every new export you do with different `--type`, `--type-filter`, or `--since` parameters will
go into its own subdirectory with its own log.
Then a symbolic link (symlink) will be created in the target folder pointing to the newly
downloaded files.

For example, after a couple exports (exporting Conditions and Observations separately),
you might have a folder that looks like:
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

This lets you work with each individual export or the entire group of exports,
depending on your use case at the time.

If you want more predictably named subfolders, pass `--nickname my-nickname` and the folder will
be named something like `003.my-nickname` instead of using the current date.

## Compression

By default, SMART Fetch writes compressed NDJSON files, which makes the files ~90% smaller.
But if gzip files are awkward for your pipeline to process, you can turn off compression by
passing `--no-compression` to the `export` command (or any command that writes output files).
