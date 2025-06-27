# SMART Fetch

A tool for extracting FHIR data from your EHR.

## Quick Start

If you want to skip any description and dive right in:

```shell
pipx install smart-fetch

smart-fetch export \
  --fhir-url https://bulk-data.smarthealthit.org/fhir \
  /tmp/export
```

After running the above, you'll have a bunch of NDJSON resource files sitting in `/tmp/export`.

Run `smart-fetch --help` to learn more about the various options.

## Initial Export

SMART Fetch can do an initial pull from your EHR,
with either a "bulk export" or by manually searching through resources 
based on a list of patient MRNs,
when bulk exporting is either not available or too slow.

## Post-Processing Your Export

Very often, the initial export from the EHR will be missing some useful data.
There will be referenced Medications, Observations, and clinical notes that you need to manually
download to have a complete set of data.

SMART Fetch can help fill in those gaps by post-processing (or hydrating) your FHIR data.

Mostly these hydration tasks focus on grabbing data now so that you'll always have it,
no matter what happens with your EHR later.

### Example Hydration Tasks

- Inlining clinical notes from DiagnosticReport or DocumentReference
  (it's more reliable to get the note once upfront, and then you have it forever)
- Adding missing Observations (several EHRs don't provide
  DiagnosticReport.result or Observation.hasMember linked
  Observations by default in a search/export)
- Downloading MedicationRequest.medicationReference linked
  Medications (which can't normally be bulk exported but
  are necessary for clinically interpreting a MedicationRequest)

## Managing Exports Over Time

Fetching data for a group is rarely as simple as a single export.
You may want to export some resources separately, for performance reasons
(e.g. Observations can take a long time).
Or you may want to grab updates to the group's data over time.

SMART Fetch can help with that, by keeping each export in its own subfolder
for individual processing,
but also pooling all the results together with symlinks for convenience.

### Example

```shell
pipx install smart-fetch

# Initial export
smart-fetch export \
  --fhir-url https://bulk-data.smarthealthit.org/fhir \
  --type Encounter,Patient \
  /tmp/export

ls -l /tmp/export
# 001.2025-06-26/
# Encounter.001.ndjson.gz -> 001.2025-06-26/Encounter.001.ndjson.gz
# Patient.001.ndjson.gz -> 001.2025-06-26/Patient.001.ndjson.gz

# Second export with a --since date, a new resource, and a nickname for the export
smart-fetch export \
  --fhir-url https://bulk-data.smarthealthit.org/fhir \
  --type Encounter,Patient,Condition \
  --since 2020-01-01 \
  --nickname second-run \
  /tmp/export

ls -l /tmp/export
# 001.2025-06-26/
# 002.second-run/
# Condition.001.ndjson.gz -> 002.second-run/Condition.001.ndjson.gz
# Encounter.001.ndjson.gz -> 001.2025-06-26/Encounter.001.ndjson.gz
# Encounter.002.ndjson.gz -> 002.second-run/Encounter.001.ndjson.gz
# Patient.001.ndjson.gz -> 001.2025-06-26/Patient.001.ndjson.gz
# Patient.002.ndjson.gz -> 002.second-run/Patient.001.ndjson.gz
```
