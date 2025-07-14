---
title: SMART Fetch
has_children: true
# audience: non-programmers new to the project
# type: explanation
---

# SMART Fetch

**SMART Fetch extracts FHIR data from your electronic health records (EHR).**

_In theory_, exporting FHIR data from your EHR is easy.
Just do a [bulk export](https://hl7.org/fhir/uv/bulkdata/export.html)!

But in practice, performance issues, vendor quirks, and important data not even being
bulk-exportable in the first place (e.g. clinical notes and medication info)
can make it more complicated.

SMART Fetch makes it easy again.

- **Is bulk export too slow?** SMART Fetch can fake one by searching over each patient in your
  cohort manually. This can be faster than a bulk export for some vendors.
- **Is it too difficult to get a cohort defined on your server?** SMART Fetch doesn't need to wait
  for a FHIR Group to be defined. Just give it a list of MRNs to define a local cohort,
  and it will fetch your data.
- **Does your vendor not support `_since`?** SMART Fetch can fake it by using per-resource date
  fields, to grab all resources created since a certain date.
- **Do you want offline access to clinical notes?** SMART Fetch will download the notes and inject
  them as inline FHIR data, so you have them forever.

## Quick Start
```shell
pipx install smart-fetch

smart-fetch export \
  --fhir-url https://bulk-data.smarthealthit.org/fhir \
  /tmp/export
```

After running the above, you'll have a bunch of NDJSON resource files sitting in `/tmp/export`.

Read the rest of this guide to understand all the other things you can do with SMART Fetch.

## Source Code
SMART Fetch is open source.
If you'd like to browse its code or contribute changes yourself,
the code is on [GitHub](https://github.com/smart-on-fhir/smart-fetch).
