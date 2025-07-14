---
title: Incremental Updates
parent: SMART Fetch
nav_order: 3
# audience: non-programmers vaguely familiar with the project
# type: how-to
---

# Fetching Updates Since Your Original Export

A common need is to be able to get all the updates since the last time you exported.
That's easy!

Simply pass `--since=auto` (to automatically detect the timestamp of your last export)
or with a manual timestamp like `--since=2025-02-10T10:23:54Z`.

This will only export the resources that have changed since that time.

## How it Works

Under the covers, SMART Fetch is just passing the server `_since` (in bulk mode)
or `_lastUpdated` (in crawl mode) parameters.

If you provide a date, that is passed on directly.
If you provide `auto`, SMART Fetch will look in the metadata it stores alongside your previous
exports and pull out a date to use.

## Vendors that Don't Support Update Times

Some vendors don't support the `meta.lastUpdated` field.
In which case, they won't support `--since` by default.

But that's OK!
If you pass in `--since-mode=created`, SMART Fetch will use an alternative "since" check.

Instead of using a "when was this resource last updated" check, it will use a
"when was this resource created" check.

{: .note }
Created mode is the default "since" mode for Epic servers,
because Epic does not support `meta.lastUpdated`.

### Limitations

This mode has severe limitations that discourage its use unless you have no other choice.

#### Possibility of Missed Resources

Most FHIR resource types have a "created time" field for SMART Fetch to search over.

But sometimes it's more of a clinical date than an administrative date.
For example, the only searchable date field for Observations is the `effective` choice field.

This means that if a newly created resource is clinically back-dated to before your since date,
you will miss it.

For example, if an Observation is created today, but is marked with an `effective` date of three
days ago, and you passed in a `--since` date of yesterday, that resource will not be picked up
even though you would want it to be.

Resource types affected by this:
- Encounter
- Immunization
- Observation
- Procedure

#### Some Resources Have No Searchable Dates

Some resource types don't even have searchable creation _or_ clinical dates.
SMART Fetch must export all of them every time.
Thankfully, these resource types are not often huge.
But it's something to be aware of.

Resource types affected by this:
- Device
- Patient

#### Updates to Existing Resources Are Never Noticed

If an already-created resource has edits made to it, this method will not pick that up.

If this is critical, you may need to just do an entirely new export of all resources when you need
to check for updates.
