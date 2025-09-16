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
"when was this resource created" check, for resources that have a "created" timestamp field.

{: .note }
Created mode is the default for servers that don't
declare support for the `_lastUpdated` search field (Epic is a notable example).

### Limitations

This mode has severe limitations that discourage its use unless you have no other choice.
You may prefer to just do a full fresh export each time you want updates, without using `--since`.

#### Possibility of Missed Resources

Many FHIR resource types have a "created time" field for SMART Fetch to search over.

But resources can also be imported into your system after their own creation time
(due to either a delay from internal processes or just a periodic import from external sources).
FHIR does not have a field for "import time" (absent `meta.lastUpdated`).

This can cause issues.
For example, if a DiagnosticReport is created today, but is marked with an `issued` date of three
days ago, and you passed in a `--since` date of yesterday, that resource will not be picked up
even though you would want it to be.

#### Some Resources Have No Creation Dates

Some resource types don't even have searchable creation dates.
SMART Fetch must export all of them every time.
Thankfully, these resource types are not often huge.
But it's something to be aware of.

Resource types affected by this:
- Device
- Encounter
- Immunization
- Patient
- Procedure

#### Updates to Existing Resources Are Never Noticed

If an already-created resource has edits made to it, this method will not pick that up.

If this is critical, you may need to just do an entirely new export of all resources when you need
to check for updates.
