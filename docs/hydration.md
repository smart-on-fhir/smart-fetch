---
title: Hydration
parent: SMART Fetch
nav_order: 10
# audience: non-programmers vaguely familiar with the project
# type: how-to
---

# Post-Processing Your Data

SMART Fetch strives to make as complete an export as possible for archival and research purposes.

There are situations where not all the data you might be clinically interested in gets exported,
and SMART Fetch will try to get it for you after the fact.

SMART Fetch calls this kind of post-processing "hydration"
and happens automatically for each export.

Below is a fuller explanation of each hydration task.

## Inlining Clinical Notes

Both DiagnosticReport and DocumentReference resources refer to external clinical notes.
The resources themselves are usually just a bit of metadata about the note and an external pointer.

That's not very useful for a lot of clinical use cases, since the note is the most valuable bit.
And since a note's URL may change (or you may lose access to the EHR), it's nice to have the note
downloaded and ready to go.

So SMART Fetch will download all the clinical notes it finds (that are HTML or plain text)
and stuff them back into your NDJSON as inline notes (using the `data` attachment field).

This way they can be processed with other FHIR tools that handle attachments.

## Downloading Missing Observations

Vendors have a habit of treating certain Observations specially and not including them in
bulk exports or searches.

Specifically, they may leave out Observations pointed to by `DiagnosticReport.result`
or `Observation.hasMember`.

But since both kinds of Observations are clinically useful,
SMART Fetch will download them all for you, if they weren't already included in the export.

## Downloading Medications

MedicationRequests might reference external Medication resources.
And those Medication resources are not patient-linked, so they cannot normally be bulk-exported.

But since they are so clinically relevant,
SMART Fetch will download all linked Medications for you.
