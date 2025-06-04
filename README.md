# SMART Extract

A tool for extracting FHIR data from your EHR.

## Initial Extraction

SMART Extract can do an initial pull from your EHR,
in either "bulk export" format or by manually searching
through resources based on a list of patient MRNs if bulk
exporting is not available or too slow.

## Post-Extract Curing

Very often, the initial export from the EHR will need some
post-processing (or "curing"). SMART Extract can help with that.

Mostly these steps focus on grabbing data now so you'll always
have it, no matter what happens with the EHR later.

Steps like:
- Inlining clinical notes from DiagnosticReport or DocumentReference
- Adding missing Observations (several EHRs don't provide
  DiagnosticReport.result or Observation.hasMember linked
  Observations by default in a search/export)
- Downloading MedicationRequest.medicationReference linked
  Medications (which can't normally be bulk exported but
  are necessary for clinically interpreting a MedicationRequest)

## Managing Your Extract Process

If you're looking to just "get the data out and ready to be processed",
SMART Extract can manage that for you. Just give it a Group name or a
list of patient MRNs and it will go and download all the interesting
resources for that cohort, curing the data as needed.
