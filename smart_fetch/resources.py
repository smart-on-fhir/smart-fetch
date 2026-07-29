ALLERGY_INTOLERANCE = "AllergyIntolerance"
BINARY = "Binary"
BUNDLE = "Bundle"
CARE_PLAN = "CarePlan"
CONDITION = "Condition"
DEVICE = "Device"
DIAGNOSTIC_REPORT = "DiagnosticReport"
DOCUMENT_REFERENCE = "DocumentReference"
ENCOUNTER = "Encounter"
EPISODE_OF_CARE = "EpisodeOfCare"
FAMILY_MEMBER_HISTORY = "FamilyMemberHistory"
IMMUNIZATION = "Immunization"
LOCATION = "Location"
MEDICATION = "Medication"
MEDICATION_ADMINISTRATION = "MedicationAdministration"
MEDICATION_DISPENSE = "MedicationDispense"
MEDICATION_REQUEST = "MedicationRequest"
OBSERVATION = "Observation"
OPERATION_OUTCOME = "OperationOutcome"
ORGANIZATION = "Organization"
PATIENT = "Patient"
PRACTITIONER = "Practitioner"
PRACTITIONER_ROLE = "PractitionerRole"
PROCEDURE = "Procedure"
QUESTIONNARE = "Questionnare"
RESEARCH_SUBJECT = "ResearchSubject"
SERVICE_REQUEST = "ServiceRequest"
SPECIMEN = "Specimen"

# All resources that are linked to patients, in the order we usually like to process them.
# Patient first, Encounter, then the rest. (Encounters so early is due to a Cumulus quirk,
# where it likes to have Encounters exported before/at-same-time as other patient resources.)
PATIENT_TYPES = [
    PATIENT,
    ENCOUNTER,
    ALLERGY_INTOLERANCE,
    CARE_PLAN,
    CONDITION,
    DEVICE,
    DIAGNOSTIC_REPORT,
    DOCUMENT_REFERENCE,
    EPISODE_OF_CARE,
    FAMILY_MEMBER_HISTORY,
    IMMUNIZATION,
    MEDICATION_ADMINISTRATION,
    MEDICATION_DISPENSE,
    MEDICATION_REQUEST,
    OBSERVATION,
    PROCEDURE,
    QUESTIONNARE,
    RESEARCH_SUBJECT,
    SERVICE_REQUEST,
    SPECIMEN,
]

SCOPE_TYPES = {
    *PATIENT_TYPES,
    BINARY,
    LOCATION,
    MEDICATION,
    ORGANIZATION,
    PRACTITIONER,
    PRACTITIONER_ROLE,
}

# These are the fields to search for to ask "when was this record created?"
# (i.e. the administrative date, not the clinical date for "when did this described event happen?")
# Would be nice if there were a `meta.created` field.
#
# Some fields do have clinical dates that we could try to use as a fallback proxy,
# when the resource doesn't let you search on an administrative date, but... in practice that's
# no good. Take Immunization.occurrenceDateTime (searched with date=). Sometimes older vaccinations
# get imported into an EHR from external sources. You'd miss those with date=. Or sometimes it
# can take a day or two for even your own institutional data to be reflected in the system. So
# we might miss some events near our search date.
#
# Thankfully, the resources that tend to have no admin date also tend to be somewhat small.
# So we'll just get them all each time, without using a search field.
#
# If you update this, update the get_created_date call below too.
CREATED_SEARCH_FIELDS = {
    ALLERGY_INTOLERANCE: "date",
    # CARE_PLAN: The field should be "created", but in practice, this doesn't exist
    CONDITION: "recorded-date",
    # DEVICE has no admin date to search on
    DIAGNOSTIC_REPORT: "issued",
    DOCUMENT_REFERENCE: "date",
    # ENCOUNTER: has no admin date to search on (but does have clinical date of "date")
    # EPISODE_OF_CARE has no admin date (but does have clinical date of "period")
    FAMILY_MEMBER_HISTORY: "date",
    # IMMUNIZATION has `recorded` but you can't search it (but does have clinical date of "date")
    # MEDICATION_ADMINISTRATION: no field in the spec
    # MEDICATION_DISPENSE has no admin date to search on (but does have two clinical dates)
    MEDICATION_REQUEST: "authoredon",
    OBSERVATION: "issued",  # not searchable per spec, but some servers allow it (notably, Epic)
    # PATIENT has no admin date to search on (which is sort of good - merges.py relies on it)
    # PROCEDURE: has no admin date to search on (but does have clinical date of "date")
    QUESTIONNARE: "date",
    # RESEARCH_SUBJECT: no field in spec
    SERVICE_REQUEST: "authored",
    # SPECIMEN has no admin date (but does have clinical date of "collected")
}


# Get the FHIR version of the search params above.
# If you update this, update the CREATED_SEARCH_FIELDS dictionary above too.
def get_created_date(resource: dict) -> str | None:
    res_type = resource["resourceType"]

    if res_type == ALLERGY_INTOLERANCE:
        return resource.get("recordedDate")
    elif res_type == CONDITION:
        return resource.get("recordedDate")
    elif res_type == DIAGNOSTIC_REPORT:
        return resource.get("issued")
    elif res_type == DOCUMENT_REFERENCE:
        return resource.get("date")
    elif res_type == FAMILY_MEMBER_HISTORY:
        return resource.get("date")
    elif res_type == IMMUNIZATION:
        return resource.get("recorded")  # not searchable yet, but grab it for the future
    elif res_type == MEDICATION_REQUEST:
        return resource.get("authoredOn")
    elif res_type == OBSERVATION:
        return resource.get("issued")
    elif res_type == QUESTIONNARE:
        return resource.get("date")
    elif res_type == SERVICE_REQUEST:
        return resource.get("authoredOn")

    return None


def get_updated_date(resource: dict) -> str | None:
    return resource.get("meta", {}).get("lastUpdated")
