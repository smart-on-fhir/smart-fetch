ALLERGY_INTOLERANCE = "AllergyIntolerance"
BINARY = "Binary"
CONDITION = "Condition"
DEVICE = "Device"
DIAGNOSTIC_REPORT = "DiagnosticReport"
DOCUMENT_REFERENCE = "DocumentReference"
ENCOUNTER = "Encounter"
IMMUNIZATION = "Immunization"
MEDICATION = "Medication"
MEDICATION_REQUEST = "MedicationRequest"
OBSERVATION = "Observation"
OPERATION_OUTCOME = "OperationOutcome"
PATIENT = "Patient"
PROCEDURE = "Procedure"
SERVICE_REQUEST = "ServiceRequest"

# All resources that are linked to patients, in the order we usually like to process them.
# Patient first, Encounter, then the rest.
PATIENT_TYPES = [
    PATIENT,
    ENCOUNTER,
    ALLERGY_INTOLERANCE,
    CONDITION,
    DEVICE,
    DIAGNOSTIC_REPORT,
    DOCUMENT_REFERENCE,
    IMMUNIZATION,
    MEDICATION_REQUEST,
    OBSERVATION,
    PROCEDURE,
    SERVICE_REQUEST,
]
