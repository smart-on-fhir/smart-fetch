from smart_fetch import hydrate_utils, resources


class DevOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "dev-org"
    INPUT_RES_TYPE = resources.DEVICE
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("owner",)


class DxReportOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "dxr-org"
    INPUT_RES_TYPE = resources.DIAGNOSTIC_REPORT
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("performer*", "resultsInterpreter*")


class DocOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "doc-org"
    INPUT_RES_TYPE = resources.DOCUMENT_REFERENCE
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("author*", "authenticator", "custodian")


class EncOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "enc-org"
    INPUT_RES_TYPE = resources.ENCOUNTER
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("hospitalization.origin", "hospitalization.destination", "serviceProvider")


class ImmOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "imm-org"
    INPUT_RES_TYPE = resources.IMMUNIZATION
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("manufacturer", "performer*.actor", "protocolApplied*.authority")


class LocOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "loc-org"
    INPUT_RES_TYPE = resources.LOCATION
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("managingOrganization",)


class MedReqOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "medreq-org"
    INPUT_RES_TYPE = resources.MEDICATION_REQUEST
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("reportedReference", "requester", "performer", "dispenseRequest.performer")


class ObsOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "obs-org"
    INPUT_RES_TYPE = resources.OBSERVATION
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("performer*",)


class PatOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "pat-org"
    INPUT_RES_TYPE = resources.PATIENT
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("contact*.organization", "generalPractitioner*", "managingOrganization")


class PractOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "pract-org"
    INPUT_RES_TYPE = resources.PRACTITIONER
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("qualification*.issuer",)


class PractRoleOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "practrole-org"
    INPUT_RES_TYPE = resources.PRACTITIONER_ROLE
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("organization",)


class ProcedureOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "proc-org"
    INPUT_RES_TYPE = resources.PROCEDURE
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("performer*.actor", "performer*.onBehalfOf")


class ServReqOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "servreq-org"
    INPUT_RES_TYPE = resources.SERVICE_REQUEST
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("requester", "performer*")


class OrgOrgTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "org-org"
    INPUT_RES_TYPE = resources.ORGANIZATION
    OUTPUT_RES_TYPE = resources.ORGANIZATION
    REFS = ("partOf",)


ORGANIZATION_TASKS = [
    DevOrgTask,
    DxReportOrgTask,
    DocOrgTask,
    EncOrgTask,
    ImmOrgTask,
    LocOrgTask,
    MedReqOrgTask,
    ObsOrgTask,
    PatOrgTask,
    PractOrgTask,
    PractRoleOrgTask,
    ProcedureOrgTask,
    ServReqOrgTask,
    OrgOrgTask,
]
