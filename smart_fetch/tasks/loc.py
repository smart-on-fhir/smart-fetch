from smart_fetch import hydrate_utils, resources


class DeviceLocationTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "dev-loc"
    INPUT_RES_TYPE = resources.DEVICE
    OUTPUT_RES_TYPE = resources.LOCATION
    REFS = ("location",)


class DxReportLocationTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "dxr-loc"
    INPUT_RES_TYPE = resources.DIAGNOSTIC_REPORT
    OUTPUT_RES_TYPE = resources.LOCATION
    REFS = ("subject",)


class EncLocationTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "enc-loc"
    INPUT_RES_TYPE = resources.ENCOUNTER
    OUTPUT_RES_TYPE = resources.LOCATION
    REFS = ("hospitalization.origin", "hospitalization.destination", "location*.location")


class ImmLocationTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "imm-loc"
    INPUT_RES_TYPE = resources.IMMUNIZATION
    OUTPUT_RES_TYPE = resources.LOCATION
    REFS = ("location",)


class ObsLocationTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "obs-loc"
    INPUT_RES_TYPE = resources.OBSERVATION
    OUTPUT_RES_TYPE = resources.LOCATION
    REFS = ("subject",)


class PractRoleLocationTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "practrole-loc"
    INPUT_RES_TYPE = resources.PRACTITIONER_ROLE
    OUTPUT_RES_TYPE = resources.LOCATION
    REFS = ("location*",)


class ProcedureLocationTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "proc-loc"
    INPUT_RES_TYPE = resources.PROCEDURE
    OUTPUT_RES_TYPE = resources.LOCATION
    REFS = ("location",)


class ServReqLocationTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "servreq-loc"
    INPUT_RES_TYPE = resources.SERVICE_REQUEST
    OUTPUT_RES_TYPE = resources.LOCATION
    REFS = ("subject", "locationReference*")


class LocLocationTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "loc-loc"
    INPUT_RES_TYPE = resources.LOCATION
    OUTPUT_RES_TYPE = resources.LOCATION
    REFS = ("partOf",)


LOCATION_TASKS = [
    DeviceLocationTask,
    DxReportLocationTask,
    EncLocationTask,
    ImmLocationTask,
    ObsLocationTask,
    PractRoleLocationTask,
    ProcedureLocationTask,
    ServReqLocationTask,
    LocLocationTask,
]
