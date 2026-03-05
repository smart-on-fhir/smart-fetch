from smart_fetch import hydrate_utils, resources


class MedDispMedTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "meddisp-med"
    INPUT_RES_TYPE = resources.MEDICATION_DISPENSE
    OUTPUT_RES_TYPE = resources.MEDICATION
    REFS = ("medicationReference",)


class MedReqMedTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "medreq-med"
    INPUT_RES_TYPE = resources.MEDICATION_REQUEST
    OUTPUT_RES_TYPE = resources.MEDICATION
    REFS = ("medicationReference",)


MEDICATION_TASKS = [MedDispMedTask, MedReqMedTask]
