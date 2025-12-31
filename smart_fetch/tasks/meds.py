from smart_fetch import hydrate_utils, resources


class MedsTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "meds"
    INPUT_RES_TYPE = resources.MEDICATION_REQUEST
    OUTPUT_RES_TYPE = resources.MEDICATION
    REFS = ("medicationReference",)
    FILE_SLUG = None  # only one Medication source, don't need to differentiate


MEDICATION_TASKS = [MedsTask]
