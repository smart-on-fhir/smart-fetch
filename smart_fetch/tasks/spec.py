"""
Hydration tasks for Specimen.

Specimen is a patient-linked resource, so we shouldn't normally need this hydration task.
But because Epic doesn't really let you crawl Specimen (it only lets you search on _id),
we need to hydrate at least in the Epic case. And since we have the task, we might as well hydrate
in all cases in case an EHR hides some on us.
"""

from smart_fetch import hydrate_utils, resources


class DxReportSpecTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "dxr-spec"
    INPUT_RES_TYPE = resources.DIAGNOSTIC_REPORT
    OUTPUT_RES_TYPE = resources.SPECIMEN
    REFS = ("specimen*",)


class ObsSpecTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "obs-spec"
    INPUT_RES_TYPE = resources.OBSERVATION
    OUTPUT_RES_TYPE = resources.SPECIMEN
    REFS = ("specimen",)


class ServReqSpecTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "servreq-spec"
    INPUT_RES_TYPE = resources.SERVICE_REQUEST
    OUTPUT_RES_TYPE = resources.SPECIMEN
    REFS = ("specimen*",)


SPECIMEN_TASKS = [DxReportSpecTask, ObsSpecTask, ServReqSpecTask]
