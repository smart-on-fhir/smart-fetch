"""
Hydration tasks for EpisodeOfCare.

EpisodeOfCare is a patient-linked resource, so we shouldn't normally need this hydration task.
But because Epic doesn't really let you crawl EpisodeOfCare (it needs a specific type= argument),
we need to hydrate at least in the Epic case. And since we have the task, we might as well hydrate
in all cases in case an EHR hides some on us.
"""

from smart_fetch import hydrate_utils, resources


class DocRefEpOfCareTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "docref-epofcare"
    INPUT_RES_TYPE = resources.DOCUMENT_REFERENCE
    OUTPUT_RES_TYPE = resources.EPISODE_OF_CARE
    REFS = ("context.encounter*",)


class EncEpOfCareTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "enc-epofcare"
    INPUT_RES_TYPE = resources.ENCOUNTER
    OUTPUT_RES_TYPE = resources.EPISODE_OF_CARE
    REFS = ("episodeOfCare*",)


class MedDispEpOfCareTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "meddisp-epofcare"
    INPUT_RES_TYPE = resources.MEDICATION_DISPENSE
    OUTPUT_RES_TYPE = resources.EPISODE_OF_CARE
    REFS = ("context",)


EPISODE_OF_CARE_TASKS = [DocRefEpOfCareTask, EncEpOfCareTask, MedDispEpOfCareTask]
