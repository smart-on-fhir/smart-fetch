import abc

from smart_fetch import hydrate_utils, resources


class ObsTask(hydrate_utils.ReferenceDownloadTask, abc.ABC):
    OUTPUT_RES_TYPE = resources.OBSERVATION


class DxrResultsTask(ObsTask):
    NAME = "dxr-results"
    INPUT_RES_TYPE = resources.DIAGNOSTIC_REPORT
    REFS = ("result*",)
    FILE_SLUG = "results"


class ObsMembersTask(ObsTask):
    NAME = "obs-members"
    INPUT_RES_TYPE = resources.OBSERVATION
    REFS = ("hasMember*",)
    FILE_SLUG = "members"


OBSERVATION_TASKS = [DxrResultsTask, ObsMembersTask]
