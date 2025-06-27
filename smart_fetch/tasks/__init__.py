from collections.abc import Callable

from smart_fetch import resources

from .inline import task_doc_inline, task_dxr_inline
from .meds import task_meds
from .obs import task_obs_dxr, task_obs_members

TaskFunc = Callable

# TODO: make hydration tasks proper little classes, not this complicated dict of tuples
all_tasks: dict[str, tuple[str, str, TaskFunc]] = {
    "doc-inline": (resources.DOCUMENT_REFERENCE, resources.DOCUMENT_REFERENCE, task_doc_inline),
    "dxr-inline": (resources.DIAGNOSTIC_REPORT, resources.DIAGNOSTIC_REPORT, task_dxr_inline),
    "dxr-results": (resources.DIAGNOSTIC_REPORT, resources.OBSERVATION, task_obs_dxr),
    "meds": (resources.MEDICATION_REQUEST, resources.MEDICATION, task_meds),
    "obs-members": (resources.OBSERVATION, resources.OBSERVATION, task_obs_members),
}
