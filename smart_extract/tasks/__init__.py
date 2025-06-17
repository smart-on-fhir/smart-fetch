from collections.abc import Callable

from smart_extract import resources

from .inline import task_doc_inline, task_dxr_inline
from .meds import task_meds
from .obs import task_obs_dxr, task_obs_members

TaskFunc = Callable

all_tasks: dict[str, tuple[str, TaskFunc]] = {
    "doc-inline": (resources.DOCUMENT_REFERENCE, task_doc_inline),
    "dxr-inline": (resources.DIAGNOSTIC_REPORT, task_dxr_inline),
    "meds": (resources.MEDICATION_REQUEST, task_meds),
    "obs-dxr": (resources.OBSERVATION, task_obs_dxr),
    "obs-members": (resources.OBSERVATION, task_obs_members),
}
