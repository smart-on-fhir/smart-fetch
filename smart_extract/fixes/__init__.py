from smart_extract import resources

from .inline import fix_doc_inline, fix_dxr_inline
from .meds import fix_meds
from .obs import fix_obs_dxr, fix_obs_members

all_fixes = {
    "doc-inline": (resources.DOCUMENT_REFERENCE, fix_doc_inline),
    "dxr-inline": (resources.DIAGNOSTIC_REPORT, fix_dxr_inline),
    "meds": (resources.MEDICATION_REQUEST, fix_meds),
    "obs-dxr": (resources.OBSERVATION, fix_obs_dxr),
    "obs-members": (resources.OBSERVATION, fix_obs_members),
}
