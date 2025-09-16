from .inline import INLINE_TASKS
from .loc import LOCATION_TASKS
from .meds import MEDICATION_TASKS
from .obs import OBSERVATION_TASKS
from .org import ORGANIZATION_TASKS
from .pract import PRACTITIONER_TASKS

all_tasks = {
    "inline": INLINE_TASKS,
    "medication": MEDICATION_TASKS,
    "observation": OBSERVATION_TASKS,
    "practitioner": PRACTITIONER_TASKS,  # depends on lots
    "location": LOCATION_TASKS,  # depends on PractitionerRole
    "organization": ORGANIZATION_TASKS,  # depends on Location
}
