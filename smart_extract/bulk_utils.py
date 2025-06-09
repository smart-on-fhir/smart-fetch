import os


def export_url(fhir_url: str, group: str) -> str:
    if group:
        return os.path.join(fhir_url, "Group", group)
    else:
        return fhir_url
