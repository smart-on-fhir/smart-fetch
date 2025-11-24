from smart_fetch import cli_utils, crawl_utils, hydrate_utils, resources


class AllergyPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "allergy-pract"
    INPUT_RES_TYPE = resources.ALLERGY_INTOLERANCE
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("recorder", "asserter")


class AllergyPractitionerRoleTask(AllergyPractitionerTask):
    NAME = "allergy-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class ConditionPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "cond-pract"
    INPUT_RES_TYPE = resources.CONDITION
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("recorder", "asserter")


class ConditionPractitionerRoleTask(ConditionPractitionerTask):
    NAME = "cond-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class DxReportPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "dxr-pract"
    INPUT_RES_TYPE = resources.DIAGNOSTIC_REPORT
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("performer*", "resultsInterpreter*")


class DxReportPractitionerRoleTask(DxReportPractitionerTask):
    NAME = "dxr-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class DocPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "doc-pract"
    INPUT_RES_TYPE = resources.DOCUMENT_REFERENCE
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("subject", "author*", "authenticator")


class DocPractitionerRoleTask(DocPractitionerTask):
    NAME = "doc-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class EncPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "enc-pract"
    INPUT_RES_TYPE = resources.ENCOUNTER
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("participant*.individual",)


class EncPractitionerRoleTask(EncPractitionerTask):
    NAME = "enc-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class ImmPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "imm-pract"
    INPUT_RES_TYPE = resources.IMMUNIZATION
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("performer*.actor",)


class ImmPractitionerRoleTask(ImmPractitionerTask):
    NAME = "imm-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class MedReqPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "medreq-pract"
    INPUT_RES_TYPE = resources.MEDICATION_REQUEST
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("reportedReference", "requester", "performer", "recorder")


class MedReqPractitionerRoleTask(MedReqPractitionerTask):
    NAME = "medreq-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class ObsPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "obs-pract"
    INPUT_RES_TYPE = resources.OBSERVATION
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("performer*",)


class ObsPractitionerRoleTask(ObsPractitionerTask):
    NAME = "obs-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class PatPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "pat-pract"
    INPUT_RES_TYPE = resources.PATIENT
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("generalPractitioner*",)


class PatPractitionerRoleTask(PatPractitionerTask):
    NAME = "pat-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class ProcedurePractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "procedure-pract"
    INPUT_RES_TYPE = resources.PROCEDURE
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("recorder", "asserter", "performer*.actor")


class ProcedurePractitionerRoleTask(ProcedurePractitionerTask):
    NAME = "procedure-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class ServReqPractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "servreq-pract"
    INPUT_RES_TYPE = resources.SERVICE_REQUEST
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("requester", "performer*")


class ServReqPractitionerRoleTask(ServReqPractitionerTask):
    NAME = "servreq-practrole"
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE


class PractitionerRolePractitionerTask(hydrate_utils.ReferenceDownloadTask):
    NAME = "practrole-pract"
    INPUT_RES_TYPE = resources.PRACTITIONER_ROLE
    OUTPUT_RES_TYPE = resources.PRACTITIONER
    REFS = ("practitioner",)


# Sometimes nothing directly links to a PractitionerRole, but the server will still have them.
# Epic is in that boat. The Role information is still very useful though. So grab the roles for
# all Practitioners we know about by manually searching.
class PractitionerPractitionerRoleTask(hydrate_utils.Task):
    NAME = "pract-practrole"
    INPUT_RES_TYPE = resources.PRACTITIONER
    OUTPUT_RES_TYPE = resources.PRACTITIONER_ROLE

    async def run(self, workdir: str, source_dir: str | None = None, **kwargs) -> None:
        stats = await hydrate_utils.process(
            task_name=self.NAME,
            desc="Downloading",
            workdir=workdir,
            source_dir=source_dir or workdir,
            input_type=self.INPUT_RES_TYPE,
            output_type=self.OUTPUT_RES_TYPE,
            callback=self.process_one,
            file_slug="referenced",
            compress=self.compress,
        )
        if stats:
            stats.print("downloaded", f"{self.OUTPUT_RES_TYPE}s")

    async def process_one(
        self, resource: dict, id_pool: set[str], **kwargs
    ) -> hydrate_utils.Result:
        url = f"{resources.PRACTITIONER_ROLE}?practitioner={resource['id']}"
        results = []
        async for resource in crawl_utils.crawl_bundle_chain(self.client, url):
            if resource.get("resourceType") != self.OUTPUT_RES_TYPE:  # OperationOutcome
                cli_utils.maybe_print_type_mismatch(self.OUTPUT_RES_TYPE, resource)
                # This could be misleading - it might be a retry error. But it's awkward to
                # grab which kind it was, using crawl_bundle_chain. Just assume fatal for now.
                results.append((None, hydrate_utils.TaskResultReason.FATAL_ERROR))
                continue

            ref = f"{resources.PRACTITIONER_ROLE}/{resource['id']}"
            if ref in id_pool:
                results.append((None, hydrate_utils.TaskResultReason.ALREADY_DONE))
                continue
            id_pool.add(ref)

            results.append((resource, hydrate_utils.TaskResultReason.NEWLY_DONE))

        return results


PRACTITIONER_TASKS = [
    AllergyPractitionerTask,
    AllergyPractitionerRoleTask,
    ConditionPractitionerTask,
    ConditionPractitionerRoleTask,
    DxReportPractitionerTask,
    DxReportPractitionerRoleTask,
    DocPractitionerTask,
    DocPractitionerRoleTask,
    EncPractitionerTask,
    EncPractitionerRoleTask,
    ImmPractitionerTask,
    ImmPractitionerRoleTask,
    MedReqPractitionerTask,
    MedReqPractitionerRoleTask,
    ObsPractitionerTask,
    ObsPractitionerRoleTask,
    PatPractitionerTask,
    PatPractitionerRoleTask,
    ProcedurePractitionerTask,
    ProcedurePractitionerRoleTask,
    ServReqPractitionerTask,
    ServReqPractitionerRoleTask,
    PractitionerPractitionerRoleTask,
    PractitionerRolePractitionerTask,
]
