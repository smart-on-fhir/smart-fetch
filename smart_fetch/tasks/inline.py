import base64
import email
import hashlib
from functools import partial

import cumulus_fhir_support as cfs

from smart_fetch import hydrate_utils, resources


def parse_mimetypes(mimetypes: str | None) -> set[str]:
    if mimetypes is None:
        return {"text/plain", "text/html", "application/xhtml+xml"}

    return set(mimetypes.casefold().split(","))


def parse_content_type(content_type: str) -> tuple[str, str]:
    """Returns (mimetype, encoding)"""
    msg = email.message.EmailMessage()
    msg["content-type"] = content_type
    return msg.get_content_type(), msg.get_content_charset("utf8")


async def _inline_resource(mimetypes: set[str], client, resource: dict) -> hydrate_utils.Result:
    match resource.get("resourceType"):
        case "DiagnosticReport":
            attachments = resource.get("presentedForm", [])
        case "DocumentReference":
            attachments = [
                content["attachment"]
                for content in resource.get("content", [])
                if "attachment" in content
            ]
        case _:
            attachments = []  # don't do anything, but we will leave the resource line in place

    if not attachments:
        return [(resource, hydrate_utils.TaskResultReason.IGNORED)]

    result = [
        [None, await _inline_attachment(client, attachment, mimetypes=mimetypes)]
        for attachment in attachments
    ]
    result[0][0] = resource  # write back the original resource

    return result


async def _inline_attachment(
    client: cfs.FhirClient, attachment: dict, *, mimetypes: set[str]
) -> hydrate_utils.TaskResultReason:
    # First, check if we should even examine this attachment
    if "contentType" not in attachment:
        return hydrate_utils.TaskResultReason.IGNORED
    mimetype, _charset = parse_content_type(attachment["contentType"])
    if mimetype not in mimetypes:
        return hydrate_utils.TaskResultReason.IGNORED

    # OK - this is a valid attachment to process

    if "data" in attachment:
        return hydrate_utils.TaskResultReason.ALREADY_DONE

    if "url" not in attachment:
        # neither data nor url... nothing to do
        return hydrate_utils.TaskResultReason.IGNORED

    try:
        response = await client.request(
            "GET",
            attachment["url"],
            # We need to pass Accept to get the raw data, not a Binary FHIR object.
            # See https://www.hl7.org/fhir/binary.html
            headers={"Accept": mimetype},
        )
    except cfs.FatalNetworkError:
        return hydrate_utils.TaskResultReason.FATAL_ERROR
    except cfs.TemporaryNetworkError:
        return hydrate_utils.TaskResultReason.RETRY_ERROR

    attachment["data"] = base64.standard_b64encode(response.content).decode("ascii")
    # Overwrite other associated metadata with latest info (existing metadata might now be stale)
    attachment["contentType"] = f"{mimetype}; charset={response.encoding}"
    attachment["size"] = len(response.content)
    sha1_hash = hashlib.sha1(response.content).digest()  # noqa: S324
    attachment["hash"] = base64.standard_b64encode(sha1_hash).decode("ascii")

    return hydrate_utils.TaskResultReason.NEWLY_DONE


class InlineTask(hydrate_utils.Task):
    async def run(self, workdir: str, mimetypes: str | None = None, **kwargs) -> None:
        mimetypes = parse_mimetypes(mimetypes)
        stats = await hydrate_utils.process(
            client=self.client,
            task_name=self.NAME,
            desc="Inlining",
            workdir=workdir,
            input_type=self.INPUT_RES_TYPE,
            callback=partial(self.process_one, mimetypes=mimetypes),
            append=False,
        )
        if stats:
            stats.print("inlined", f"{self.INPUT_RES_TYPE}s", "Attachments")

    async def process_one(
        self, resource: dict, id_pool: set[str], **kwargs
    ) -> hydrate_utils.Result:
        del id_pool
        mimetypes = kwargs["mimetypes"]
        return await _inline_resource(mimetypes, self.client, resource)


class InlineDocTask(InlineTask):
    NAME = "doc-inline"
    INPUT_RES_TYPE = resources.DOCUMENT_REFERENCE
    OUTPUT_RES_TYPE = resources.DOCUMENT_REFERENCE


class InlineDxrTask(InlineTask):
    NAME = "dxr-inline"
    INPUT_RES_TYPE = resources.DIAGNOSTIC_REPORT
    OUTPUT_RES_TYPE = resources.DIAGNOSTIC_REPORT


INLINE_TASKS = [InlineDocTask, InlineDxrTask]
