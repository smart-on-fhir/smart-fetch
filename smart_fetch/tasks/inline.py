import base64
import email
import hashlib
from functools import partial

import cumulus_fhir_support as cfs
import rich.progress

from smart_fetch import hydrate_utils, resources


def parse_mimetypes(mimetypes: str | None) -> set[str]:
    if mimetypes is None:
        return {"text/plain", "text/html", "application/xhtml+xml"}

    return set(mimetypes.casefold().split(","))


def parse_content_type(content_type: str) -> (str, str):
    """Returns (mimetype, encoding)"""
    msg = email.message.EmailMessage()
    msg["content-type"] = content_type
    return msg.get_content_type(), msg.get_content_charset("utf8")


async def _inline_task(
    *,
    client: cfs.FhirClient,
    task_name: str,
    input_type: str,
    workdir: str,
    mimetypes: str | None = None,
    progress: rich.progress.Progress | None = None,
) -> None:
    mimetypes = parse_mimetypes(mimetypes)
    stats = await hydrate_utils.process(
        client=client,
        task_name=task_name,
        desc="Inlining",
        workdir=workdir,
        input_type=input_type,
        callback=partial(_inline_resource, mimetypes),
        append=False,
        progress=progress,
    )
    if stats:
        stats.print("inlined", f"{input_type}s", "Attachments")


async def task_doc_inline(
    client: cfs.FhirClient,
    workdir: str,
    mimetypes: str | None = None,
    progress: rich.progress.Progress | None = None,
    **kwargs,
):
    await _inline_task(
        client=client,
        task_name="doc-inline",
        workdir=workdir,
        input_type=resources.DOCUMENT_REFERENCE,
        mimetypes=mimetypes,
        progress=progress,
    )


async def task_dxr_inline(
    client: cfs.FhirClient,
    workdir: str,
    mimetypes: str | None = None,
    progress: rich.progress.Progress | None = None,
    **kwargs,
):
    await _inline_task(
        client=client,
        task_name="dxr-inline",
        workdir=workdir,
        input_type=resources.DIAGNOSTIC_REPORT,
        mimetypes=mimetypes,
        progress=progress,
    )


async def _inline_resource(
    mimetypes: set[str], client, resource: dict, id_pool: set[str]
) -> hydrate_utils.Result:
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
