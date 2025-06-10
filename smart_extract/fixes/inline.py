import base64
import hashlib
from functools import partial

from cumulus_etl import errors, fhir

from smart_extract import fix_utils, resources


def parse_mimetypes(mimetypes: str | None) -> set[str]:
    if mimetypes is None:
        return {"text/plain", "text/html", "application/xhtml+xml"}

    return set(mimetypes.casefold().split(","))


async def fix_doc_inline(client, args):
    mimetypes = parse_mimetypes(args.mimetypes)
    stats = await fix_utils.process(
        client=client,
        folder=args.folder,
        input_type=resources.DOCUMENT_REFERENCE,
        fix_name="doc-inline",
        desc="Inlining",
        callback=partial(_inline_resource, mimetypes),
        append=False,
    )
    stats.print("inlined", f"{resources.DOCUMENT_REFERENCE}s", "Attachments")


async def fix_dxr_inline(client, args):
    mimetypes = parse_mimetypes(args.mimetypes)
    stats = await fix_utils.process(
        client=client,
        folder=args.folder,
        input_type=resources.DIAGNOSTIC_REPORT,
        fix_name="dxr-inline",
        desc="Inlining",
        callback=partial(_inline_resource, mimetypes),
        append=False,
    )
    stats.print("inlined", f"{resources.DIAGNOSTIC_REPORT}s", "Attachments")


async def _inline_resource(
    mimetypes: set[str], client, resource: dict, id_pool: set[str]
) -> fix_utils.Result:
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
        return [(resource, fix_utils.FixResultReason.IGNORED)]

    result = [
        [None, await _inline_attachment(client, attachment, mimetypes=mimetypes)]
        for attachment in attachments
    ]
    result[0][0] = resource  # write back the original resource

    return result


async def _inline_attachment(
    client: fhir.FhirClient, attachment: dict, *, mimetypes: set[str]
) -> fix_utils.FixResultReason:
    # First, check if we should even examine this attachment
    if "contentType" not in attachment:
        return fix_utils.FixResultReason.IGNORED
    mimetype, _charset = fhir.parse_content_type(attachment["contentType"])
    if mimetype not in mimetypes:
        return fix_utils.FixResultReason.IGNORED

    # OK - this is a valid attachment to process

    if "data" in attachment:
        return fix_utils.FixResultReason.ALREADY_DONE

    if "url" not in attachment:
        # neither data nor url... nothing to do
        return fix_utils.FixResultReason.IGNORED

    try:
        response = await fhir.request_attachment(client, attachment)
    except errors.FatalNetworkError:
        return fix_utils.FixResultReason.FATAL_ERROR
    except errors.TemporaryNetworkError:
        return fix_utils.FixResultReason.RETRY_ERROR

    attachment["data"] = base64.standard_b64encode(response.content).decode("ascii")
    # Overwrite other associated metadata with latest info (existing metadata might now be stale)
    attachment["contentType"] = f"{mimetype}; charset={response.encoding}"
    attachment["size"] = len(response.content)
    sha1_hash = hashlib.sha1(response.content).digest()  # noqa: S324
    attachment["hash"] = base64.standard_b64encode(sha1_hash).decode("ascii")

    return fix_utils.FixResultReason.NEWLY_DONE
