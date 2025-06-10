import httpx

from smart_extract import resources
from tests import utils


class FixDocInlineTests(utils.TestCase):
    async def test_edge_cases(self):
        """All sorts of edge cases"""
        docrefs = [
            {
                "content": [
                    # Normal, happy. Why can't they all be like this?
                    {"attachment": {"url": "Binary/x", "contentType": "text/html"}},
                    # Already has data
                    {"attachment": {"url": "Binary/a", "data": "aaa", "contentType": "text/plain"}},
                    # Has neither data nor URL
                    {"attachment": {"contentType": "text/plain"}},
                    # No content type
                    {"attachment": {"url": "Binary/b"}},
                    # With existing size and hash (will be overwritten)
                    {
                        "attachment": {
                            "url": "Binary/y",
                            "size": 1,
                            "hash": "bogus",
                            "contentType": "text/plain",
                        }
                    },
                    # xhtml
                    {"attachment": {"url": "Binary/z", "contentType": "application/xhtml+xml"}},
                    # error case
                    {"attachment": {"url": "Binary/error", "contentType": "text/plain"}},
                    # ignored mimetype
                    {"attachment": {"url": "Binary/c", "contentType": "text/custom"}},
                ]
            },
            {},  # No attachments
        ]
        self.write_res(resources.DOCUMENT_REFERENCE, docrefs)

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            match res_id:
                case "x":
                    return httpx.Response(
                        200,
                        request=request,
                        content=b"<body>hi</body>",
                        headers={
                            "Content-Type": "text/html; charset=ascii",
                        },
                    )
                case "y":
                    return httpx.Response(200, request=request, text="hello")
                case "z":
                    return httpx.Response(
                        200,
                        request=request,
                        content=b"<body>bye</body>",
                        headers={
                            "Content-Type": "application/xhtml+xml; charset=utf8",
                        },
                    )
                case "error":
                    return httpx.Response(404)
                case _:
                    assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)
        await self.cli("fix", self.folder, "doc-inline")

        self.assert_folder(
            {
                resources.DOCUMENT_REFERENCE: {
                    ".fix.done": None,
                    f"{resources.DOCUMENT_REFERENCE}.ndjson.gz": [
                        {
                            "resourceType": resources.DOCUMENT_REFERENCE,
                            "id": "0",
                            "content": [
                                {
                                    "attachment": {
                                        "url": "Binary/x",
                                        "contentType": "text/html; charset=ascii",
                                        "data": "PGJvZHk+aGk8L2JvZHk+",
                                        "size": 15,
                                        "hash": "TO1v+xI5ie/MceVDtUjyhQS8o0I=",
                                    }
                                },
                                {
                                    "attachment": {
                                        "url": "Binary/a",
                                        "data": "aaa",
                                        "contentType": "text/plain",
                                    }
                                },
                                {"attachment": {"contentType": "text/plain"}},
                                {"attachment": {"url": "Binary/b"}},
                                {
                                    "attachment": {
                                        "url": "Binary/y",
                                        "contentType": "text/plain; charset=utf-8",
                                        "data": "aGVsbG8=",
                                        "size": 5,
                                        "hash": "qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                                    }
                                },
                                {
                                    "attachment": {
                                        "url": "Binary/z",
                                        "contentType": "application/xhtml+xml; charset=utf8",
                                        "data": "PGJvZHk+YnllPC9ib2R5Pg==",
                                        "size": 16,
                                        "hash": "ybLPOkRO4i3shB3X4HDeMpAK6U4=",
                                    }
                                },
                                {
                                    "attachment": {
                                        "url": "Binary/error",
                                        "contentType": "text/plain",
                                    }
                                },
                                {"attachment": {"url": "Binary/c", "contentType": "text/custom"}},
                            ],
                        },
                        {"resourceType": resources.DOCUMENT_REFERENCE, "id": "1"},
                    ],
                },
            }
        )


class FixDxrInlineTests(utils.TestCase):
    async def test_basic(self):
        """Simple dxr-inline fix from scratch, edge cases are handled above"""
        dxr = [{"presentedForm": [{"url": "Binary/x", "contentType": "text/plain"}]}]
        self.write_res(resources.DIAGNOSTIC_REPORT, dxr)

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            match res_id:
                case "x":
                    return httpx.Response(200, request=request, text="hello")
                case _:
                    assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)
        await self.cli("fix", self.folder, "dxr-inline")

        self.assert_folder(
            {
                resources.DIAGNOSTIC_REPORT: {
                    ".fix.done": None,
                    f"{resources.DIAGNOSTIC_REPORT}.ndjson.gz": [
                        {
                            "resourceType": resources.DIAGNOSTIC_REPORT,
                            "id": "0",
                            "presentedForm": [
                                {
                                    "url": "Binary/x",
                                    "contentType": "text/plain; charset=utf-8",
                                    "data": "aGVsbG8=",
                                    "size": 5,
                                    "hash": "qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                                },
                            ],
                        },
                    ],
                },
            }
        )
