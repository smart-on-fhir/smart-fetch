import json
import os
from unittest import mock

import httpx

from smart_fetch import resources
from tests import utils


class HydrateDocInlineTests(utils.TestCase):
    @mock.patch("asyncio.sleep")
    async def test_edge_cases(self, mock_sleep):
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
                    # fatal error case
                    {"attachment": {"url": "Binary/fatal-error", "contentType": "text/plain"}},
                    # retry error case
                    {"attachment": {"url": "Binary/retry-error", "contentType": "text/plain"}},
                    # ignored mimetype
                    {"attachment": {"url": "Binary/c", "contentType": "text/custom"}},
                ]
            },
            {},  # No attachments at all
            {"resourceType": resources.PROCEDURE},  # Wrong type
        ]
        self.write_res(resources.DOCUMENT_REFERENCE, docrefs)

        # Write a random uncompressed file out, to confirm we actually look at every source file
        # and inline each of them
        with open(os.path.join(self.folder, "extra.jsonl"), "w", encoding="utf8") as f:
            json.dump(
                {
                    "resourceType": resources.DOCUMENT_REFERENCE,
                    "id": "extra",
                    "content": [{"attachment": {"url": "Binary/x", "contentType": "text/html"}}],
                },
                f,
            )

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
                case "fatal-error":
                    return httpx.Response(404)
                case "retry-error":
                    return httpx.Response(500)
                case _:
                    assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)
        await self.cli("hydrate", self.folder, "--hydration-tasks=doc-inline")

        self.assert_folder(
            {
                "extra.jsonl": [
                    {
                        "resourceType": resources.DOCUMENT_REFERENCE,
                        "id": "extra",
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
                        ],
                    },
                ],
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
                                    "url": "Binary/fatal-error",
                                    "contentType": "text/plain",
                                }
                            },
                            {
                                "attachment": {
                                    "url": "Binary/retry-error",
                                    "contentType": "text/plain",
                                }
                            },
                            {"attachment": {"url": "Binary/c", "contentType": "text/custom"}},
                        ],
                    },
                    {"resourceType": resources.DOCUMENT_REFERENCE, "id": "1"},
                    {"resourceType": resources.PROCEDURE, "id": "2"},
                ],
            }
        )

    async def test_custom_mimetypes(self):
        docrefs = [
            {
                "content": [
                    {"attachment": {"url": "Binary/a", "contentType": "text/html"}},
                    {"attachment": {"url": "Binary/b", "contentType": "text/plain"}},
                    {"attachment": {"url": "Binary/c", "contentType": "application/xhtml+xml"}},
                    {"attachment": {"url": "Binary/d", "contentType": "text/custom1"}},
                    {"attachment": {"url": "Binary/e", "contentType": "application/custom2"}},
                ]
            },
        ]
        self.write_res(resources.DOCUMENT_REFERENCE, docrefs)

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            match res_id:
                case "d":
                    return httpx.Response(
                        200,
                        request=request,
                        content=b"hello",
                        headers={"Content-Type": "text/custom1; charset=utf8"},
                    )
                case "e":
                    return httpx.Response(
                        200,
                        request=request,
                        content=b"hello",
                        headers={"Content-Type": "application/custom2; charset=utf8"},
                    )
                case _:
                    assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)
        await self.cli(
            "hydrate",
            self.folder,
            "--hydration-tasks=doc-inline",
            "--mimetypes=tEXt/Custom1,application/custom2",
        )

        self.assert_folder(
            {
                f"{resources.DOCUMENT_REFERENCE}.ndjson.gz": [
                    {
                        "resourceType": resources.DOCUMENT_REFERENCE,
                        "id": "0",
                        "content": [
                            {"attachment": {"url": "Binary/a", "contentType": "text/html"}},
                            {"attachment": {"url": "Binary/b", "contentType": "text/plain"}},
                            {
                                "attachment": {
                                    "url": "Binary/c",
                                    "contentType": "application/xhtml+xml",
                                }
                            },
                            {
                                "attachment": {
                                    "url": "Binary/d",
                                    "contentType": "text/custom1; charset=utf8",
                                    "data": "aGVsbG8=",
                                    "hash": "qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                                    "size": 5,
                                }
                            },
                            {
                                "attachment": {
                                    "url": "Binary/e",
                                    "contentType": "application/custom2; charset=utf8",
                                    "data": "aGVsbG8=",
                                    "hash": "qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                                    "size": 5,
                                }
                            },
                        ],
                    },
                ],
            }
        )


class HydrateDxrInlineTests(utils.TestCase):
    async def test_basic(self):
        """Simple dxr-inline task from scratch, edge cases are handled above"""
        dxr = [{"presentedForm": [{"url": "Binary/x", "contentType": "text/plain"}]}]
        self.write_res(resources.DIAGNOSTIC_REPORT, dxr)

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            match res_id:
                case "x":
                    return httpx.Response(200, request=request, text="hello")
                case _:
                    assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)
        await self.cli("hydrate", self.folder, "--hydration-tasks=dxr-inline")

        self.assert_folder(
            {
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
            }
        )
