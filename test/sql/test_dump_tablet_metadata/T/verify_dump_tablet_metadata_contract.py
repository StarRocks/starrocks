#!/usr/bin/env python3
"""Exercise both valid response-envelope shapes without a live cluster."""

import json

import verify_dump_tablet_metadata as verifier


HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
}


def verify_document(document):
    original_request = verifier.request
    verifier.request = lambda _url, _read_body: (200, HEADERS, json.dumps(document).encode("utf-8"))
    try:
        verifier.verify_exact("http://127.0.0.1:1", 11979, 23, False)
    finally:
        verifier.request = original_request


def require_rejected(document):
    try:
        verify_document(document)
    except RuntimeError:
        return
    raise AssertionError("invalid response envelope was accepted")


def main():
    verify_document({"metadata": {"id": 11979, "version": 23}})
    verify_document(
        {
            "metadata": {"id": 11979, "version": 23},
            "redacted_fields": ["starrocks.lake.FileMetaPB.encryption_meta"],
        }
    )
    require_rejected({"metadata": {"id": 11979, "version": 23}, "redacted_fields": []})
    require_rejected(
        {"metadata": {"id": 11979, "version": 23}, "redacted_fields": ["starrocks.lake.FileMetaPB.path"]}
    )
    print("response_envelope_contract=PASS")


if __name__ == "__main__":
    main()
