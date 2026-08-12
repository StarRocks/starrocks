# Exact Tablet Metadata Dump Design

Status: implemented

## Summary

`dump_tablet_metadata` is a diagnostic API for parsing one known Lake tablet metadata version. It is not a metadata discovery API.

The existing endpoint is narrowed in place:

```http
GET /api/cloudnative/dump_tablet_metadata/{TabletId}?version={Version}&is_bundle={true|false}
```

One successful request returns one logical `TabletMetadataPB`. The endpoint never lists a metadata directory, discovers versions, chooses the latest version, or dumps multiple tablets. Metadata discovery belongs to FE, the AWS CLI, or another purpose-built inventory tool.

This design optimizes for agent callers. An agent can compose multiple narrow tools without the usability cost that a human experiences when manually calling many APIs. The API therefore favors a small capability surface, deterministic work, and hard server-side resource bounds over one-call convenience.

## Problem

The current implementation uses the path `TabletId` only to locate a shared metadata directory. It then iterates every object whose name ends in `.meta`, parses each object, converts every protobuf to JSON, and writes an array response. See [the current action](../../be/src/http/action/lake/dump_tablet_metadata_action.cpp).

This behavior has three problems:

1. The request does not identify the data it will return. A request for one tablet can return metadata for every tablet and version in the same partition directory.
2. Work and response size grow with directory cardinality. The caller cannot infer or bound the cost from the request.
3. `HttpStreamChannel` is not an effective backpressure boundary in this path. The synchronous handler occupies the same libevent worker that must later drain the socket, so queued response data can approach the complete unbounded response.

The incident exposed the implementation bug, but the root design issue is broader: the endpoint combines discovery, physical-format parsing, batch export, and transport in one unrestricted operation.

## Product Positioning

### Intended use

The caller already knows:

- the tablet ID;
- the metadata version;
- whether that version uses bundled metadata storage.

The caller needs StarRocks to turn the corresponding durable object into a logical `TabletMetadataPB`. The API provides value by handling:

- StarOS virtual locations and object-store credentials;
- metadata filename encoding;
- checksummed and legacy headerless protobuf files;
- bundle trailer, footer, page pointers, and checksums;
- extraction of one tablet page from a bundle;
- restoration of schemas stripped from bundled tablet pages;
- Lake metadata normalization across file-format generations.

### Tool composition

`dump_tablet_metadata` deliberately provides only simple, bounded online diagnosis: it parses one known tablet version into one logical metadata object. Complex work—including metadata discovery, batch inspection, full-bundle or raw-file analysis, malformed-object forensics, repair, and format conversion—is outside this API and should use `meta_tool`, with FE or object-store tooling used to locate and obtain the input.

The expected diagnostic workflow is:

```text
FE, logs, profiles, or AWS CLI
        |
        | discover tablet, version, and version-specific storage format
        v
dump_tablet_metadata
        |
        | parse exactly one logical metadata object
        v
agent analysis
```

FE is the preferred source for authoritative logical state such as visible versions. The AWS CLI or another object-store tool is appropriate for physical inventory, prefix listing, raw download, and break-glass investigation. `dump_tablet_metadata` complements those tools; it does not replace them.

## Goals

- Read one known tablet metadata version without listing a directory.
- Support standalone and bundled metadata storage.
- Return the same logical `TabletMetadataPB` shape for both layouts.
- Read durable object-store state rather than a cached or FE-synthesized value.
- Make the number and size of storage reads, response size, and endpoint concurrency bounded by server-owned limits.
- Fail before sending HTTP 200 when validation, reading, parsing, redaction, or serialization fails.
- Keep the endpoint read-only and avoid cache pollution.

## Non-goals

- Listing tablets or metadata versions.
- Returning the latest metadata version.
- Pagination, limits, cursors, or partial list results.
- Batch or range dumps.
- Accepting an arbitrary object-store path or URI.
- Automatically detecting a version's storage format.
- Automatically trying the opposite value of `is_bundle`.
- Field projection or caller-selectable redaction profiles.
- Pretty-printed JSON.
- FE synthesis of initial tablet metadata.
- General-purpose asynchronous HTTP streaming infrastructure.
- Compatibility with the previous diagnostic behavior.

The last item concerns only the old HTTP contract. Reading historical durable metadata is a separate data-compatibility requirement: the exact reader must continue to understand headerless standalone protobufs and legacy bundle footers that can still exist in object storage.

## System Model

### Metadata layouts

| Layout | Durable path | Payload | Exact read |
|---|---|---|---|
| Standalone, version 2 or later | `meta/{tablet_id:016X}_{version:016X}.meta` | `TabletMetadataPB`, optionally with a fixed checksum header | Read the bounded object and parse one protobuf |
| Bundle, version 2 or later | `meta/0000000000000000_{version:016X}.meta` | Tablet pages followed by `BundleTabletMetadataPB` footer and trailer | Range-read trailer, footer, and the requested tablet page |
| Per-tablet initial metadata | `meta/{tablet_id:016X}_0000000000000001.meta` | Standalone `TabletMetadataPB` | Read as standalone metadata |
| Shared initial metadata | `meta/0000000000000000_0000000000000001.meta` | Standalone `TabletMetadataPB`, not a bundle | Read as standalone metadata and map it to the requested logical tablet ID |
| Lightweight initial creation | No durable version 1 object | None | Return not found |

Path construction is defined by [LocationProvider](../../be/src/storage/lake/location_provider.h). Bundle construction and schema stripping are implemented in [TabletManager](../../be/src/storage/lake/tablet_manager.cpp).

### Relevant mechanisms

| Mechanism | Required behavior |
|---|---|
| Protobuf compatibility | Accept the checksummed Lake header format and legacy headerless protobuf files. Reject empty, truncated, length-mismatched, trailing-garbage, checksum-mismatched, or unparsable content. A headered object is valid only when its remaining bytes equal the declared protobuf length exactly. |
| Bundle footer | Support legacy `[footer][size]` and checksummed `[footer][adler32][size-with-flag]` layouts. Footer and page checksums use StarRocks' existing Adler-32 implementation. |
| Bundle schema restoration | Run `normalize_tablet_metadata_after_load` on the page, restore the current and referenced historical schemas, then run `force_cloud_native_pk_persistent_index` after the current schema is available. |
| Metadata cache | Bypass lookup and do not populate metadata cache. `fill_meta_cache=false` alone is insufficient; the read must also set `skip_meta_cache=true`. |
| Data and disk cache | Do not fill or read the local object cache for this diagnostic durable-state operation. |
| Vacuum | A requested historical version may legitimately be absent because it was vacuumed. Return not found. There is no read-side GC pin. |
| Format transition | `is_bundle` describes the requested version's physical format, not the table's current `file_bundling` property. Historical versions can use a different layout from the current table setting. |

### Explicit exclusions

- `FileSystem::iterate_dir` and `TabletManager::list_tablet_metadata` are never called.
- `TabletManager::get_metas_from_bundle_tablet_metadata` is not used because it reads the complete bundle and parses every tablet.
- The current `get_single_tablet_metadata` bundle implementation is not used because it calls `read_all()` on the complete bundle.
- Existing metadata-cache entries are not accepted as proof that a durable object exists.
- The version 1 FE fallback is not invoked.
- A failed standalone lookup does not trigger a bundle lookup, and a failed bundle lookup does not trigger a standalone lookup. Version 1's two known standalone paths are the only bounded path fallback.

## API Contract

### Request

```http
GET /api/cloudnative/dump_tablet_metadata/{TabletId}?version={Version}&is_bundle={true|false}
```

Parameter rules:

| Parameter | Contract |
|---|---|
| `TabletId` | Required positive decimal `int64`. Signs, whitespace, hexadecimal notation, zero, negatives, and overflow are rejected. |
| `version` | Required positive decimal `int64` with the same strict syntax. |
| `is_bundle` | Required. Only lowercase `true` and `false` are accepted. |
| Other query parameters | Rejected. This catches misspellings instead of silently changing behavior. |
| Duplicate parameters | Rejected before the query is flattened into a map. |

`HttpRequest` must retain query entries with their multiplicity and keep route captures in a namespace separate from query parameters. The action validates those sources directly instead of using today's merged parameter map. This scoped request-parsing change is part of the design; a query parameter named `TabletId` must never shadow the URI path segment.

For `version == 1`, `is_bundle` must be `false`; `true` returns `INVALID_ARGUMENT`. The zero-ID initial metadata filename is a shared standalone object, not bundle encoding.

### Success response

```json
{
  "metadata": {
    "id": 11979,
    "version": 2
  },
  "redacted_fields": [
    "starrocks.SegmentMetadataPB.encryption_meta"
  ]
}
```

The response contains:

- one logical `TabletMetadataPB` JSON object under `metadata`;
- `redacted_fields` only when the fixed security policy removed values.

The response does not contain `is_bundle`, `num`, pagination state, object paths, or an array wrapper. Metadata already carries its logical tablet ID and version.

All cryptographic material, including every current and deprecated field whose protobuf name contains `encryption_meta`, is always removed before JSON conversion, regardless of that field's protobuf type. There is no request option to disable redaction. The fixed redaction policy is versioned with the server, and schema-change tests must fail when a new encryption-bearing metadata field is added without a policy decision.

Other metadata can contain user schema, default values, delete-predicate values, and sort-key bounds. The complete response therefore remains sensitive diagnostic data even after cryptographic redaction.

Response headers:

```http
Content-Type: application/json
Cache-Control: no-store
X-Content-Type-Options: nosniff
```

The response is always compact JSON. Human callers can pipe it through `jq` when they need formatting.

The first implementation preserves the existing json2pb representation and emits protobuf 64-bit integers as JSON numbers. Callers must use a lossless JSON-number implementation; JavaScript callers must not coerce these values through IEEE-754 `Number` when they can exceed `2^53 - 1`.

## Exact Read Algorithm

### Standalone metadata, version 2 or later

1. Construct only `tablet_metadata_location(tablet_id, version)`.
2. Open a random-access file with metadata, data, and disk cache bypass enabled.
3. Read the file size and reject it before allocation if it exceeds the raw metadata limit.
4. Read exactly the bounded object.
5. Parse the checksummed header format or the legacy headerless protobuf format.
6. Validate checksum and require the bytes following a Lake header to equal its declared protobuf length exactly; both truncation and trailing bytes are corruption.
7. Parse `TabletMetadataPB` and apply Lake after-load normalization.
8. Require `metadata.id == tablet_id` and `metadata.version == version`.
9. Apply fixed cryptographic redaction.
10. Serialize through a capped compact-JSON sink.

### Bundled metadata, version 2 or later

1. Construct only `bundle_tablet_metadata_location(tablet_id, version)`.
2. Open a random-access file with local cache reads and fills disabled.
3. Read the total file size without reading the file contents.
4. Range-read the trailing 8-byte size field.
5. Determine the legacy or checksummed footer layout and validate all size arithmetic.
6. Reject an oversized footer before allocation.
7. Range-read and validate the footer and suffix.
8. Parse `BundleTabletMetadataPB` and find `tablet_meta_pages[tablet_id]`.
9. Require the target page to lie completely before the footer; reject overflow and overlap.
10. Reject an oversized page before allocation.
11. Range-read only the target page and verify its checksum when present.
12. Parse `TabletMetadataPB`; require its ID and version to match the request.
13. Run `normalize_tablet_metadata_after_load` while the page is still in its persisted bundle shape.
14. Restore the current schema and every schema referenced by `rowset_to_schema`.
15. Run `force_cloud_native_pk_persistent_index` after restoring the current schema.
16. Apply fixed cryptographic redaction and capped JSON serialization.

The existing range-read implementation in [lake_replication_txn_manager.cpp](../../be/src/storage/lake/lake_replication_txn_manager.cpp) is the source pattern. It should be extracted into a reusable storage-layer reader rather than copied into the HTTP action.

### Initial metadata, version 1

Version 1 has a bounded special case because both known physical forms use standalone protobuf encoding:

1. Try `{tablet_id}_1.meta` with the same bounded standalone parser and normalizer used for later standalone versions.
2. Require the per-tablet object's original ID to equal the requested tablet ID and its version to equal 1.
3. On not found, try `0_1.meta` with the same bounded standalone parser and normalizer.
4. Require the shared object's parsed version to equal 1, then copy it and set the logical ID to the requested tablet ID. Its original ID is the physical owner selected during optimized creation and is not compared with the requested logical ID.
5. If both durable paths are absent, return not found.
6. Do not call `construct_initial_metadata` or another FE fallback.

No directory listing is needed for either path.

## Resource Safety

The first implementation uses fixed server-owned policies. They are not request parameters and cannot be raised dynamically by an agent.

| Resource | Server policy |
|---|---:|
| Standalone object or target bundle page | 16 MiB |
| Bundle footer | 16 MiB |
| Compact JSON response | 64 MiB |
| Request-scoped tracked memory | 256 MiB |
| Concurrent accepted requests per CN | 1 |
| Waiting work items | 0 |
| Retries added by this endpoint | 0 |
| Directory LIST calls | 0 |

The reader rejects an oversized file, footer, or page before allocating its buffer. Headered standalone metadata must consume exactly the declared protobuf length. Checked arithmetic is required before every range read. The existing protobuf parser recursion limit remains in force, and the fixed raw/footer/page limits also bound the encoded number of bundle entries and repeated values accepted from storage.

Parsing, normalization, redaction, and JSON conversion execute under a 256 MiB request `MemTracker`. The raw protobuf and compact JSON each have their own byte ceiling. JSON is written through a capped protobuf `ZeroCopyOutputStream`, and overflow is checked independently of the converter's boolean result because the bundled json2pb writer can report success after its sink refuses another buffer. The cap covers the complete response envelope, not just the nested metadata object. Crossing a limit fails the complete request. The API never truncates metadata and returns HTTP 200.

The HTTP action acquires one process-local permit before starting storage work. There is no queue: a second request receives `DIAGNOSTIC_BUSY` without opening an object. Permit ownership is attached to the `HttpRequest` handler context and is released only when libevent frees the request after response completion or disconnect. This includes the bounded connection output buffer in the admitted lifetime instead of releasing the permit as soon as the handler returns.

`64 MiB` is a response-payload limit, not an exact RSS formula. During `HttpChannel::send_reply`, the response string and libevent output chains can overlap, and allocator overhead and parsed protobuf objects also consume memory. Concurrency one plus the response cap bounds endpoint amplification, while the request tracker bounds the synchronous parse/serialize pipeline.

### End-to-end admission lease

The single concurrency permit covers the complete resource lifetime:

```text
admission
  -> object-store read
  -> protobuf parse and normalization
  -> redaction and JSON generation
  -> response drained or connection closed
```

Releasing the permit when the handler returns is insufficient because libevent may still own the complete response in its output buffer. The request handler context releases the permit on request free, after a successful drain or connection close.

This means a slow or stuck client can make this diagnostic endpoint unavailable until the connection is released. The response remains capped and no second diagnostic operation is admitted. The API does not add a new transport timer or make an absolute drain-time guarantee.

## Execution and Transport

The first implementation keeps the existing synchronous action model. The action performs exact storage I/O, parsing, normalization, redaction, and compact JSON conversion in the libevent worker callback. It sends one complete, bounded response through `HttpChannel`; `HttpStreamChannel` is not used. All fallible application work completes before success headers are sent.

This endpoint does not create a thread pool whose shutdown could be held by an uncancellable filesystem call, and it does not add asynchronous `HttpRequest` ownership. It also does not claim a wall-clock storage deadline: provider-level retry, timeout, and cancellation behavior remain the storage provider's responsibility.

A slow object-store read can occupy one HTTP worker, and a slow client can retain the sole diagnostic permit. Other HTTP endpoints are not guaranteed to remain responsive when the server is configured with too few workers. That limitation is explicit; the safety properties here are fixed work cardinality, bounded object and response sizes, and no concurrent amplification—not latency isolation.

## Authentication and Data Handling

The endpoint requires authenticated `OPERATE` privilege regardless of the global `enable_http_auth` setting. Introduce an always-authenticated handler policy for this endpoint; authentication-service failure is fail-closed. The current FE `checkAuth` protocol reports both bad credentials and insufficient privilege as `NOT_AUTHORIZED`, so this design does not promise to distinguish those two cases at the HTTP layer.

Because BE HTTP does not itself provide TLS, the endpoint must be reached through a TLS-terminating management proxy or a trusted management network. It is not a tenant-level data API.

The implementation must never log:

- authorization headers or passwords;
- metadata response bodies;
- encryption material;
- bucket names or physical object paths;
- raw object-store errors that may contain credentials or endpoints.

Metadata strings are untrusted data. Agent integrations must keep them as structured tool output and must not interpolate them into shell commands or higher-privilege instructions.

## Error Contract

After authentication dispatches to the action, every application error uses a stable JSON envelope:

```json
{
  "code": "METADATA_NOT_FOUND",
  "message": "tablet metadata is unavailable"
}
```

Authentication and authorization fail before the action runs and retain the shared BE HTTP authentication response format. This API does not fork that global protocol merely to make its envelope match application errors.

| HTTP status | Code | Meaning |
|---:|---|---|
| 400 | `INVALID_ARGUMENT` | A parameter is missing, malformed, out of range, duplicated, or unsupported. |
| 404 | `METADATA_NOT_FOUND` | The exact durable metadata is absent, was vacuumed, or the caller supplied the wrong physical format. |
| 413 | `METADATA_TOO_LARGE` | The footer, page, protobuf heap, or JSON exceeds a hard ceiling. |
| 502 | `STORAGE_READ_FAILED` | The object store failed without exposing its internal error to the client. |
| 503 | `DIAGNOSTIC_BUSY` | The end-to-end admission permit is occupied. |
| 500 | `CORRUPT_METADATA` | File structure, checksum, protobuf, page identity, version, or schema restoration is invalid. |
| 500 | `SERIALIZATION_FAILED` | Valid logical metadata could not be converted to bounded JSON. |

Authentication completes before any storage access. All application-level fallible work completes before a success response begins, so the endpoint never embeds an error object in an HTTP 200 response. A disconnect after response start is an ordinary transport failure; it cannot be replaced with another HTTP status.

## Observability and Audit

The first implementation emits one sanitized completion log containing:

- tablet ID and version;
- stable result code;
- elapsed time;
- busy state.

It adds no per-tablet metric labels and no response-body logging. Endpoint-specific counters can be added later if operational use demonstrates a need; they are not required for the exact-read safety boundary.

Logging must not use an HTTP debug dump that includes headers.

## Component Boundaries

### HTTP request parsing support

Responsibilities:

- retain raw query multiplicity so duplicate parameters can be rejected;
- expose route captures separately from query parameters;
- preserve the existing merged parameter view for handlers that still depend on it.

This is a small shared HTTP-layer change, not an expansion of the diagnostic API's capability.

### HTTP action

Responsibilities:

- enforce mandatory authentication and `OPERATE`;
- parse the strict request contract;
- acquire the end-to-end admission lease;
- run bounded diagnostic work synchronously;
- map storage-layer statuses to stable HTTP errors;
- set response headers and attach permit cleanup to request lifetime.

It does not construct object paths, parse bundle files, mutate caches, or inspect protobuf internals.

### Exact durable metadata reader

Responsibilities:

- construct deterministic metadata paths through `LocationProvider`;
- implement standalone, bundle, and version 1 read branches;
- enforce raw/footer/page/parser limits before allocation;
- bypass caches;
- verify checksums, identity, version, and schema references;
- return one normalized logical `TabletMetadataPB`.

This belongs in the Lake storage layer and is reusable by other exact-read callers. Extract the existing replication range reader into this component to avoid divergent bundle parsers.

### Safe metadata serializer

Responsibilities:

- apply the fixed cryptographic redaction policy;
- report sorted protobuf field-descriptor full names for fields whose populated values were removed;
- serialize compact protobuf JSON through a capped sink and enforce the complete response byte limit before sending;
- enforce request memory and JSON byte limits.

It has no storage or HTTP dependencies.

### Diagnostic admission controller

Responsibilities:

- enforce fixed concurrency and no-queue policy;
- keep the permit through response completion;
- release the permit exactly once on request free.

## Correctness Argument

### Completeness

Every supported durable layout has one explicit branch:

- standalone version 2 or later uses the tablet-specific path;
- bundled version 2 or later uses the zero-ID bundle path and one indexed page;
- version 1 tries the two defined standalone paths;
- absent lightweight-created version 1 metadata returns not found.

No supported layout requires directory discovery.

### Soundness

A successful response represents only the requested logical tablet and version:

- the client supplies the version-specific physical format;
- the reader constructs only the corresponding deterministic path;
- standalone and bundle pages validate ID and version;
- the shared initial object has a documented logical-ID remapping branch;
- schema restoration must satisfy every referenced schema ID;
- checksum and parse failures never produce a success response.

### Boundedness

Directory cardinality is absent from the algorithm. A standalone request performs one bounded object read. A bundle request performs a constant number of bounded range reads. Version 1 tries at most two deterministic standalone objects. Server-side policies bound allocated raw data, protobuf expansion, JSON output, admitted work, and queued response payload. A blocked filesystem call can occupy one HTTP worker and the sole diagnostic permit, but it cannot cause additional diagnostic work to accumulate.

### Side effects

The endpoint is read-only. It does not list, write, delete, synthesize, populate caches, or modify FE state. Diagnostic corruption does not trigger local cache eviction because the read bypasses local caches.

## Test Plan

### Contract tests

- Accept valid positive decimal tablet IDs and versions.
- Reject missing values, whitespace, signs, zero, negatives, hexadecimal input, overflow, duplicates, and unknown parameters before storage access.
- Reject `version=1&is_bundle=true`.
- Prove a query parameter cannot shadow `TabletId` from the URI.
- Verify missing credentials, invalid credentials, missing `OPERATE`, and an unavailable auth service never access storage; authorization denials use 401 and auth-service failures use 503.

### Storage-layout tests

- Read standalone checksummed metadata.
- Read legacy headerless standalone metadata.
- Reject a valid headered protobuf followed by trailing bytes.
- Read a checksummed bundle by footer and page ranges.
- Read a legacy bundle footer.
- Restore current and historical schemas from a bundle.
- Read per-tablet and shared initial metadata.
- Return not found for lightweight initial creation without invoking FE.
- Test historical format transitions by independently selecting standalone and bundle for different versions.

### Exactness tests

- Populate the same metadata directory with many tablets and versions and return only the requested object.
- Use a fake filesystem whose directory-list method fails the test if called.
- Assert bundle reads never call `read_all()` and transfer less than the complete large bundle.
- Assert a wrong `is_bundle` value does not probe the other path.
- Assert existing metacache content cannot turn a missing durable object into success.

### Boundary and corruption tests

- Exercise every byte ceiling at `limit - 1`, `limit`, and `limit + 1`.
- Reject oversized declared lengths before allocation.
- Reject truncated headers, zero-length legacy protobufs, invalid footer sizes, arithmetic overflow, pages overlapping the footer, missing page pointers, bad checksums, malformed protobufs, missing schemas, and ID/version mismatches.
- Generate high protobuf-expansion and JSON-escaping ratios and prove request memory and JSON remain bounded.
- Verify no partial HTTP 200 is emitted on any failure.

### Admission and transport tests

- Hold one request context open and verify a later diagnostic request fails immediately without storage access.
- Verify the admission lease remains held after the handler returns and is released only when the request is freed after output completion or disconnect.
- Disconnect during response write and verify no crash, use-after-free, body logging, or leaked permit.
- Verify the complete response goes through `HttpChannel`, never `HttpStreamChannel`.

### Security and observability tests

- Populate every current and deprecated encryption metadata field and verify none appears in response bodies, logs, traces, or diagnostic records.
- Put fake bucket names, credentials, and paths in storage errors and verify the client sees only the stable error envelope.
- Verify diagnostic records include tablet/version, sizes, latency, and outcome without headers or body content.
- Verify metrics do not use tablet, version, path, or principal labels.

### Performance invariant

Run the same exact request with 1, 10,000, and 1,000,000 unrelated directory objects. The number of storage operations, CPU work, and peak memory for the endpoint must not grow with directory cardinality. A bundle's total size may grow, but the endpoint transfers only the bounded footer and target page.

## Acceptance Criteria

The implementation is accepted when:

- each success returns exactly one normalized logical metadata object;
- every request requires explicit tablet ID, version, and version-specific `is_bundle`;
- directory LIST count is always zero;
- bundle `read_all()` count is always zero;
- version 1 never uses a bundle parser or FE synthesis;
- neither metadata nor local object caches are read or filled;
- raw, footer, protobuf heap, JSON, and concurrency policies are enforced without admitting replacement work behind an active request;
- a slow client cannot increase total queued output beyond one bounded response;
- all application errors before response start use complete, stable JSON;
- cryptographic material never leaves the endpoint;
- the synchronous worker limitation is documented rather than hidden behind an unenforceable latency guarantee.
