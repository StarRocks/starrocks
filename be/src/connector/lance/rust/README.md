# Native Lance reader

This crate uses Lance SDK 8.0.1 and exports a private C ABI declared in
`../lance_rs_ffi.h`. It exchanges record batches using the Arrow C Data Interface;
C++ imports them and converts the projected columns to StarRocks columns.
The reader has no JVM dependency.

## Build and test

```sh
cargo test --locked --manifest-path be/src/connector/lance/rust/Cargo.toml
cargo clippy --locked --manifest-path be/src/connector/lance/rust/Cargo.toml --tests -- -D warnings
./run-be-ut.sh --build-target connector_lance_test --module connector_lance_test --without-java-ext
```

BE CMake builds the shared library with `build-support/build_lance.py`. It uses
Rust 1.96.1 or newer, or installs a checksum-verified toolchain within the BE build
directory on Linux x86-64/AArch64. `Cargo.lock` pins dependencies. Configure
`WITH_CONNECTOR_LANCE=OFF` to omit the crate and its toolchain entirely.
`LANCE_BUILD_JOBS` bounds Cargo parallelism independently of the C++ build.

## Ownership and query semantics

- C strings and property arrays are borrowed only during `sr_lance_reader_open`.
- The caller owns a successful reader handle; calls on one handle cannot overlap.
- Each successful `sr_lance_reader_next` transfers Arrow release callbacks to C++.
  Imported buffers can outlive the reader. EOF, pending and errors transfer no buffers.
- The cancellation callback is borrowed during open and called on the calling
  thread. Next polls return within 100 ms while I/O is pending so C++ can check
  query cancellation. Open polls the same cancellation callback every 100 ms
  while pending and respects the BE query timeout through that callback.
- Rust unwinds are caught at fallible FFI boundaries. SDK error strings are not
  returned because they may contain credentials or signed URLs.
- Sessions and credential-bearing object stores are private to each reader.
  Only the bounded Tokio runtime is shared. Readahead is limited to one batch and
  one fragment. Rust uses a private allocator to avoid BE thread-local allocator
  accounting across Tokio workers. Exported Arrow buffers are charged to the BE
  instance memory tracker until released. SDK-internal caches and temporary I/O
  allocations remain outside the query tracker; this is not a hard memory limit.
- The dataset version is resolved once at open (or selected through the ABI's
  explicit version argument) and retained for the entire stream. The current FE
  sends one full-dataset range and does not pin a version across separate scan
  operators. All fragments are read. Fragment scheduling and ANN are future work.
- Projection is pushed down. Predicates and LIMIT remain in StarRocks; pushing a
  limit ahead of residual filtering would change query results.

Rust tests create real multi-fragment datasets and exercise the public ABI,
snapshot stability, projection, nulls, buffer ownership, cancellation and errors.
For BE test builds the build helper also generates a local fixture used by
`connector_lance_test` to exercise Rust through C++ and StarRocks column conversion.

## Reference implementation and attribution

The native reader framework and naming follow the
[`lance-cp-sr-lance-dev` branch](https://github.com/tianfy17/starrocks/tree/lance-cp-sr-lance-dev)
submitted by @tianfy17 in [StarRocks #77326](https://github.com/StarRocks/starrocks/pull/77326),
as discussed in [#79428](https://github.com/StarRocks/starrocks/issues/79428).
The reference was inspected at `2bf405c23e5ec717f93c7b5cc961b3f4083fe636`.
Credit goes to lixianhai for the original native reader implementation
([01616406f109](https://github.com/StarRocks/starrocks/commit/01616406f109029d5a1822f36ca1f7b848f363d1),
cherry-picked there from `2af107159dd`) and to @tianfy17 for the contributed
branch and integration work.

The matching concepts use `lance_rs_ffi.h`, `SrLanceReader`, `SrLanceString`,
`SrLanceStringPair`, `sr_lance_reader_open/next/close`, `open_reader`,
`string_from_raw`, `strings_from_raw`, `storage_options_from_raw`, and
`export_batch`. The C++ adapter follows the same reader stages: `_init_read_fields`,
`_open_reader`, `_next_batch`, `_append_batch_to_read_chunk`, and
`_batch_is_exhausted`, with `_reader`, `_field_names`, `_arrow_batch`, and
`_batch_start_idx` holding the corresponding state.

This remains a private ABI, not a drop-in binary replacement for that branch.
The open arguments retain version selection, StarRocks cloud properties and
cancellation; next also returns a pending status. The adapter stays inside
ConnectorLance rather than depending on HdfsScanner/ParquetScanner. It converts
each batch slice with strict query error handling and explicit memory accounting.
Shared credential-bearing sessions, ANN, fragment scheduling and limit pushdown
are not part of this series.
