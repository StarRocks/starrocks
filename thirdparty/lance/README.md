# Standalone Lance Rust SDK shared library

This crate uses Lance SDK 12.0.0 with Arrow 58.3.0 and exports a private C ABI declared in
`lance_rs_ffi.h`. It exchanges record batches using the Arrow C Data Interface;
C++ imports them and converts the projected columns to StarRocks columns.
The reader has no JVM dependency.

Following maintainer feedback, this integration uses the Lance Rust SDK through
a C FFI. The standalone build produces `libstarrocks_lance.so` without compiling
StarRocks FE or BE. The exported C ABI is specific to this wrapper; it takes
interface design guidance from lance-c without depending on that library.

Object storage reads currently go through the Lance SDK. This package does not
integrate StarRocks DataCache or expose a file-read callback for it. That requires
separate I/O adaptation when the connector interface is agreed.

## Build and install the optional thirdparty package

Build this dependency **before** configuring the BE. The normal StarRocks build
does not invoke Cargo or install Rust. Like paimon-cpp, the dependency has a
`build-thirdparty.sh` target and its own installation prefix. Lance remains an
explicit target locally; the Ubuntu and Rocky9 dependency images build it in a
separate stage. The existing dependencies are reused from a pinned, verified
dev-env image when their build inputs match; otherwise they are rebuilt. The
Lance stage then reuses the installed protoc and builds only Lance with
`./thirdparty/build-thirdparty.sh -j2 lance`. It does not download or rebuild
zlib, protobuf or googletest separately.

Install Rust >= 1.96.1, a native C/C++ toolchain, and protobuf/protoc first:

```sh
./thirdparty/build-lance.sh -j 2
# Default Linux output:
ls thirdparty/installed/lance/lib/libstarrocks_lance.so
# Custom thirdparty installation or prepopulated Cargo cache:
./thirdparty/build-lance.sh --prefix /opt/starrocks-thirdparty/lance \
  --protoc /opt/starrocks-thirdparty/bin/protoc --offline
# Standard Linux thirdparty entrypoint, after protobuf is installed:
./thirdparty/build-thirdparty.sh -j2 lance
```

The default installation is `thirdparty/installed/lance` (or
`$TP_INSTALL_DIR/lance`). Build intermediates stay under
`thirdparty/src/lance-build`; `--target-dir` can select a reusable cache.
`Cargo.lock` pins dependencies, and the builder neither downloads a toolchain
nor changes rustup settings. It runs all native tests and generates the BE
integration fixture before replacing a previous installation. Frame pointers
remain enabled for Rust and native dependencies.

`docker/dockerfiles/dev-env/dev-env.Dockerfile` copies the preinstalled Rust/Cargo
1.96.1 toolchain from the official `rust:1.96.1-slim-bookworm` image, pinned by
its multi-platform digest. Image builds do not run a Rust installer or download
a toolchain from `static.rust-lang.org`. Only the Rust distribution is copied;
Debian system libraries are not copied. The `lance-builder` stage
still compiles in the selected StarRocks Ubuntu or Rocky9 toolchain environment,
so the resulting library uses that platform's system ABI. It builds Lance
with two Cargo jobs to limit peak memory, then copies the installed package
into the final image; the Rust toolchain and Cargo cache are not copied. The
legacy CentOS7 image skips Lance because the official Rust toolchain requires
glibc 2.28 or newer. macOS developers use `build-lance.sh` directly with a native
Rust toolchain.

To validate just the Lance producer in the same toolchain image as CI, run from
the repository root (requires Docker with BuildKit):

```sh
DOCKER_BUILDKIT=1 docker build --target lance-builder \
  --build-arg distro=ubuntu \
  -f docker/dockerfiles/dev-env/dev-env.Dockerfile -t starrocks-lance-build:ubuntu .
# Repeat with distro=rocky9 and a corresponding image tag.
```

This target first obtains the validated dependency installation, then runs the
native tests and installs the package at `/var/local/thirdparty/installed/lance`.
With matching prebuilt inputs, it reuses protoc without downloading C++ source
archives or rebuilding existing libraries. A full build or invalidated baseline
builds the existing dependencies first. The final `dev-env` target requires both
the dependency installation and Lance build to succeed; this does not skip the
existing thirdparty image checks. See [dependency reuse](../README.md#reusing-dependencies-in-dev-env-images)
for the input validation and `thirdparty_source=full` override.

The installed package contains:

- `lib/libstarrocks_lance.so` (`.dylib` on macOS).
- `include/lance_rs_ffi.h`, matching the library's C ABI.
- `lib/cmake/StarRocksLance/`, a relocatable imported CMake target.
- `share/lance/testdata/rows.lance`, the BE integration test fixture.
- `share/lance/manifest.json`, source/toolchain metadata and library/header hashes.

A dependency image can carry this installation. Consumers do not need Rust,
Cargo, the Rust source tree, or network access:

```cmake
find_package(StarRocksLance 0.1.0 EXACT REQUIRED CONFIG
             PATHS "${THIRDPARTY_DIR}/lance" NO_DEFAULT_PATH)
target_link_libraries(my_reader PRIVATE lance_rust)
```

The imported package rejects a different OS/architecture or damaged library and
header files. It preserves Linux `$ORIGIN` / macOS `@loader_path` lookup after
packaging. Its version is the **StarRocks wrapper version**, not the Lance SDK
version; update it and its consumers when changing the delivered interface.
Checksums detect accidental mixed installations; they are not signatures for an
untrusted distribution channel.

Build natively in the target platform's thirdparty environment. Release producers
must use the same minimum glibc/macOS and CPU baseline as their StarRocks build;
architecture matching alone does not ensure runtime compatibility. Do not use
`target-cpu=native` for redistributable packages. Cross-compilation is rejected.
Linux x86-64/ARM64 and macOS x86-64/ARM64 are recognized; native release tests are
required for each supported platform.

Run source checks separately when modifying the wrapper:

```sh
cargo fmt --manifest-path thirdparty/lance/Cargo.toml --check
cargo clippy --locked --manifest-path thirdparty/lance/Cargo.toml --tests -- -D warnings
python3 -m unittest discover -s thirdparty/lance -p 'test_*.py'
```

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
The thirdparty builder also generates a local fixture used by
`connector_lance_test` to exercise Rust through C++ and StarRocks column conversion.

## Reference implementation and attribution

The FFI contract also references
[`lance-format/lance-c`](https://github.com/lance-format/lance-c/blob/2089f6ffc7a138f52ac90c0592fdd3e85727d57d/include/lance/lance.h):
opaque reader handles, Arrow C Data Interface outputs with caller-owned outer
structures and release callbacks, and explicit close/free functions. A reader
must be closed exactly once; exported batches can outlive it. Callbacks must
return normally without throwing across the C ABI.

This wrapper retains length-delimited inputs, per-call sanitized errors and
bounded polling for StarRocks cancellation instead of adopting lance-c's full
dataset/scanner API or thread-local error channel. Function names, return codes
and signatures are not ABI-compatible with lance-c. DataCache callbacks remain
a separate integration task.

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
