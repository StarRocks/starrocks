# AGENTS.md - StarRocks Backend

Backend operating contract for agentic work in `be/`.

## Read This First

- Repo-wide topology and active engineering plans live in [`handbook/index.md`](../handbook/index.md).
- The backend domain map lives in [`handbook/domains/backend.md`](../handbook/domains/backend.md).
- The BE harness overview lives in [`handbook/architecture/be-boundary-harness.md`](../handbook/architecture/be-boundary-harness.md).
- The BE module boundary source of truth is [`be/module_boundary_manifest.json`](./module_boundary_manifest.json).
- The harness is enforced by [`build-support/check_be_module_boundaries.py`](../build-support/check_be_module_boundaries.py).
- The generated module section in this file is enforced by [`build-support/render_be_agents.py`](../build-support/render_be_agents.py).
- Reviewed legacy violations are frozen in [`build-support/be_module_boundary_baseline.json`](../build-support/be_module_boundary_baseline.json). Only the deferred allocator-related entries should remain, and the baseline must only shrink.

## Harness Commands

```bash
# Full BE architecture check
python3 build-support/check_be_module_boundaries.py --mode full

# PR-style changed-files check
python3 build-support/check_be_module_boundaries.py --mode changed --base origin/main

# PR-style check plus baseline shrink-only guard
python3 build-support/check_be_module_boundaries.py --mode changed --base origin/main --enforce-baseline-shrink

# Verify this file matches the manifest
python3 build-support/render_be_agents.py --check

# Rewrite the generated module section after manifest edits
python3 build-support/render_be_agents.py --write
```

## Fast Loops

```bash
# Standard BE build
./build.sh --be

# Debug or ASAN build
BUILD_TYPE=Debug ./build.sh --be
BUILD_TYPE=ASAN ./build.sh --be

# Fast single-binary UT loop
./run-be-ut.sh --build-target <test_binary> --module <test_binary> --without-java-ext
```

Useful core binaries for fast iterations:

- `base_test`
- `io_test`
- `fs_core_test`
- `types_test`
- `column_test`
- `runtime_core_test`
- `expr_core_test`

## Core C++ Rules

- Format with the repo `.clang-format`.
- Prefer `#pragma once`.
- Keep include order: corresponding header, C system, C++ stdlib, third-party, StarRocks.
- Use `Status` and `StatusOr` for recoverable errors.
- Treat hot-path allocations and row-by-row virtual dispatch as performance smells.

## Architecture Workflow

- If you change BE layering rules, edit `be/module_boundary_manifest.json` first.
- After manifest changes, run `python3 build-support/render_be_agents.py --write`.
- Before finishing BE work, run `python3 build-support/check_be_module_boundaries.py --mode full`.
- If the check fails on a pre-existing edge, confirm it is already listed in `build-support/be_module_boundary_baseline.json`.
- If the check fails on a new edge, fix the code or update the manifest. Do not add to the baseline unless the debt is deliberate and reviewed.
- If you touch the baseline file, run the changed-files check with `--enforce-baseline-shrink`; fixed entries should be deleted, never rewritten into new debt.

<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### Base (`base`)
Lowest-level BE primitives. Keep it free of higher-level StarRocks module dependencies.
- Targets: `Base`
- Allowed internal include prefixes: `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Gutil`
- Core tests: `base_test`
- Remediation: Move shared helpers into Base/Gutil or introduce a lower-level interface instead of pulling in higher-level BE modules.

### Gutil (`gutil`)
Standalone utility substrate. Do not couple it to BE modules.
- Targets: `Gutil`
- Allowed internal include prefixes: `gutil/`
- Allowed target deps: `Common`, `Base`, `Gutil`
- Remediation: Keep Gutil independent; move BE-specific logic out instead of importing BE modules.

### Common (`common`)
Core shared infrastructure above Base/Gutil and generated code only. Higher-level BE modules must not leak back into it.
- Targets: `Common`
- Allowed internal include prefixes: `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Base`, `Gutil`, `StarRocksGen`
- Core tests: `common_test`
- Remediation: Move the dependency upward or add a lower-level abstraction; Common may only depend on Base, Gutil, and generated code.

### Cache (`cache`)
Cache implementation module for DataCache facade, cache engines, monitors, metrics, utilities, StarCache integration, and peer-cache RPC reads without service/bootstrap or ExecEnv singleton coupling.
- Targets: `Cache`
- Allowed internal include prefixes: `cache/`, `common/brpc/`, `runtime/current_thread.h`, `runtime/mem_tracker.h`, `fs/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `RuntimeCore`, `FSCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `cache_test`
- Remediation: Keep Cache self-contained within cache engines, monitors, metrics, utilities, and injected peer-cache BRPC stubs; keep service startup, storage code, HTTP/admin code, and ExecEnv singleton access outside cache.

### HttpCore (`httpcore`)
Reusable HTTP transport and request primitives above Common without BE admin or page-handler code.
- Targets: `HttpCore`
- Allowed internal include prefixes: `http/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`
- Core tests: `http_core_test`
- Remediation: Keep HttpCore limited to reusable HTTP transport and request primitives; move BE-specific pages, actions, auth helpers, and client/download helpers upward.

### IOCore (`iocore`)
Minimal IO foundation used by upper IO/FS layers.
- Targets: `IOCore`
- Allowed internal include prefixes: `io/core/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`
- Core tests: `io_test`
- Remediation: Keep IOCore free of higher IO/FS/runtime/storage code; lift the dependency into IO or FileSystem instead.

### FSCore (`fscore`)
Minimal filesystem core on top of IOCore.
- Targets: `FSCore`
- Allowed internal include prefixes: `fs/`, `io/core/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `IOCore`, `Common`, `Base`, `Gutil`
- Core tests: `fs_core_test`
- Remediation: Keep FSCore limited to IOCore plus core FS abstractions; move backend-specific behavior into FileSystem.

### SpillCore (`spillcore`)
Core spill block, directory, query-local spill, and spill memory-resource primitives without pipeline/runtime-service coupling.
- Targets: `SpillCore`
- Allowed internal include prefixes: `exec/spill/`, `fs/`, `io/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `RuntimeCore`, `FSCore`, `IOCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `spill_core_test`
- Remediation: Keep SpillCore limited to reusable spill block, directory, query-local spill, and spill memory-resource infrastructure; move pipeline ownership and runtime-service integration upward.

### TypesCore (`typecore`)
Core type system without runtime/storage/exec coupling.
- Targets: `TypesCore`
- Allowed internal include prefixes: `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`
- Core tests: `types_test`
- Remediation: Keep TypesCore independent of runtime/util/storage/exec layers; move integration code into higher layers.

### ColumnCore (`columncore`)
Core column representations that must stay independent of ChunkCore and higher layers.
- Targets: `ColumnCore`
- Allowed internal include prefixes: `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `column_test`
- Remediation: Keep ColumnCore free of ChunkCore/Runtime/Exec/Storage coupling; move integration code upward or introduce an interface.

### ChunkCore (`chunkcore`)
Core chunk, schema, field, and chunk-adjacent helpers on top of ColumnCore.
- Targets: `ChunkCore`
- Allowed internal include prefixes: `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ColumnCore`, `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `chunk_test`
- Remediation: Keep ChunkCore limited to chunk/schema helpers and free of Runtime/Exec/Storage/Serde coupling.

### RuntimeCore (`runtimecore`)
Core runtime building blocks without full Runtime/Exec/Storage coupling.
- Targets: `RuntimeCore`
- Allowed internal include prefixes: `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ChunkCore`, `ColumnCore`, `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `runtime_core_test`
- Remediation: Keep RuntimeCore restricted to core runtime infrastructure; move service/storage/stream-load/integration code into Runtime.

### ExprCore (`exprcore`)
Core expression infrastructure that depends only on RuntimeCore and lower layers.
- Targets: `ExprCore`
- Allowed internal include prefixes: `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `RuntimeCore`, `ChunkCore`, `ColumnCore`, `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `expr_core_test`
- Remediation: Keep ExprCore limited to core expression infrastructure; move aggregate/UDF/integration code into Exprs.

### ExecCore (`execcore`)
Execution-node base and runtime-filter infrastructure on top of ExprCore without broader Exec/Runtime service coupling.
- Targets: `ExecCore`
- Allowed internal include prefixes: `exec/runtime_filter/`, `exec/pipeline/pipeline_fwd.h`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `exec_core_test`
- Remediation: Keep ExecCore limited to the legacy execution-node base and runtime-filter orchestration; only the pipeline forward header is allowed from broader pipeline code.

### PipelinePrimitives (`pipelineprimitives`)
Stable pipeline operator primitives without pipeline runtime, scheduler, context, factory, or concrete operator coupling.
- Targets: `PipelinePrimitives`
- Allowed internal include prefixes: `exec/pipeline/operator.h`, `exec/pipeline/primitives/`, `exec/pipeline/runtime_filter_core_types.h`, `exec/runtime_filter/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExecCore`, `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep pipeline primitives limited to base operator contracts and narrow helper types; move runtime, scheduler, factory, and concrete operator behavior into higher pipeline modules.

### ExecSinkCore (`execsinkcore`)
DataSink base contract and default mechanics without concrete sink, pipeline, connector, storage, service, or Runtime coupling.
- Targets: `ExecSinkCore`
- Allowed internal include prefixes: `exec/data_sink.h`, `exec/pipeline/pipeline_fwd.h`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `data_sink_core_test`
- Remediation: Keep ExecSinkCore limited to the DataSink base contract; move construction, concrete sinks, pipeline decomposition, and future sink IO behavior into higher modules.

### ExecSchemaScannerCore (`execschemascannercore`)
Schema scanner base contract and shared mechanics without concrete scanner, pipeline, storage, service, or ExecEnv coupling.
- Targets: `ExecSchemaScannerCore`
- Allowed internal include prefixes: `exec/schema_scanner.h`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `schema_scanner_core_test`
- Remediation: Keep SchemaScannerCore limited to the base SchemaScanner contract; move concrete scanner creation and service-specific logic into higher schema scanner modules.

### ExecSchemaScanners (`execschemascanners`)
Clean concrete schema scanners that do not depend on SchemaHelper, FE RPC/client helpers, storage, service, cache, pipeline, or ExecEnv.
- Targets: `ExecSchemaScanners`
- Allowed internal include prefixes: `exec/schema_scanner.h`, `exec/schema_scanner/schema_column_filler.h`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExecSchemaScannerCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `exec_schema_scanners_test`
- Remediation: Keep this first schema scanner target limited to clean local/static scanners; leave SchemaHelper, storage, HTTP, cache, service, and ExecEnv users in higher compatibility modules until they get explicit boundaries.

### ExecSortingCore (`execsortingcore`)
Reusable exec sorting algorithms without pipeline, spill, workgroup, storage, or service coupling.
- Targets: `ExecSortingCore`
- Allowed internal include prefixes: `exec/sorting/`, `exprs/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `exec_sorting_core_test`
- Remediation: Keep ExecSortingCore limited to reusable sorting algorithms; leave merge-path pipeline observer integration, chunk sorters, spill-aware sorting, and exec-node adapters in higher modules.

### ExecJoinCore (`execjoincore`)
Reusable exec join hash table algorithms without join nodes, pipeline, storage, service, or util coupling.
- Targets: `ExecJoinCore`
- Allowed internal include prefixes: `exec/join/`, `exec/sorting/sort_helper.h`, `exprs/`, `serde/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExecSortingCore`, `ExprCore`, `Serde`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `TypesCore`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `exec_join_core_test`
- Remediation: Keep ExecJoinCore limited to reusable join hash table algorithms; leave join nodes, pipeline operators, storage/service integration, and util diagnostics in higher modules.

### StarOSIntegration (`starosintegration`)
StarRocks-owned Starlet integration for worker runtime, shard metadata, status conversion, StarCache updates, and worker metrics outside service bootstrap.
- Targets: `StarOSIntegration`
- Allowed internal include prefixes: `staros_integration/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `staros_integration_test`
- Remediation: Keep the Starlet adapter independent of service/bootstrap, storage, runtime, and filesystem modules; expose narrow accessors instead of pulling higher layers into lower layers.
<!-- END GENERATED: BE MODULE HARNESSES -->

## BE-Specific Sync Rules

- BE configs are declared in `be/src/common/config.h`; update `docs/en/administration/management/BE_configuration.md` and `docs/zh/` peer docs when config behavior changes.
- BE metrics must update the matching monitoring docs when names, meanings, or labels change.
- `be/src/common` has extra config-header rules in [`be/src/common/AGENTS.md`](./src/common/AGENTS.md).

## Verification Before Handoff

Run the smallest relevant UT binary plus the architecture harness:

```bash
python3 build-support/check_be_module_boundaries.py --mode full
python3 build-support/render_be_agents.py --check
```

If you changed a core module, prefer its focused test binary before broader `run-be-ut.sh`.
