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
Cache implementation module for DataCache facade, cache engines, scan read-buffer/cache stream wrappers, monitors, metrics, utilities, StarCache integration, and peer-cache RPC reads without service/bootstrap or ExecEnv singleton coupling.
- Targets: `Cache`
- Allowed internal include prefixes: `cache/`, `common/brpc/`, `runtime/current_thread.h`, `runtime/mem_tracker.h`, `fs/`, `io/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `RuntimeCore`, `FSCore`, `IO`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `cache_test`
- Remediation: Keep Cache self-contained within cache engines, scan read-buffer/cache stream wrappers, monitors, metrics, utilities, and injected peer-cache BRPC stubs; keep service startup, storage code, HTTP/admin code, and ExecEnv singleton access outside cache.

### HttpCore (`httpcore`)
Reusable HTTP transport and request primitives above Common without BE admin or page-handler code.
- Targets: `HttpCore`
- Allowed internal include prefixes: `http/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`
- Core tests: `http_core_test`
- Remediation: Keep HttpCore limited to reusable HTTP transport and request primitives; move BE-specific pages, actions, auth helpers, and client/download helpers upward.

### IO (`io`)
Minimal IO foundation used by FS and upper layers.
- Targets: `IO`
- Allowed internal include prefixes: `io/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`
- Core tests: `io_test`
- Remediation: Keep IO free of higher FS/runtime/storage/exec/service code; move the dependency upward or add a lower-level interface.

### FSCore (`fscore`)
Minimal filesystem core on top of IO.
- Targets: `FSCore`
- Allowed internal include prefixes: `fs/`, `io/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `IO`, `Common`, `Base`, `Gutil`
- Core tests: `fs_core_test`
- Remediation: Keep FSCore limited to IO plus core FS abstractions; move backend-specific behavior into FileSystem.

### Platform (`platform`)
Shared BE platform utilities above IO/FS/Common and below Runtime/Exec/Storage/Service.
- Targets: `Platform`
- Allowed internal include prefixes: `platform/`, `http/`, `fs/`, `io/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `HttpCore`, `FSCore`, `IO`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `platform_test`
- Remediation: Keep Platform limited to reusable host, filesystem-adjacent, download, temp-file, environment, retry, and platform-level helpers; move runtime, exec, storage, and service integration upward.

### Types (`typecore`)
Core type system without runtime/storage/exec coupling.
- Targets: `Types`
- Allowed internal include prefixes: `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`
- Core tests: `types_test`
- Remediation: Keep Types independent of runtime/util/storage/exec layers; move integration code into higher layers.

### Retrieval (`retrieval`)
Pure retrieval framework: ANN index interface, row-id filter contracts, and search strategy orchestration under retrieval/vector/. Designed to also host future full-text and hybrid retrieval abstractions under sibling subdirectories without storage/exec/runtime/connector coupling.
- Targets: `Retrieval`
- Allowed internal include prefixes: `retrieval/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`
- Core tests: `retrieval_test`
- Remediation: Keep Retrieval as a pure interface layer; concrete index implementations (TenANN/HNSW/Paimon/full-text) and scanner integration belong in their owning modules and depend on Retrieval downward.

### ColumnCore (`columncore`)
Core column representations that must stay independent of ChunkCore and higher layers.
- Targets: `ColumnCore`
- Allowed internal include prefixes: `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `column_test`
- Remediation: Keep ColumnCore free of ChunkCore/Runtime/Exec/Storage coupling; move integration code upward or introduce an interface.

### ChunkCore (`chunkcore`)
Core chunk, schema, field, and chunk-adjacent helpers on top of ColumnCore.
- Targets: `ChunkCore`
- Allowed internal include prefixes: `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `chunk_test`
- Remediation: Keep ChunkCore limited to chunk/schema helpers and free of Runtime/Exec/Storage/Serde coupling.

### RuntimeCore (`runtimecore`)
Core runtime building blocks without full Runtime/Exec/Storage coupling.
- Targets: `RuntimeCore`
- Allowed internal include prefixes: `runtime/`, `platform/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Platform`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `runtime_core_test`
- Remediation: Keep RuntimeCore restricted to core runtime infrastructure; move service/storage/stream-load/integration code into Runtime.

### FormatCore (`formatcore`)
Format-oriented core primitives above ComputeEnv, ExecPrimitive, ExprCore, RuntimeCore, FSCore, ChunkCore, ColumnCore, and Types.
- Targets: `FormatCore`
- Allowed internal include prefixes: `formats/column_evaluator.h`, `formats/deletion_bitmap.h`, `formats/disk_range.hpp`, `formats/file_writer.h`, `formats/io/`, `formats/reserved_columns.h`, `formats/scan_context.h`, `formats/utils.h`, `exprs/`, `runtime/`, `fs/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ComputeEnv`, `ExecPrimitive`, `ExprCore`, `RuntimeCore`, `FSCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `format_test`
- Remediation: Keep FormatCore limited to reusable format primitives; move connector orchestration and higher execution policy upward.

### RuntimeEnv (`runtimeenv`)
Process-scoped runtime environment resources below full Runtime and above RuntimeCore.
- Targets: `RuntimeEnv`
- Allowed internal include prefixes: `runtime/env/`, `runtime/`, `platform/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Platform`, `RuntimeCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `runtime_env_test`
- Remediation: Keep RuntimeEnv limited to process-scoped runtime environment resources; move query execution, storage, service, connector, and UDF integration upward.

### StoragePrimitive (`storageprimitive`)
Primitive storage contracts, predicate contracts, predicate trees, and value types shared below ComputeEnv without concrete Storage engine, tablet, rowset, lake, service, or full Exec coupling.
- Targets: `StoragePrimitive`
- Allowed internal include prefixes: `storage/primitive/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `storage_primitive_test`
- Remediation: Keep StoragePrimitive limited to reusable storage contracts, predicate contracts, predicate trees, and value types; move concrete storage engine, tablet, rowset, lake, service, connector, and full Exec integration upward.

### StorageBase (`storagebase`)
Base storage algorithms and mask-buffer helpers above StoragePrimitive and ComputeEnv without concrete Storage engine, tablet, rowset, lake, service, or full Exec coupling.
- Targets: `StorageBase`
- Allowed internal include prefixes: `storage/base/`, `storage/primitive/`, `compute_env/sorting/`, `serde/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ComputeEnv`, `StoragePrimitive`, `Serde`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `storage_base_test`
- Remediation: Keep StorageBase limited to reusable storage algorithms and helpers that may depend on ComputeEnv; move concrete storage engine, tablet, rowset, lake, service, connector, cache, and full Exec integration upward.

### ComputeEnv (`computeenv`)
Shared compute-side BE environment boundary for process-scoped compute resources, load-path management, query-scoped scan coordination helpers, query-cache primitives, WorkGroup scheduling/executor resources, and pipeline controls below full Exec/Storage and above RuntimeEnv.
- Targets: `ComputeEnv`
- Allowed internal include prefixes: `compute_env/`, `exec/runtime_filter/`, `exec/pipeline/pipeline_fwd.h`, `exec/pipeline/operator.h`, `exec/pipeline/primitives/`, `exec/pipeline/runtime_filter_core_types.h`, `exec/pipeline/scan/scan_morsel.h`, `exec/pipeline/scan/morsel_queue.h`, `exec/pipeline/scan/morsel_queue_builder.h`, `exec/pipeline/scan/fixed_morsel_queue.h`, `exec/pipeline/scan/fixed_morsel_queue_builder.h`, `exec/pipeline/scan/dynamic_morsel_queue.h`, `exec/pipeline/scan/dynamic_morsel_queue_builder.h`, `exec/pipeline/scan/ticketed_morsel_queue.h`, `storage/primitive/`, `exprs/`, `serde/`, `runtime/env/`, `runtime/`, `util/time_guard.h`, `platform/`, `fs/`, `io/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `StoragePrimitive`, `ExecPrimitive`, `ExprCore`, `Serde`, `Util`, `RuntimeEnv`, `RuntimeCore`, `Platform`, `FSCore`, `IO`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `compute_env_test`, `compute_env_sorting_test`
- Remediation: Keep ComputeEnv limited to process-scoped compute resources, load-path management without ExecEnv or concrete storage engine coupling, query-scoped scan coordination helpers without concrete scan/storage policy, query-cache primitives without concrete storage policy, WorkGroup scheduling/executor resources, shared compute-side service contracts, stable execution primitives, reusable compute-side sorting algorithms, and spill infrastructure; move concrete Exec, Storage, Service, Connector, and Agent integration upward.

### AgentServer (`agentserver`)
FE-agent task orchestration, heartbeat handling, agent metrics, and agent worker helpers below Service.
- Targets: `AgentServer`
- Allowed internal include prefixes: `agent/`, `data_workflows/`, `cache/`, `compute_env/workgroup/`, `exec/pipeline/query_context.h`, `exec/runtime/query_context_manager.h`, `fs/`, `io/`, `runtime/`, `storage/`, `platform/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Runtime`, `DataWorkflows`, `Storage`, `Cache`, `ComputeEnv`, `ExecRuntime`, `StoragePrimitive`, `RuntimeCore`, `Platform`, `FSCore`, `IO`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep AgentServer as FE-agent task and heartbeat orchestration below Service; move service/bootstrap integration upward.

### DataWorkflows (`dataworkflows`)
Data-plane workflow orchestration above concrete Storage and Exec for load, delete, schema-change, and tablet-write workflows without service/bootstrap ownership.
- Targets: `DataWorkflows`
- Allowed internal include prefixes: `data_workflows/`, `storage/`, `exec/`, `runtime/`, `compute_env/`, `serde/`, `platform/`, `fs/`, `io/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Storage`, `Rowset`, `Exec`, `Runtime`, `StorageBase`, `StoragePrimitive`, `ExecRuntime`, `ExecPrimitive`, `ComputeEnv`, `Serde`, `RuntimeEnv`, `RuntimeCore`, `Platform`, `FSCore`, `IO`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `data_workflows_test`
- Remediation: Keep DataWorkflows limited to data-plane orchestration over concrete Storage and Exec; move service/bootstrap, agent scheduling, connector registration, and lower reusable primitives to their owning layers.

### ExprCore (`exprcore`)
Core expression infrastructure that depends only on RuntimeCore and lower layers.
- Targets: `ExprCore`
- Allowed internal include prefixes: `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `expr_core_test`
- Remediation: Keep ExprCore limited to core expression infrastructure; move aggregate/UDF/integration code into Exprs.

### ExecPrimitive (`execprimitive`)
Primitive execution contracts, DataSink base contract, runtime-filter infrastructure, generic morsel queues, and stable pipeline operator/factory primitives without broader Exec runtime, scheduler, storage, service, or connector coupling.
- Targets: `ExecPrimitive`
- Allowed internal include prefixes: `exec/data_sink.h`, `exec/runtime_filter/`, `exec/pipeline/pipeline_fwd.h`, `exec/pipeline/operator.h`, `exec/pipeline/operator_factory.h`, `exec/pipeline/primitives/`, `exec/pipeline/runtime_filter_hub.h`, `exec/pipeline/runtime_filter_core_types.h`, `exec/pipeline/scan/scan_morsel.h`, `exec/pipeline/scan/morsel_queue.h`, `exec/pipeline/scan/morsel_queue_builder.h`, `exec/pipeline/scan/fixed_morsel_queue.h`, `exec/pipeline/scan/fixed_morsel_queue_builder.h`, `exec/pipeline/scan/dynamic_morsel_queue.h`, `exec/pipeline/scan/dynamic_morsel_queue_builder.h`, `exec/pipeline/scan/split_morsel_ticket_checker.h`, `exec/pipeline/scan/ticketed_morsel_queue.h`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `exec_primitive_test`
- Remediation: Keep ExecPrimitive limited to execution contracts, the DataSink base contract, runtime filters, generic morsel queues, and stable operator/factory primitives; move runtime, scheduler, concrete operators, concrete sinks, storage, service, and connector integration upward.

### ExecRuntime (`execruntime`)
Operator-tree execution framework for query and fragment contexts, driver lifecycle, execution groups, and scheduling-adjacent behavior above ComputeEnv and ExecPrimitive without concrete operators, storage, service, connector, cache, or broad Exec coupling.
- Targets: `ExecRuntime`
- Allowed internal include prefixes: `compute_env/`, `exec/runtime/`, `exec/pipeline/pipeline_fwd.h`, `exec/pipeline/operator.h`, `exec/pipeline/operator_with_dependency.h`, `exec/pipeline/source_operator.h`, `exec/pipeline/primitives/`, `exec/pipeline/runtime_filter_hub.h`, `exec/pipeline/runtime_filter_core_types.h`, `exec/pipeline/scan/morsel_queue.h`, `exec/pipeline/scan/split_morsel_ticket_checker.h`, `exec/pipeline/scan/ticketed_morsel_queue.h`, `exec/runtime_filter/`, `storage/primitive/`, `exprs/`, `serde/`, `runtime/env/`, `runtime/`, `platform/`, `fs/`, `io/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ComputeEnv`, `ExecPrimitive`, `StoragePrimitive`, `ExprCore`, `Serde`, `RuntimeEnv`, `RuntimeCore`, `Platform`, `FSCore`, `IO`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `exec_runtime_test`
- Remediation: Keep ExecRuntime limited to the operator-tree execution framework and runtime behavior that can be expressed through ComputeEnv and ExecPrimitive contracts; move concrete operators, storage, service, connector, cache, HTTP, and broad Exec integration upward.

### ConnectorPrimitive (`connectorprimitive`)
Read-side connector contracts, DataSource, and DataSourceProvider default mechanics without concrete connectors, sinks, registry composition, storage, service, or full Exec coupling.
- Targets: `ConnectorPrimitive`
- Allowed internal include prefixes: `connector/connector.h`, `connector/data_source.h`, `connector/data_source_provider.h`, `exec/pipeline/scan/scan_morsel.h`, `exec/pipeline/scan/morsel_queue_builder.h`, `exec/pipeline/scan/dynamic_morsel_queue_builder.h`, `exec/runtime_filter/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExecPrimitive`, `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_primitive_test`
- Remediation: Keep ConnectorPrimitive limited to read-side connector contracts and default scan-range-to-morsel mechanics; move concrete connectors, sinks, registry wiring, storage, service, and full Exec integration upward.

### ConnectorBootstrap (`connectorbootstrap`)
Connector-layer bootstrap for split connector libraries that install into the default registry without depending on the legacy built-in registry.
- Targets: `ConnectorBootstrap`
- Allowed internal include prefixes: `connector/connector_bootstrap.h`, `connector/benchmark/`, `connector/elasticsearch/`, `connector/mysql/`, `connector/connector.h`, `connector/connector_registry.h`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Connector`, `ConnectorBenchmark`, `ConnectorElasticsearch`, `ConnectorMySQL`, `ConnectorPrimitive`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep split connector bootstrap independent from ConnectorBuiltinRegistry; add newly split connector libraries here and let service-level startup compose legacy registry plus this bootstrap.

### ConnectorBuiltinRegistry (`connectorbuiltinregistry`)
Top-level built-in connector registration composition above connector contracts and concrete connector libraries.
- Targets: `ConnectorBuiltinRegistry`
- Allowed internal include prefixes: `connector/builtin_connector_registry.h`, `connector/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Connector`, `ConnectorPrimitive`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep legacy built-in connector registration as top-level composition for unsplit connector libraries; split connector libraries register through ConnectorBootstrap instead of depending back on this target.

### ExecSchemaScannerCore (`execschemascannercore`)
Schema scanner base contract and shared mechanics without concrete scanner, pipeline, storage, service, or ExecEnv coupling.
- Targets: `ExecSchemaScannerCore`
- Allowed internal include prefixes: `exec/schema_scanner.h`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `schema_scanner_core_test`
- Remediation: Keep SchemaScannerCore limited to the base SchemaScanner contract; move concrete scanner creation and service-specific logic into higher schema scanner modules.

### ExecSchemaScanners (`execschemascanners`)
Clean concrete schema scanners that do not depend on SchemaHelper, FE RPC/client helpers, storage, service, cache, pipeline, or ExecEnv.
- Targets: `ExecSchemaScanners`
- Allowed internal include prefixes: `exec/schema_scanner.h`, `exec/schema_scanner/schema_column_filler.h`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExecSchemaScannerCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `exec_schema_scanners_test`
- Remediation: Keep this first schema scanner target limited to clean local/static scanners; leave SchemaHelper, storage, HTTP, cache, service, and ExecEnv users in higher compatibility modules until they get explicit boundaries.

### ExecJoinCore (`execjoincore`)
Reusable exec join hash table algorithms without join nodes, pipeline, storage, service, or util coupling.
- Targets: `ExecJoinCore`
- Allowed internal include prefixes: `exec/join/`, `compute_env/sorting/sort_helper.h`, `exprs/`, `serde/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ComputeEnv`, `ExprCore`, `Serde`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `exec_join_core_test`
- Remediation: Keep ExecJoinCore limited to reusable join hash table algorithms; leave join nodes, pipeline operators, storage/service integration, and util diagnostics in higher modules.

### StarOSIntegration (`starosintegration`)
StarRocks-owned Starlet integration for worker runtime, shard metadata, status conversion, StarCache updates, and worker metrics outside service bootstrap.
- Targets: `StarOSIntegration`
- Allowed internal include prefixes: `staros_integration/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `staros_integration_test`
- Remediation: Keep the Starlet adapter independent of service/bootstrap, storage, runtime, and filesystem modules; expose narrow accessors instead of pulling higher layers into lower layers.

### Service (`service`)
Shared service-layer target above runtime, cache, compute, and AgentServer without owning ServiceBE bootstrap code.
- Targets: `Service`
- Allowed internal include prefixes: `service/`, `agent/`, `base/`, `cache/`, `column/`, `common/`, `connector/`, `compute_env/`, `exec/`, `exprs/`, `formats/`, `fs/`, `gen_cpp/`, `gutil/`, `http/`, `io/`, `platform/`, `runtime/`, `serde/`, `staros_integration/`, `storage/`, `types/`, `util/`
- Allowed target deps: `Runtime`, `RuntimeEnv`, `RuntimeCore`, `Cache`, `AgentServer`, `ComputeEnv`, `ExecRuntime`, `ExecPrimitive`, `Platform`, `Storage`, `StoragePrimitive`, `StorageBase`, `FSCore`, `FileSystem`, `IO`, `HttpCore`, `Webserver`, `Common`, `Base`, `Gutil`, `StarRocksGen`, `StarOSIntegration`, `Connector`, `ConnectorPrimitive`, `ConnectorBootstrap`, `ConnectorBuiltinRegistry`, `Exec`, `FormatCore`, `Formats`, `FormatOrc`, `Util`, `Serde`, `ChunkCore`, `ColumnCore`, `Types`
- Remediation: Keep shared Service below ServiceBE and depend on checked module targets such as AgentServer instead of ad hoc lower-layer reach-through.
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
