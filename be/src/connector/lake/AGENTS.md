<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorLake (`connectorlake`)
Concrete shared-data lake connector and lake late-materialization context above Storage, without registry composition, service, or full Exec coupling.
- Targets: `ConnectorLake`
- Allowed internal include prefixes: `connector/lake/`, `connector_primitive/`, `storage/`, `storage_primitive/`, `compute_env/`, `exec_primitive/`, `exprs/`, `runtime/`, `platform/`, `fs/`, `io/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorPrimitive`, `Storage`, `ComputeEnv`, `StoragePrimitive`, `ExecPrimitive`, `Expr`, `Runtime`, `Platform`, `FileSystem`, `IO`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_lake_test`
- Remediation: Keep ConnectorLake limited to shared-data lake scans and lake late-materialization state over Storage and lower compute/runtime contracts; keep registration in ModuleBootstrap and avoid registry, service, or full Exec coupling.
<!-- END GENERATED: BE MODULE HARNESSES -->
