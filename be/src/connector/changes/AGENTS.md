<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorChanges (`connectorchanges`)
CHANGES connector and per-publish change-read planner above Storage, without registry composition, service, or full Exec coupling.
- Targets: `ConnectorChanges`
- Allowed internal include prefixes: `connector/changes/`, `connector_primitive/`, `compute_env/`, `storage/`, `storage_primitive/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorPrimitive`, `Storage`, `ComputeEnv`, `StoragePrimitive`, `Expr`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_changes_test`
- Remediation: Keep ConnectorChanges limited to CHANGES scan planning and reads over Storage and lower compute/runtime contracts; keep registration in ModuleBootstrap and avoid registry, service, or full Exec coupling.
<!-- END GENERATED: BE MODULE HARNESSES -->
