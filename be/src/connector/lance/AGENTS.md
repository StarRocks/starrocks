<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorLance (`connectorlance`)
Lance dataset connector implementation that reads Arrow batches through its Rust SDK FFI above connector contracts and without registry composition, storage, service, or full Exec coupling.
- Targets: `ConnectorLance`
- Allowed internal include prefixes: `connector/lance/`, `formats/arrow/`, `connector_primitive/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `FormatCore`, `ConnectorPrimitive`, `Expr`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep ConnectorLance limited to Lance read-side scan logic that uses its isolated Rust SDK FFI; keep registration in ModuleBootstrap and avoid registry, storage, service, or full Exec dependencies.
<!-- END GENERATED: BE MODULE HARNESSES -->
