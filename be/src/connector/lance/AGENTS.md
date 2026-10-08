<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorLance (`connectorlance`)
Lance dataset connector implementation that delegates scan reads to the shared JNI scanner above connector contracts and without registry composition, storage, service, or full Exec coupling.
- Targets: `ConnectorLance`
- Allowed internal include prefixes: `connector/lance/`, `connector/hive/scanner/hdfs_scanner_context.h`, `connector/hive/scanner/jni_scanner.h`, `connector_primitive/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorHive`, `ConnectorPrimitive`, `Expr`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep ConnectorLance limited to Lance read-side scan logic that delegates to the shared JNI scanner; keep registration in ModuleBootstrap and avoid registry, storage, service, or full Exec dependencies.
<!-- END GENERATED: BE MODULE HARNESSES -->
