<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### SchemaScannerBuiltin (`schemascannerbuiltin`)
Builtin schema scanner factory composition above the concrete scanner module without full Exec coupling.
- Targets: `SchemaScannerBuiltin`
- Allowed internal include prefixes: `schema_scanner/`, `exec/schema_scanner.h`, `exec/schema_scanner_factory.h`, `gen_cpp/`
- Allowed target deps: `SchemaScanners`, `ExecSchemaScannerCore`
- Core tests: `schema_scanner_test`
- Remediation: Keep builtin scanner selection as composition over SchemaScanners and ExecSchemaScannerCore; do not link or include full Exec.

### SchemaScanners (`schemascanners`)
Concrete schema scanner implementations over the Exec-owned core contract, FE RPC helpers, Storage, Cache, and Platform without full Exec coupling.
- Targets: `SchemaScanners`
- Allowed internal include prefixes: `schema_scanner/`, `exec/schema_scanner.h`, `storage/`, `storage_primitive/`, `cache/`, `platform/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExecSchemaScannerCore`, `Storage`, `StoragePrimitive`, `Cache`, `Platform`, `Expr`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep concrete scanners in SchemaScanners over explicit lower module dependencies; route execution through the core contract and keep full Exec, service, and connector code out.
<!-- END GENERATED: BE MODULE HARNESSES -->
