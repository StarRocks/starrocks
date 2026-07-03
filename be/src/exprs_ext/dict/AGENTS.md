<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ExprDict (`exprdict`)
Dictionary expression extensions and dict expression factory registration above Expr, ComputeEnv, and Storage integration.
- Targets: `ExprDict`
- Allowed internal include prefixes: `exprs_ext/dict/`, `exprs/`, `compute_env/`, `storage/`, `exec/exec_env.h`, `platform/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Expr`, `ComputeEnv`, `Storage`, `StoragePrimitive`, `Exec`, `Platform`, `Runtime`, `ColumnSortCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep dictionary-specific expression implementations in ExprDict; shared expression contracts belong in Expr, and broader query execution orchestration belongs above the extension.
<!-- END GENERATED: BE MODULE HARNESSES -->
