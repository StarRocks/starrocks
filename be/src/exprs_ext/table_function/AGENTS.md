<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ExprTableFunction (`exprtablefunction`)
Table-function expression extensions that need concrete storage integration.
- Targets: `ExprTableFunction`
- Allowed internal include prefixes: `exprs_ext/table_function/`, `exprs/`, `storage/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Expr`, `Storage`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep table-function extensions in ExprTableFunction; reusable table-function contracts belong in Expr.
<!-- END GENERATED: BE MODULE HARNESSES -->
