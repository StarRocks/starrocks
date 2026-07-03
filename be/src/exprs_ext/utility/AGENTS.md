<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ExprUtility (`exprutility`)
Utility expression functions that need runtime, execution-runtime, platform, or storage-primitive integration.
- Targets: `ExprUtility`
- Allowed internal include prefixes: `exprs_ext/utility/`, `exprs/`, `exec/pipeline/fragment_context.h`, `storage/primitive/`, `platform/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Expr`, `ExecRuntime`, `StoragePrimitive`, `Platform`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep utility expression functions in ExprUtility; move lower reusable helpers into their owning modules instead of expanding Expr.
<!-- END GENERATED: BE MODULE HARNESSES -->
