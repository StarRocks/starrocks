<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorIceberg (`connectoriceberg`)
Concrete Iceberg connector write implementation, including data, position-delete, and row-delta sinks, without registry composition or full Exec and Storage coupling.
- Targets: `ConnectorIceberg`
- Allowed internal include prefixes: `connector/iceberg/`, `connector/common/`, `connector_primitive/`, `formats/`, `exprs/`, `runtime/`, `fs/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorCommon`, `ConnectorPrimitive`, `Expr`, `Runtime`, `FormatCore`, `FormatParquet`, `FileSystem`, `ColumnSortCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_iceberg_test`
- Remediation: Keep ConnectorIceberg limited to Iceberg connector sinks and format-specific helpers over lower connector, runtime, format, and filesystem modules; keep registration in ModuleBootstrap and orchestration in Exec.
<!-- END GENERATED: BE MODULE HARNESSES -->
