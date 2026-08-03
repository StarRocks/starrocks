<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorFile (`connectorfile`)
Concrete file connector read/write implementation, including file scanner formats, file scan metrics, and connector-native file chunk sink, without registry composition or full Exec coupling.
- Targets: `ConnectorFile`
- Allowed internal include prefixes: `connector/file/`, `connector/common/`, `connector_primitive/`, `compute_env/`, `formats/`, `exprs/`, `runtime/`, `platform/`, `fs/`, `io/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorCommon`, `ConnectorPrimitive`, `ComputeEnv`, `Cache`, `Expr`, `Runtime`, `Platform`, `FormatCore`, `FormatCsv`, `FormatJson`, `FormatAvro`, `FormatOrc`, `FormatParquet`, `FileSystem`, `IO`, `ColumnSortCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_file_test`
- Remediation: Keep ConnectorFile limited to concrete file connector scanner and connector-native sink behavior over connector contracts, compute/runtime helpers, formats, and FS/IO; move registry wiring and pipeline/export orchestration upward.
<!-- END GENERATED: BE MODULE HARNESSES -->
