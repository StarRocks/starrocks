<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorIceberg (`connectoriceberg`)
Iceberg connector implementation, connector-native Iceberg sinks, partition utilities, and Iceberg delete-file build helpers without registry composition, storage, service, or full Exec coupling.
- Targets: `ConnectorIceberg`
- Allowed internal include prefixes: `connector/iceberg/`, `connector/common/`, `connector_primitive/`, `compute_env/`, `cache/`, `exprs/`, `runtime/`, `platform/`, `formats/`, `fs/`, `io/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorCommon`, `ConnectorPrimitive`, `ComputeEnv`, `Cache`, `Expr`, `Runtime`, `Platform`, `FormatCore`, `FormatOrc`, `FormatParquet`, `FileSystem`, `IO`, `ColumnSortCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_iceberg_test`
- Remediation: Keep ConnectorIceberg limited to Iceberg-specific connector, sink, partition, and delete-file build behavior over connector contracts and lower scan/file primitives; move registry wiring and pipeline/table-sink orchestration upward.
<!-- END GENERATED: BE MODULE HARNESSES -->
