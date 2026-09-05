<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorHive (`connectorhive`)
Concrete Hive-compatible connector read/write implementation, including HDFS scanner formats and JNI-backed lake readers, without registry composition or full Exec coupling.
- Targets: `ConnectorHive`
- Allowed internal include prefixes: `connector/hive/`, `connector/common/`, `connector_primitive/`, `compute_env/`, `cache/`, `storage_primitive/`, `exec_primitive/`, `formats/`, `exprs/`, `runtime/`, `platform/`, `fs/`, `io/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorCommon`, `ConnectorPrimitive`, `ComputeEnv`, `Cache`, `StoragePrimitive`, `ExecPrimitive`, `Expr`, `Runtime`, `Platform`, `Formats`, `FormatCore`, `FormatCsv`, `FormatJson`, `FormatAvro`, `FormatOrc`, `FormatParquet`, `FileSystem`, `IO`, `ColumnSortCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_hive_test`
- Remediation: Keep ConnectorHive limited to Hive-compatible connector scans and sinks over lower connector, compute, runtime, format, and filesystem modules; keep registration in ModuleBootstrap and avoid full Exec or concrete Storage dependencies.
<!-- END GENERATED: BE MODULE HARNESSES -->
