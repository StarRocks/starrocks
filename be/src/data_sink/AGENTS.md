<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### DataSinkFile (`datasinkfile`)
Reusable file sink builders for delimited text and Parquet output over explicit format, filesystem, expression, and column dependencies without full Exec, concrete Storage, or orchestration coupling.
- Targets: `DataSinkFile`
- Allowed internal include prefixes: `data_sink/file/`, `formats/`, `fs/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `FormatCore`, `FormatCsv`, `FormatParquet`, `Expr`, `FileSystem`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `data_sink_file_test`
- Remediation: Keep file serialization builders independent of full Exec and concrete Storage; place sink lifecycle and orchestration in higher data-sink modules instead of expanding this leaf target.
<!-- END GENERATED: BE MODULE HARNESSES -->
