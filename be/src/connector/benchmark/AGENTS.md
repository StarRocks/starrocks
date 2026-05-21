<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorBenchmark (`connectorbenchmark`)
Benchgen-backed benchmark connector implementation above connector contracts without registry composition, storage, service, or full Exec coupling.
- Targets: `ConnectorBenchmark`
- Allowed internal include prefixes: `connector/benchmark/`, `connector/connector.h`, `connector/data_source.h`, `connector/data_source_provider.h`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorPrimitive`, `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep ConnectorBenchmark limited to the benchgen connector implementation; move registration into ConnectorBootstrap and avoid pulling Connector, storage, service, or full Exec code into the connector library.
<!-- END GENERATED: BE MODULE HARNESSES -->
