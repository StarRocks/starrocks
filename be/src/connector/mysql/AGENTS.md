<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorMySQL (`connectormysql`)
MySQL connector implementation above connector contracts without registry composition, storage, service, or full Exec coupling.
- Targets: `ConnectorMySQL`
- Allowed internal include prefixes: `connector/mysql/`, `connector/connector.h`, `connector/data_source.h`, `connector/data_source_provider.h`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorPrimitive`, `ExprCore`, `RuntimeCore`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_mysql_test`
- Remediation: Keep ConnectorMySQL limited to MySQL read-side scan and scanner logic; move registration into ConnectorBootstrap and avoid pulling Connector, storage, service, or full Exec code into the connector library.
<!-- END GENERATED: BE MODULE HARNESSES -->
