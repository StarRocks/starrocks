<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorJDBC (`connectorjdbc`)
JDBC connector implementation, scanner, driver manager, and type checker above connector contracts without registry composition, storage, service, or full Exec coupling.
- Targets: `ConnectorJDBC`
- Allowed internal include prefixes: `connector/jdbc/`, `connector/connector.h`, `connector/data_source.h`, `connector/data_source_provider.h`, `platform/`, `fs/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorPrimitive`, `Platform`, `FileSystem`, `Expr`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_jdbc_test`
- Remediation: Keep ConnectorJDBC limited to JDBC read-side scan, driver management, and type checking; move registration into ConnectorBootstrap and avoid pulling Connector, storage, service, or full Exec code into the connector library.
<!-- END GENERATED: BE MODULE HARNESSES -->
