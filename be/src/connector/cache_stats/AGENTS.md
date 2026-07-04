<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorCacheStats (`connectorcachestats`)
Cache-stats connector implementation above connector contracts with storage access, without registry composition, service, cache, or full Exec coupling.
- Targets: `ConnectorCacheStats`
- Allowed internal include prefixes: `connector/cache_stats/`, `connector/connector.h`, `connector/data_source.h`, `connector/data_source_provider.h`, `storage/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorPrimitive`, `Storage`, `Expr`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_cache_stats_test`
- Remediation: Keep ConnectorCacheStats limited to the cache-stats connector and scanner implementation over Storage; move registration into ConnectorBootstrap and avoid pulling Connector, service, cache, or full Exec code into the connector library.
<!-- END GENERATED: BE MODULE HARNESSES -->
