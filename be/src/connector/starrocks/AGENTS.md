<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### ConnectorStarRocks (`connectorstarrocks`)
STARROCKS external-catalog remote-scan client connector that fetches brpc-chunk / Arrow Flight result streams from a remote StarRocks cluster, without registry composition, service, or full Exec coupling.
- Targets: `ConnectorStarRocks`
- Allowed internal include prefixes: `connector/starrocks/`, `connector_primitive/`, `compute_env/`, `storage/`, `storage_primitive/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ConnectorPrimitive`, `Storage`, `ComputeEnv`, `StoragePrimitive`, `Expr`, `Runtime`, `Platform`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `connector_starrocks_test`
- Remediation: Keep ConnectorStarRocks limited to the remote-scan client (fetch RPC / Arrow Flight decode) over connector/runtime contracts; keep registration in ModuleBootstrap and avoid registry, service, or full Exec coupling.
<!-- END GENERATED: BE MODULE HARNESSES -->
