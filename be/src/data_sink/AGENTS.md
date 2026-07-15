<!-- BEGIN GENERATED: BE MODULE HARNESSES -->
## Module Harness

This section is generated from `be/module_boundary_manifest.json`.
Run `python3 build-support/render_be_agents.py --write` after changing the manifest.
Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.

### DataSinkPipelineCore (`datasinkpipelinecore`)
Immutable pipeline sink provider contracts and dispatch over execution-runtime primitives without concrete Exec pipeline composition, sink implementations, or orchestration coupling.
- Targets: `DataSinkPipelineCore`
- Allowed internal include prefixes: `data_sink/pipeline/`, `exec/runtime/query_context.h`, `exec/pipeline/fragment_execution_params.h`, `exec_primitive/`, `runtime/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExecRuntime`, `ExecPrimitive`, `Runtime`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `data_sink_pipeline_core_test`
- Remediation: Keep provider metadata, immutable lookup, and dispatch independent of concrete pipeline graph construction; place graph adapters in Orchestration and sink implementations in their owning data-sink modules.

### DataSinkFile (`datasinkfile`)
Reusable file sink builders for delimited text and Parquet output over explicit format, filesystem, expression, and column dependencies without full Exec, concrete Storage, or orchestration coupling.
- Targets: `DataSinkFile`
- Allowed internal include prefixes: `data_sink/file/`, `formats/`, `fs/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `FormatCore`, `FormatCsv`, `FormatParquet`, `Expr`, `FileSystem`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `data_sink_file_test`
- Remediation: Keep file serialization builders independent of full Exec and concrete Storage; place sink lifecycle and orchestration in higher data-sink modules instead of expanding this leaf target.

### DataSinkResult (`datasinkresult`)
Query-result and memory-scratch sinks plus their protocol and file writers over explicit compute-result, execution-contract, format, filesystem, expression, and runtime dependencies without full Exec or concrete Storage coupling.
- Targets: `DataSinkResult`
- Allowed internal include prefixes: `data_sink/result/`, `data_sink/file/`, `compute_env/result/`, `exec_primitive/`, `formats/`, `exprs/`, `runtime/`, `platform/`, `fs/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `DataSinkFile`, `ComputeEnv`, `ExecPrimitive`, `FormatCore`, `FormatCsv`, `FormatParquet`, `Expr`, `Runtime`, `Platform`, `FileSystem`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `data_sink_result_test`
- Remediation: Keep result serialization and delivery in DataSinkResult over compute-result and lower contracts; keep concrete storage, full Exec composition, and service/bootstrap behavior outside this module.

### DataSinkExchange (`datasinkexchange`)
Inter-backend exchange sink implementations over compute data-stream services, execution contracts, expressions, runtime serialization, and RPC primitives without full Exec, Storage, or service coupling.
- Targets: `DataSinkExchange`
- Allowed internal include prefixes: `data_sink/exchange/`, `compute_env/data_stream/`, `exec_primitive/`, `exprs/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ComputeEnv`, `ExecPrimitive`, `Expr`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep exchange sending in DataSinkExchange over compute data-stream and lower contracts; keep pipeline composition, storage, and service/bootstrap behavior outside this module.

### DataSinkExternal (`datasinkexternal`)
Export, MySQL, Hive, Iceberg, table-function, schema, blackhole, and no-op sinks over reusable file builders, formats, filesystem, execution contracts, and runtime helpers without full Exec or concrete Storage coupling.
- Targets: `DataSinkExternal`
- Allowed internal include prefixes: `data_sink/external/`, `data_sink/file/`, `storage_primitive/`, `exec_primitive/`, `formats/`, `exprs/`, `runtime/`, `platform/`, `fs/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `DataSinkFile`, `StoragePrimitive`, `ExecPrimitive`, `FormatCore`, `FormatCsv`, `Expr`, `Runtime`, `Platform`, `FileSystem`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Remediation: Keep non-tablet external and administrative sinks in DataSinkExternal over file, format, filesystem, and lower contracts; keep concrete storage, full Exec composition, and service/bootstrap behavior outside this module.

### DataSinkDictionaryCache (`datasinkdictionarycache`)
Pipeline-only dictionary-cache sink lifecycle over execution contracts and runtime without concrete Exec operator or writer coupling.
- Targets: `DataSinkDictionaryCache`
- Allowed internal include prefixes: `data_sink/dictionary_cache/`, `exec_primitive/`, `runtime/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `ExecPrimitive`, `Runtime`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `data_sink_dictionary_cache_test`
- Remediation: Keep dictionary-cache sink lifecycle independent of its Exec-owned pipeline operator and writer; pipeline composition remains in Exec.

### DataSinkTablet (`datasinktablet`)
OLAP tablet routing, validation, channel management, and RPC sending over explicit Storage, ComputeEnv, execution-contract, expression, and runtime dependencies without full Exec or service coupling.
- Targets: `DataSinkTablet`
- Allowed internal include prefixes: `data_sink/tablet/`, `storage/`, `storage_primitive/`, `compute_env/`, `exec_primitive/`, `exprs/`, `runtime/`, `platform/`, `column/`, `types/`, `common/`, `base/`, `gutil/`, `gen_cpp/`
- Allowed target deps: `Storage`, `StoragePrimitive`, `ComputeEnv`, `ExecPrimitive`, `Expr`, `Runtime`, `Platform`, `ChunkCore`, `ColumnCore`, `Types`, `Common`, `Base`, `Gutil`, `StarRocksGen`
- Core tests: `data_sink_tablet_test`
- Remediation: Keep tablet routing and write-channel behavior in DataSinkTablet over explicit Storage, ComputeEnv, and lower contracts; keep full Exec composition and service/bootstrap behavior outside this module.
<!-- END GENERATED: BE MODULE HARNESSES -->
