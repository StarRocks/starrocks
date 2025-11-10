// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Disabled components shim for macOS build
// Provides stub implementations for components disabled on macOS

#ifdef __APPLE__

#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// ============================================================================
// FORWARD DECLARATIONS AND BASIC TYPES
// ============================================================================

namespace starrocks {

// Status class
class Status {
public:
    Status() : _code(0) {}
    Status(int code, const std::string& msg) : _code(code), _msg(msg) {}

    static Status OK() { return Status(); }
    static Status NotSupported(const std::string& msg) { return Status(-1, "Not supported on macOS: " + msg); }
    static Status InternalError(const std::string& msg) { return Status(-1, "Internal error: " + msg); }

    std::string message() const { return _msg; }
    bool ok() const { return _code == 0; }

private:
    int _code;
    std::string _msg;
};

// Forward declarations
class RuntimeState;
class StreamLoadContext;
class Chunk;
class AggregatorParams;
class LoadChannel;
class MemTracker;
class RuntimeProfile;
class DataDir;
class Tablet;
class TabletMeta;
class Rowset;
class RowsetMeta;
class TabletSchema;
class ExecEnv;

struct TabletsChannelKey;
struct StorePath;

// Thrift types
class TAgentResult;
class TAgentTaskRequest;
class TSnapshotRequest;
class TAgentPublishRequest;
class TFinishTaskRequest;
class TBrokerScanRange;
class TPushReq;
class TReportExecStatusParams;
class TDataCacheMetrics;
class TExprNode;

// Arrow types
namespace arrow {
class Array;
}

// Variant/JSON types
class VariantMetadata;

// Lake namespace
namespace lake {
class TabletManager;
}

// Parquet/file scanner types
class ScannerCounter;
class SlotDescriptor;
class ConvertFuncTree;
class ArrowConvertContext;

// Data consumer types
class DataConsumer;
class DataConsumerGroup;

// Metrics types
class DataCacheMemMetrics;
class DataCacheDiskMetrics;

// IOProfiler types
class IOStatEntry;

// Snapshot types
class SnapshotMeta;
enum SnapshotTypePB { SNAPSHOT_TYPE_FULL = 0, SNAPSHOT_TYPE_INCREMENTAL = 1 };
class Version;

// OlapTuple
class OlapTuple;

// ============================================================================
// 1. GOOGLE STACK TRACE STUB (for gperftools in brpc)
// ============================================================================

} // namespace starrocks

// brpc is looking for a C++-mangled GetStackTrace symbol, not a C-style one.
// We define a simple C++ function here to satisfy the linker.
int GetStackTrace(void** result, int max_depth, int skip_count) {
    return 0;
}

namespace starrocks {

// ============================================================================
// 2. META_TOOL STUB
// ============================================================================

} // namespace starrocks

// meta_tool_main is called from starrocks_main.cpp
int meta_tool_main(int argc, char** argv) {
    std::cerr << "Error: meta_tool is not supported on macOS" << std::endl;
    return -1;
}

namespace starrocks {

// ============================================================================
// 3. S2 GEOMETRY FLAGS STUB
// ============================================================================

} // namespace starrocks

// S2 debug flag referenced in code but S2 is disabled
namespace fLB {
bool FLAGS_s2debug = false;
}

namespace starrocks {

// ============================================================================
// 4. LZO COMPRESSION STUB (ORC namespace)
// ============================================================================

} // namespace starrocks

// ORC LZO decompression - ORC is disabled on macOS
namespace orc {
void lzoDecompress(const char* inputAddress, const char* inputLimit, char* outputAddress, char* outputLimit) {
    throw std::runtime_error("LZO decompression not supported on macOS (ORC disabled)");
}
} // namespace orc

namespace starrocks {

// ============================================================================
// 5. THRIFT UUID STUBS - REMOVED
// ============================================================================

} // namespace starrocks

// The UUID stub code has been removed because it was causing ODR violations
// and breaking JSON parsing. The linker errors for UUID methods will be
// resolved by checking if they are actually needed in the codebase.
//
// If UUID link errors occur, we need to investigate which code paths require
// UUID support and either:
// 1. Disable/stub those specific code paths
// 2. Patch the thrift library to add proper UUID support
// 3. Use a different approach that doesn't violate ODR

namespace starrocks {

// ============================================================================
// 6. STARROCKS VERSION CONSTANTS
// ============================================================================

// Build version constants - defined in build_version.cc.in but may be missing
const char* STARROCKS_VERSION = "3.0.0-macOS";
const char* STARROCKS_COMMIT_HASH = "unknown";
const char* STARROCKS_BUILD_TIME = __DATE__ " " __TIME__;
const char* STARROCKS_BUILD_USER = "macos";
const char* STARROCKS_BUILD_HOST = "macos";
const char* STARROCKS_BUILD_ARCH = "arm64";
const char* STARROCKS_BUILD_DISTRO_ID = "macOS";

// ============================================================================
// 9. PARQUET SCANNER (Parquet disabled on macOS)
// ============================================================================

// Cow template forward declaration
template <typename T>
class Cow {
public:
    template <typename U>
    using ImmutPtr = std::shared_ptr<const U>;
};

class Column;

class ParquetScanner {
public:
    ParquetScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                   ScannerCounter* counter, bool schema_only = false);

    ~ParquetScanner();

    static Status convert_array_to_column(ConvertFuncTree* func_tree, size_t num_elements, const arrow::Array* array,
                                          Cow<Column>::ImmutPtr<Column>& column, size_t start_index, size_t end_index,
                                          std::vector<uint8_t>* null_data, ArrowConvertContext* context);
};

// Out-of-line definitions
ParquetScanner::ParquetScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                               ScannerCounter* counter, bool schema_only) {}

ParquetScanner::~ParquetScanner() {}

Status ParquetScanner::convert_array_to_column(ConvertFuncTree* func_tree, size_t num_elements,
                                               const arrow::Array* array, Cow<Column>::ImmutPtr<Column>& column,
                                               size_t start_index, size_t end_index, std::vector<uint8_t>* null_data,
                                               ArrowConvertContext* context) {
    return Status::NotSupported("ParquetScanner not available on macOS");
}

// ============================================================================
// 10. DATA CONSUMER POOL (Kafka/Pulsar disabled on macOS)
// ============================================================================

class DataConsumerPool {
public:
    Status get_consumer(StreamLoadContext* ctx, std::shared_ptr<DataConsumer>* consumer);

    void return_consumer(const std::shared_ptr<DataConsumer>& consumer);

    Status get_consumer_grp(StreamLoadContext* ctx, std::shared_ptr<DataConsumerGroup>* consumer_grp);

    void return_consumers(DataConsumerGroup* consumer_grp);

    void start_bg_worker();

    void stop();
};

// Out-of-line definitions
Status DataConsumerPool::get_consumer(StreamLoadContext* ctx, std::shared_ptr<DataConsumer>* consumer) {
    return Status::NotSupported("DataConsumerPool not available on macOS");
}

void DataConsumerPool::return_consumer(const std::shared_ptr<DataConsumer>& consumer) {
    // No-op
}

Status DataConsumerPool::get_consumer_grp(StreamLoadContext* ctx, std::shared_ptr<DataConsumerGroup>* consumer_grp) {
    return Status::NotSupported("DataConsumerPool not available on macOS");
}

void DataConsumerPool::return_consumers(DataConsumerGroup* consumer_grp) {
    // No-op
}

void DataConsumerPool::start_bg_worker() {
    // No-op
}

void DataConsumerPool::stop() {
    // No-op
}

// ============================================================================
// 11. KAFKA DATA CONSUMER (Kafka disabled on macOS)
// ============================================================================

class KafkaDataConsumer {
public:
    Status get_partition_meta(std::vector<int32_t>* partition_ids, int timeout_ms);

    Status get_partition_offset(std::vector<int32_t>* partition_ids, std::vector<int64_t>* beginning_offsets,
                                std::vector<int64_t>* latest_offsets, int timeout_ms);

    Status commit(const std::string& group_id, const std::map<int32_t, int64_t>& offset_map);
};

// Out-of-line definitions
Status KafkaDataConsumer::get_partition_meta(std::vector<int32_t>* partition_ids, int timeout_ms) {
    return Status::NotSupported("Kafka not available on macOS");
}

Status KafkaDataConsumer::get_partition_offset(std::vector<int32_t>* partition_ids,
                                               std::vector<int64_t>* beginning_offsets,
                                               std::vector<int64_t>* latest_offsets, int timeout_ms) {
    return Status::NotSupported("Kafka not available on macOS");
}

Status KafkaDataConsumer::commit(const std::string& group_id, const std::map<int32_t, int64_t>& offset_map) {
    return Status::NotSupported("Kafka not available on macOS");
}

// ============================================================================
// 12. PULSAR DATA CONSUMER (Pulsar disabled on macOS)
// ============================================================================

// Forward declaration for pulsar MessageId
namespace pulsar {
class MessageId;
}

class PulsarDataConsumer {
public:
    Status assign_partition(const std::string& partition, StreamLoadContext* ctx, int64_t initial_position);

    Status get_topic_partition(std::vector<std::string>* partitions);

    Status get_partition_backlog(int64_t* backlog);

    void acknowledge_cumulative(pulsar::MessageId& message_id);
};

// Out-of-line definitions
Status PulsarDataConsumer::assign_partition(const std::string& partition, StreamLoadContext* ctx,
                                            int64_t initial_position) {
    return Status::NotSupported("Pulsar not available on macOS");
}

Status PulsarDataConsumer::get_topic_partition(std::vector<std::string>* partitions) {
    return Status::NotSupported("Pulsar not available on macOS");
}

Status PulsarDataConsumer::get_partition_backlog(int64_t* backlog) {
    return Status::NotSupported("Pulsar not available on macOS");
}

void PulsarDataConsumer::acknowledge_cumulative(pulsar::MessageId& message_id) {
    // No-op
}

// ============================================================================
// 14. FILE RESULT WRITER (File export disabled on macOS)
// ============================================================================

class ExprContext;
class ResultFileOptions;

class FileResultWriter {
public:
    FileResultWriter(const ResultFileOptions* options, const std::vector<ExprContext*>& output_expr_ctxs,
                     RuntimeProfile* parent_profile);

    ~FileResultWriter();

    Status init(RuntimeState* state);
    Status open(RuntimeState* state);
    Status append_chunk(Chunk* chunk);
    Status close();
};

// Out-of-line definitions
FileResultWriter::FileResultWriter(const ResultFileOptions* options, const std::vector<ExprContext*>& output_expr_ctxs,
                                   RuntimeProfile* parent_profile) {}

FileResultWriter::~FileResultWriter() {}

Status FileResultWriter::init(RuntimeState* state) {
    return Status::NotSupported("FileResultWriter not available on macOS");
}

Status FileResultWriter::open(RuntimeState* state) {
    return Status::NotSupported("FileResultWriter not available on macOS");
}

Status FileResultWriter::append_chunk(Chunk* chunk) {
    return Status::NotSupported("FileResultWriter not available on macOS");
}

Status FileResultWriter::close() {
    return Status::OK();
}

// ============================================================================
// 15. MYSQL TABLE WRITER (MySQL table sink disabled on macOS)
// ============================================================================

class MysqlTableWriter {
public:
    ~MysqlTableWriter();
};

// Out-of-line definitions
MysqlTableWriter::~MysqlTableWriter() {}

// ============================================================================
// 16. PYTHON ENV MANAGER (Python runtime disabled on macOS)
// ============================================================================

class PythonEnvManager {
public:
    void start_background_cleanup_thread();
    void close();
    ~PythonEnvManager();
};

// Out-of-line definitions
void PythonEnvManager::start_background_cleanup_thread() {
    // No-op
}

void PythonEnvManager::close() {
    // No-op
}

PythonEnvManager::~PythonEnvManager() {}

// ============================================================================
// 17. JDBC DRIVER MANAGER (JDBC disabled on macOS)
// ============================================================================

class JDBCDriverManager {
public:
    static JDBCDriverManager* getInstance();
    Status init(const std::string& driver_path);
};

// Out-of-line definitions
JDBCDriverManager* JDBCDriverManager::getInstance() {
    static JDBCDriverManager inst;
    return &inst;
}

Status JDBCDriverManager::init(const std::string& driver_path) {
    return Status::NotSupported("JDBC not available on macOS");
}

// ============================================================================
// 18. JAVA FUNCTION (Java UDF/UDAF/UDTF disabled on macOS)
// ============================================================================

// TFunctionBinaryType is already defined in Types_types.h

// Forward declarations
// LogicalType is an enum, not a class - already defined in logical_type.h
class FunctionContext;
struct PyFunctionDescriptor;

// Get Java UDAF function stub
void* getJavaUDAFFunction(bool is_analytic) {
    return nullptr;
}

// Get Java UDTF function stub
void* getJavaUDTFFunction() {
    return nullptr;
}

// Init UDAF context stub
Status init_udaf_context(int64_t fid, const std::string& url, const std::string& checksum, const std::string& symbol,
                         FunctionContext* context) {
    return Status::NotSupported("Java UDAF not available on macOS");
}

// Python UDF stub
Status build_py_call_stub(FunctionContext* context, const PyFunctionDescriptor& desc) {
    return Status::NotSupported("Python UDF not available on macOS");
}

class JavaFunctionCallExpr {
public:
    JavaFunctionCallExpr(const TExprNode& node);
    ~JavaFunctionCallExpr();
};

// Out-of-line definitions
JavaFunctionCallExpr::JavaFunctionCallExpr(const TExprNode& node) {}
JavaFunctionCallExpr::~JavaFunctionCallExpr() {}

// ============================================================================
// 19. UTILITY FUNCTIONS (Various helper functions)
// ============================================================================

// Execute script stub
Status execute_script(const std::string& script, std::string& output) {
    return Status::NotSupported("Script execution not available on macOS");
}

// Get array info stub - takes string_view
std::string get_array_info(std::string_view array_data) {
    return "";
}

// Get object info stub - takes string_view
std::string get_object_info(std::string_view object_data) {
    return "";
}

// Variant metadata stub
class VariantMetadata {
public:
    VariantMetadata(std::string_view data);
};

// Out-of-line definition
VariantMetadata::VariantMetadata(std::string_view data) {}

// ============================================================================
// 22. DATA CACHE UTILITIES
// ============================================================================

class BlockCache {
public:
    static Status read(const std::string& cache_key, int64_t offset, size_t size, class IOBuffer* buffer,
                       struct DiskCacheReadOptions* options);
};

// Out-of-line definitions
Status BlockCache::read(const std::string& cache_key, int64_t offset, size_t size, class IOBuffer* buffer,
                        struct DiskCacheReadOptions* options) {
    return Status::NotSupported("BlockCache not available on macOS");
}

class DataCacheUtils {
public:
    static void set_metrics_to_thrift(TDataCacheMetrics& metrics, const DataCacheMemMetrics& mem_metrics);

    static void set_metrics_to_thrift(TDataCacheMetrics& metrics, const DataCacheDiskMetrics& disk_metrics);
};

// Out-of-line definitions
void DataCacheUtils::set_metrics_to_thrift(TDataCacheMetrics& metrics, const DataCacheMemMetrics& mem_metrics) {
    // No-op
}

void DataCacheUtils::set_metrics_to_thrift(TDataCacheMetrics& metrics, const DataCacheDiskMetrics& disk_metrics) {
    // No-op
}

// ============================================================================
// 23. STORAGE PAGE CACHE
// ============================================================================

class PageCacheHandle;
struct MemCacheWriteOptions;

class StoragePageCache {
public:
    Status insert(const std::string& key, std::vector<uint8_t>* data, const MemCacheWriteOptions& options,
                  PageCacheHandle* handle);

    bool lookup(const std::string& key, PageCacheHandle* handle);
};

// Out-of-line definitions
Status StoragePageCache::insert(const std::string& key, std::vector<uint8_t>* data, const MemCacheWriteOptions& options,
                                PageCacheHandle* handle) {
    return Status::NotSupported("StoragePageCache not available on macOS");
}

bool StoragePageCache::lookup(const std::string& key, PageCacheHandle* handle) {
    return false;
}

// ============================================================================
// 24. LAKE META SCAN NODE (Lake storage disabled on macOS)
// ============================================================================

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class ExecNode;

class LakeMetaScanNode {
public:
    LakeMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& desc_tbl);
};

// Out-of-line definitions
LakeMetaScanNode::LakeMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& desc_tbl) {}

// ============================================================================
// 25. INVERTED INDEX (CLucene disabled on macOS)
// ============================================================================
// NOTE: InvertedIndexIterator is defined in inverted_index_stub.cpp to avoid duplication

// ============================================================================
// 26. STORAGE PAGE CACHE METRICS
// ============================================================================

class StoragePageCacheMetrics {
public:
    static std::atomic<uint64_t> released_page_handle_count;
};

// Define the static member
std::atomic<uint64_t> StoragePageCacheMetrics::released_page_handle_count{0};

// ============================================================================
// 27. DATA CACHE
// ============================================================================

class DataCache {
public:
    static DataCache* GetInstance();
};

// Out-of-line definition
DataCache* DataCache::GetInstance() {
    static DataCache instance;
    return &instance;
}

// ============================================================================
// 28. LAKE SERVICE RECOVERABLE STUB
// ============================================================================

namespace butil {
struct EndPoint {
    std::string ip;
    int port = 0;
};
} // namespace butil

class LakeService_RecoverableStub {
public:
    LakeService_RecoverableStub() = default;
    LakeService_RecoverableStub(const butil::EndPoint& endpoint, const std::string& service_name) {}
    ~LakeService_RecoverableStub() = default;
};

// ============================================================================
// 29. LAKE TABLET READER/MANAGER
// ============================================================================

class SeekRange;

class TabletReaderParams {
public:
    enum RangeStartOperation { GT = 0, GE = 1 };

    enum RangeEndOperation { LT = 0, LE = 1 };
};

namespace lake {

class TabletReader {
public:
    static Status parse_seek_range(const TabletSchema& schema, TabletReaderParams::RangeStartOperation start_op,
                                   TabletReaderParams::RangeEndOperation end_op,
                                   const std::vector<OlapTuple>& start_keys, const std::vector<OlapTuple>& end_keys,
                                   std::vector<SeekRange>* ranges, class MemPool* pool);
};

// Out-of-line definitions
Status TabletReader::parse_seek_range(const TabletSchema& schema, TabletReaderParams::RangeStartOperation start_op,
                                      TabletReaderParams::RangeEndOperation end_op,
                                      const std::vector<OlapTuple>& start_keys, const std::vector<OlapTuple>& end_keys,
                                      std::vector<SeekRange>* ranges, class MemPool* pool) {
    return Status::NotSupported("Lake storage not available on macOS");
}

class TabletMetadata;
class FileSystem;

class TabletManager {
public:
    std::shared_ptr<const TabletMetadata> get_tablet_metadata(int64_t tablet_id, int64_t version, bool fill_cache,
                                                              int64_t timeout_us,
                                                              const std::shared_ptr<FileSystem>& fs);
};

// Out-of-line definitions
std::shared_ptr<const TabletMetadata> TabletManager::get_tablet_metadata(int64_t tablet_id, int64_t version,
                                                                         bool fill_cache, int64_t timeout_us,
                                                                         const std::shared_ptr<FileSystem>& fs) {
    return nullptr;
}

} // namespace lake

// ============================================================================
// 30. PINTERNALSERVICEIMPLBASE TEMPLATE
// ============================================================================

class PInternalService;
class PFetchDataCacheRequest;
class PFetchDataCacheResponse;

namespace google {
namespace protobuf {
class RpcController;
class Closure;
} // namespace protobuf
} // namespace google

template <typename T>
class PInternalServiceImplBase {
public:
    void _fetch_datacache(google::protobuf::RpcController* controller, const PFetchDataCacheRequest* request,
                          PFetchDataCacheResponse* response, google::protobuf::Closure* done);
};

// Out-of-line template definition
template <typename T>
void PInternalServiceImplBase<T>::_fetch_datacache(google::protobuf::RpcController* controller,
                                                   const PFetchDataCacheRequest* request,
                                                   PFetchDataCacheResponse* response, google::protobuf::Closure* done) {
    // No-op - data cache not supported on macOS
}

// Explicit instantiation for PInternalService
template class PInternalServiceImplBase<PInternalService>;

} // namespace starrocks

#endif // __APPLE__
