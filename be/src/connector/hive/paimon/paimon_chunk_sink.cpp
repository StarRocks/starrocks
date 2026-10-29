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

#include "connector/hive/paimon/paimon_chunk_sink.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cctype>
#include <condition_variable>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <string_view>
#include <utility>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <fmt/format.h>
#include <paimon/bucket/bucket_id_calculator.h>
#include <paimon/catalog/table.h>
#include <paimon/commit_message.h>
#include <paimon/executor.h>
#include <paimon/file_store_write.h>
#include <paimon/record_batch.h>
#include <paimon/write_context.h>
#include <rapidjson/document.h>

#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/logging.h"
#include "common/thread/priority_thread_pool.hpp"
#include "connector/hive/paimon/paimon_file_system.h"
#include "connector/hive/paimon/paimon_write_column_converter.h"
#include "connector/hive/paimon/tracked_paimon_memory_pool.h"
#include "connector_primitive/sink_memory_manager.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {

Status native_status(const paimon::Status& status, std::string_view action) {
    if (status.ok()) return Status::OK();
    // SDK messages can contain filesystem paths, option values and credential-bearing URLs.
    // Keep the operation for correlation without propagating the untrusted message to FE/logs.
    const auto message = fmt::format("Paimon native {} failed", action);
    if (status.IsOutOfMemory() || status.IsCapacityError()) return Status::MemoryLimitExceeded(message);
    if (status.IsCancelled()) return Status::Cancelled(message);
    if (status.IsNotImplemented()) return Status::NotSupported(message);
    if (status.IsInvalid() || status.IsTypeError() || status.IsIndexError() || status.IsSerializationError()) {
        return Status::InvalidArgument(message);
    }
    return Status::IOError(message);
}

template <typename T>
StatusOr<T> native_result(paimon::Result<T>&& result, std::string_view action) {
    if (!result.ok()) return native_status(result.status(), action);
    return std::move(result).value();
}

template <typename T>
StatusOr<T> arrow_result(arrow::Result<T>&& result) {
    if (!result.ok()) {
        return result.status().IsOutOfMemory() ? Status::MemoryLimitExceeded("Paimon Arrow allocation failed")
                                               : Status::InvalidArgument("Paimon Arrow conversion failed");
    }
    return std::move(result).ValueOrDie();
}

Status arrow_status(const arrow::Status& status) {
    return status.ok() ? Status::OK() : Status::InvalidArgument("Paimon Arrow export failed");
}

// A per-driver hard allocation budget shared by the supplied native and Arrow pools. Native
// buffer options alone are not a bound (large values/rows can allocate more than one buffer).
class WriteMemoryBudget {
public:
    explicit WriteMemoryBudget(uint64_t limit) : _limit(limit) {}
    bool reserve(uint64_t bytes) {
        auto used = _used.load();
        do {
            if (used > _limit || bytes > _limit - used) return false;
        } while (!_used.compare_exchange_weak(used, used + bytes));
        return true;
    }
    void release(uint64_t bytes) { _used.fetch_sub(bytes); }

private:
    uint64_t _limit;
    std::atomic<uint64_t> _used{0};
};

class WriteNativePool final : public TrackedPaimonMemoryPool {
public:
    WriteNativePool(MemTracker* tracker, std::shared_ptr<WriteMemoryBudget> budget)
            : TrackedPaimonMemoryPool(tracker), _budget(std::move(budget)) {}
    void* Malloc(uint64_t size, uint64_t alignment) override {
        if (!_budget->reserve(size)) throw std::bad_alloc();
        try {
            return TrackedPaimonMemoryPool::Malloc(size, alignment);
        } catch (...) {
            _budget->release(size);
            throw;
        }
    }
    void* Realloc(void* pointer, size_t old_size, size_t new_size, uint64_t alignment) override {
        if (new_size > old_size && !_budget->reserve(new_size - old_size)) throw std::bad_alloc();
        try {
            auto* result = TrackedPaimonMemoryPool::Realloc(pointer, old_size, new_size, alignment);
            if (result == nullptr && new_size != 0) throw std::bad_alloc();
            if (new_size < old_size) _budget->release(old_size - new_size);
            return result;
        } catch (...) {
            if (new_size > old_size) _budget->release(new_size - old_size);
            throw;
        }
    }
    void Free(void* pointer, uint64_t size) override {
        TrackedPaimonMemoryPool::Free(pointer, size);
        _budget->release(size);
    }
    void Free(void* pointer, uint64_t size, uint64_t alignment) override {
        TrackedPaimonMemoryPool::Free(pointer, size, alignment);
        _budget->release(size);
    }

private:
    std::shared_ptr<WriteMemoryBudget> _budget;
};

// Arrow is the SDK's bundled copy here, never the executable's copy. Both pools explicitly fix
// the TLS tracker because native writes also allocate/free from executor threads.
class WriteArrowPool final : public arrow::MemoryPool {
public:
    WriteArrowPool(MemTracker* tracker, std::shared_ptr<WriteMemoryBudget> budget)
            : _tracker(tracker), _budget(std::move(budget)), _delegate(arrow::default_memory_pool()) {}
    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        if (size < 0 || !_budget->reserve(size)) return arrow::Status::OutOfMemory("Paimon writer memory bound");
        auto status = _delegate.Allocate(size, alignment, out);
        if (!status.ok()) _budget->release(size);
        return status;
    }
    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        if (new_size < 0 || old_size < 0) return arrow::Status::Invalid("Invalid allocation size");
        if (new_size > old_size && !_budget->reserve(new_size - old_size)) {
            return arrow::Status::OutOfMemory("Paimon writer memory bound");
        }
        auto status = _delegate.Reallocate(old_size, new_size, alignment, ptr);
        if (!status.ok() && new_size > old_size) _budget->release(new_size - old_size);
        if (status.ok() && new_size < old_size) _budget->release(old_size - new_size);
        return status;
    }
    void Free(uint8_t* ptr, int64_t size, int64_t alignment) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        _delegate.Free(ptr, size, alignment);
        _budget->release(size);
    }
    int64_t bytes_allocated() const override { return _delegate.bytes_allocated(); }
    int64_t max_memory() const override { return _delegate.max_memory(); }
    int64_t total_bytes_allocated() const override { return _delegate.total_bytes_allocated(); }
    int64_t num_allocations() const override { return _delegate.num_allocations(); }
    std::string backend_name() const override { return "paimon-write-tracked"; }

private:
    MemTracker* _tracker;
    std::shared_ptr<WriteMemoryBudget> _budget;
    arrow::ProxyMemoryPool _delegate;
};

Status validate_contract(const TPaimonTableSink& sink, bool writing) {
    if (!sink.__isset.writer_protocol_version || sink.writer_protocol_version != 1) {
        return Status::NotSupported("Paimon writer protocol must be 1; upgrade FE and BE together");
    }
    if (!sink.__isset.location || sink.location.empty() || !sink.__isset.table_identity ||
        sink.table_identity.empty() ||
        !sink.__isset.schema_id || sink.schema_id < 0 || !sink.__isset.schema_json || sink.schema_json.empty() ||
        !sink.__isset.column_names || sink.column_names.empty() || !sink.__isset.partition_columns ||
        !sink.__isset.bucket_columns || !sink.__isset.primary_key_columns || !sink.__isset.bucket_count) {
        return Status::InvalidArgument("Paimon sink is missing its frozen table/schema contract");
    }
    if (writing && (!sink.__isset.operation_id || sink.operation_id.empty() || !sink.__isset.commit_user ||
                    sink.commit_user.empty() || !sink.__isset.commit_identifier || !sink.__isset.overwrite_mode)) {
        return Status::InvalidArgument("Paimon sink is missing its operation/commit identity");
    }
    if (writing && sink.overwrite_mode != "APPEND" && sink.overwrite_mode != "FULL" &&
        sink.overwrite_mode != "STATIC" && sink.overwrite_mode != "DYNAMIC") {
        return Status::InvalidArgument("Unknown Paimon overwrite mode");
    }
    if (sink.bucket_count == 0 || sink.bucket_count < -1 ||
        (!sink.primary_key_columns.empty() && sink.bucket_count <= 0)) {
        return Status::NotSupported("Paimon writes require fixed PK buckets or fixed/unaware append buckets");
    }
    if (writing && sink.bucket_count > 0 &&
        (!sink.__isset.bucket_routed || !sink.bucket_routed || !sink.__isset.routing_bucket_slot_id)) {
        return Status::NotSupported("Paimon fixed-bucket writes require native pre-exchange bucket ownership routing");
    }
    for (const auto& name : sink.partition_columns) {
        if (!sink.primary_key_columns.empty() &&
            std::find(sink.primary_key_columns.begin(), sink.primary_key_columns.end(), name) ==
                    sink.primary_key_columns.end()) {
            return Status::NotSupported("Paimon cross-partition primary-key writes are not supported");
        }
    }
    return Status::OK();
}

// SDK Arrow erases CHAR/VARCHAR/BINARY length. Reattach those bounds from the already-validated
// complete native schema JSON, recursively, without rebuilding native field ids/nullability.
StatusOr<std::shared_ptr<arrow::Field>> add_length_bounds(const std::shared_ptr<arrow::Field>& field,
                                                        const rapidjson::Value& native_type) {
    auto result = field;
    if (native_type.IsString()) {
        std::string_view type(native_type.GetString(), native_type.GetStringLength());
        auto open = type.find('(');
        if (open != std::string_view::npos && field->type()->id() == arrow::Type::TIMESTAMP &&
            (type.substr(0, open) == "TIMESTAMP" || type.substr(0, open) == "TIMESTAMP_LTZ")) {
            const auto close = type.find(')', open);
            if (close == std::string_view::npos) return Status::InvalidArgument("Malformed Paimon timestamp precision");
            int32_t precision = -1;
            const auto text = type.substr(open + 1, close - open - 1);
            const auto parsed = std::from_chars(text.data(), text.data() + text.size(), precision);
            if (parsed.ec != std::errc() || parsed.ptr != text.data() + text.size() || precision < 0 || precision > 9) {
                return Status::InvalidArgument("Malformed Paimon timestamp precision");
            }
            auto metadata = field->metadata() ? field->metadata()->Copy() : std::make_shared<arrow::KeyValueMetadata>();
            RETURN_IF_ERROR(
                    arrow_status(metadata->Set("starrocks.paimon.timestamp.precision", std::to_string(precision))));
            result = field->WithMetadata(std::move(metadata));
        }
        if (open != std::string_view::npos &&
            (type.substr(0, open) == "CHAR" || type.substr(0, open) == "VARCHAR" ||
             type.substr(0, open) == "BINARY" || type.substr(0, open) == "VARBINARY")) {
            const auto close = type.find(')', open);
            int32_t length = 0;
            if (close == std::string_view::npos) return Status::InvalidArgument("Malformed Paimon length bound");
            auto text = type.substr(open + 1, close - open - 1);
            auto parsed = std::from_chars(text.data(), text.data() + text.size(), length);
            if (parsed.ec != std::errc() || parsed.ptr != text.data() + text.size() || length <= 0) {
                return Status::InvalidArgument("Malformed Paimon length bound");
            }
            auto metadata = field->metadata() ? field->metadata()->Copy() : std::make_shared<arrow::KeyValueMetadata>();
            RETURN_IF_ERROR(arrow_status(metadata->Set("starrocks.paimon.length", std::to_string(length))));
            result = field->WithMetadata(std::move(metadata));
        }
    } else if (native_type.IsObject()) {
        switch (field->type()->id()) {
        case arrow::Type::LIST: {
            if (!native_type.HasMember("element")) return Status::InvalidArgument("Missing Paimon array element");
            auto list = std::static_pointer_cast<arrow::ListType>(field->type());
            ASSIGN_OR_RETURN(auto element, add_length_bounds(list->value_field(), native_type["element"]));
            result = field->WithType(arrow::list(element));
            break;
        }
        case arrow::Type::MAP: {
            if (!native_type.HasMember("key") || !native_type.HasMember("value")) {
                return Status::InvalidArgument("Missing Paimon map key/value");
            }
            auto map = std::static_pointer_cast<arrow::MapType>(field->type());
            ASSIGN_OR_RETURN(auto key, add_length_bounds(map->key_field(), native_type["key"]));
            ASSIGN_OR_RETURN(auto value, add_length_bounds(map->item_field(), native_type["value"]));
            result = field->WithType(std::make_shared<arrow::MapType>(key, value, map->keys_sorted()));
            break;
        }
        case arrow::Type::STRUCT: {
            if (!native_type.HasMember("fields") || !native_type["fields"].IsArray()) break; // VARIANT extension.
            const auto& fields = native_type["fields"];
            if (fields.Size() != field->type()->num_fields())
                return Status::InvalidArgument("Paimon ROW arity mismatch");
            arrow::FieldVector children;
            for (size_t i = 0; i < fields.Size(); ++i) {
                if (!fields[i].IsObject() || !fields[i].HasMember("type")) {
                    return Status::InvalidArgument("Malformed Paimon ROW field");
                }
                ASSIGN_OR_RETURN(auto child, add_length_bounds(field->type()->field(i), fields[i]["type"]));
                children.push_back(std::move(child));
            }
            result = field->WithType(arrow::struct_(children));
            break;
        }
        default: break;
        }
    }
    return result;
}

struct FrozenSchema {
    std::shared_ptr<paimon::DataSchema> native;
    std::shared_ptr<arrow::Schema> arrow;
    std::vector<std::string> bucket_keys;
};

StatusOr<FrozenSchema> load_frozen_schema(const TPaimonTableSink& sink, const std::shared_ptr<paimon::FileSystem>& fs) {
    // Identifier is only the catalog-independent diagnostic identity; location is authoritative.
    ASSIGN_OR_RETURN(auto table,
                     native_result(paimon::Table::Create(fs, sink.location, paimon::Identifier(sink.table_identity)),
                                   "load table schema"));
    auto native = std::dynamic_pointer_cast<paimon::DataSchema>(table->LatestSchema());
    if (native == nullptr) return Status::NotSupported("Paimon sink target is not a data table");
    // Java 2.0 can relocate data directories; public C++ 0.3 does not implement that layout.
    // Reject it before routing or writer construction, rather than preparing files FE cannot own.
    const auto& native_options = native->Options();
    if (native_options.find("data-file.path-directory") != native_options.end()) {
        return Status::NotSupported("Paimon C++ 0.3 writes do not support data-file.path-directory");
    }
    auto external = native_options.find("data-file.external-paths");
    if (external != native_options.end() && !external->second.empty()) {
        return Status::NotSupported("Paimon native writes do not support external data paths");
    }
    // GetJsonSchema() reserializes Arrow types and loses native string/binary lengths in 0.3.
    // Compare the complete immutable native schema file instead; the ID comes from LatestSchema.
    std::string json;
    auto schema_path = sink.location + (sink.location.back() == '/' ? "" : "/") +
                       "schema/schema-" + std::to_string(native->Id());
    RETURN_IF_ERROR(native_status(fs->ReadFile(schema_path, &json), "read complete native schema"));
    rapidjson::Document expected;
    rapidjson::Document actual;
    expected.Parse(sink.schema_json.data(), sink.schema_json.size());
    actual.Parse(json.data(), json.size());
    if (expected.HasParseError() || actual.HasParseError() || !expected.IsObject() || !actual.IsObject()) {
        return Status::InvalidArgument("Invalid complete Paimon schema JSON");
    }
    // Java injects the table's own exact location as a redundant `path` option. Normalize only
    // that one derived item on both sides; a different path must never redirect the writer.
    for (auto* document : {&expected, &actual}) {
        if (document->HasMember("options") && (*document)["options"].IsObject()) {
            auto& options = (*document)["options"];
            auto path = options.FindMember("path");
            if (path != options.MemberEnd()) {
                if (!path->value.IsString() ||
                    std::string_view(path->value.GetString(), path->value.GetStringLength()) != sink.location) {
                    return Status::InvalidArgument("Paimon schema path differs from its frozen table location");
                }
                options.RemoveMember(path);
            }
        }
    }
    // Object member order/whitespace are not schema changes; arrays and every other native attribute are.
    if (native->Id() != sink.schema_id || expected != actual || native->FieldNames() != sink.column_names ||
        native->PartitionKeys() != sink.partition_columns || native->PrimaryKeys() != sink.primary_key_columns ||
        native->NumBuckets() != sink.bucket_count) {
        return Status::InvalidArgument("Paimon table schema/options changed since FE planning; retry the statement");
    }
    // Empty append bucket keys are intentional: Java hashes an empty BinaryRow, not all fields.
    auto bucket_keys = native->BucketKeys();
    if (bucket_keys != sink.bucket_columns) {
        return Status::InvalidArgument("Paimon native bucket keys differ from FE's frozen bucket keys");
    }
    ASSIGN_OR_RETURN(auto c_schema, native_result(native->GetArrowSchema(), "read Arrow schema"));
    ASSIGN_OR_RETURN(auto arrow_schema, arrow_result(arrow::ImportSchema(c_schema.get())));
    if (!expected.HasMember("fields") || !expected["fields"].IsArray() ||
        expected["fields"].Size() != arrow_schema->num_fields()) {
        return Status::InvalidArgument("Paimon schema JSON fields do not match its Arrow schema");
    }
    arrow::FieldVector fields;
    for (size_t i = 0; i < expected["fields"].Size(); ++i) {
        const auto& field = expected["fields"][i];
        if (!field.IsObject() || !field.HasMember("type"))
            return Status::InvalidArgument("Invalid Paimon schema field");
        ASSIGN_OR_RETURN(auto bounded, add_length_bounds(arrow_schema->field(i), field["type"]));
        fields.push_back(std::move(bounded));
    }
    auto result_schema = arrow::schema(std::move(fields), arrow_schema->metadata());
    for (const auto& name : sink.partition_columns) {
        const auto field = result_schema->GetFieldByName(name);
        if (field == nullptr) return Status::InvalidArgument("Missing Paimon partition field");
        switch (field->type()->id()) {
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::STRING:
        case arrow::Type::DATE32: break;
        default: return Status::NotSupported("Pinned Paimon SDK supports only boolean/integer/string/date partitions");
        }
    }
    return FrozenSchema{std::move(native), std::move(result_schema), std::move(bucket_keys)};
}

StatusOr<std::vector<int32_t>> bucket_ids(const TPaimonTableSink& sink, const FrozenSchema& schema,
                                         const std::shared_ptr<arrow::RecordBatch>& batch,
                                         const std::shared_ptr<paimon::MemoryPool>& pool) {
    std::vector<int32_t> ids(batch->num_rows(), 0);
    if (ids.empty() || sink.bucket_count == -1 || sink.bucket_count == 1) return ids;
    const auto function = schema.native->Options().find("bucket-function");
    const auto function_name = function == schema.native->Options().end() ? "default" : function->second;
    std::unique_ptr<paimon::BucketIdCalculator> calculator;
    if (function_name == "default" || function_name == "DEFAULT") {
        ASSIGN_OR_RETURN(calculator, native_result(paimon::BucketIdCalculator::Create(
                !sink.primary_key_columns.empty(), sink.bucket_count, pool), "create native bucket calculator"));
    } else if (function_name == "mod" || function_name == "MOD") {
        if (schema.bucket_keys.size() != 1) return Status::InvalidArgument("Paimon MOD bucket function needs one key");
        ASSIGN_OR_RETURN(auto key_type,
                         native_result(schema.native->GetFieldType(schema.bucket_keys[0]), "bucket key type"));
        ASSIGN_OR_RETURN(
                calculator,
                native_result(paimon::BucketIdCalculator::CreateMod(
                                      !sink.primary_key_columns.empty(), sink.bucket_count, key_type, pool),
                              "create native MOD bucket calculator"));
    } else if (function_name == "hive" || function_name == "HIVE") {
        std::vector<paimon::HiveFieldInfo> fields;
        for (const auto& name : schema.bucket_keys) {
            ASSIGN_OR_RETURN(auto type, native_result(schema.native->GetFieldType(name), "bucket key type"));
            const auto field = schema.arrow->GetFieldByName(name);
            if (field == nullptr) return Status::InvalidArgument("Missing Paimon Hive bucket key");
            if (field->type()->id() == arrow::Type::DECIMAL128) {
                const auto decimal = std::static_pointer_cast<arrow::Decimal128Type>(field->type());
                fields.emplace_back(type, decimal->precision(), decimal->scale());
            } else {
                fields.emplace_back(type);
            }
        }
        ASSIGN_OR_RETURN(
                calculator,
                native_result(paimon::BucketIdCalculator::CreateHive(
                                      !sink.primary_key_columns.empty(), sink.bucket_count, fields, pool),
                              "create native Hive bucket calculator"));
    } else {
        return Status::NotSupported("Unsupported Paimon native bucket-function");
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrow::FieldVector fields;
    for (const auto& name : schema.bucket_keys) {
        const auto index = batch->schema()->GetFieldIndex(name);
        if (index < 0) return Status::InvalidArgument("Missing Paimon bucket key " + name);
        arrays.push_back(batch->column(index));
        fields.push_back(batch->schema()->field(index));
    }
    auto keys = arrow::RecordBatch::Make(arrow::schema(fields), batch->num_rows(), arrays);
    ArrowArray c_array{};
    ArrowSchema c_schema{};
    RETURN_IF_ERROR(arrow_status(arrow::ExportRecordBatch(*keys, &c_array, &c_schema)));
    // CalculateBucketIds consumes and releases BOTH exports, including on SDK failures.
    RETURN_IF_ERROR(
            native_status(calculator->CalculateBucketIds(&c_array, &c_schema, ids.data()), "calculate native buckets"));
    return ids;
}

// The SDK accepts raw canonical values and performs partition path escaping itself. In particular,
// do not URL-encode values here (that would double-escape '/', '=', '%' and non-ASCII strings).
StatusOr<std::string> partition_value(const std::shared_ptr<arrow::Array>& array, int64_t row,
                                      const std::string& default_name) {
    if (array->IsNull(row)) return default_name;
    switch (array->type_id()) {
    case arrow::Type::BOOL:
        return std::string(std::static_pointer_cast<arrow::BooleanArray>(array)->Value(row) ? "true" : "false");
    case arrow::Type::INT8: return std::to_string(std::static_pointer_cast<arrow::Int8Array>(array)->Value(row));
    case arrow::Type::INT16: return std::to_string(std::static_pointer_cast<arrow::Int16Array>(array)->Value(row));
    case arrow::Type::INT32: return std::to_string(std::static_pointer_cast<arrow::Int32Array>(array)->Value(row));
    case arrow::Type::INT64: return std::to_string(std::static_pointer_cast<arrow::Int64Array>(array)->Value(row));
    case arrow::Type::DATE32: {
        ASSIGN_OR_RETURN(auto scalar, arrow_result(array->GetScalar(row)));
        return scalar->ToString(); // ISO yyyy-mm-dd; SDK parses DATE from this, even in legacy path mode.
    }
    case arrow::Type::STRING: {
        std::string value(std::static_pointer_cast<arrow::StringArray>(array)->GetView(row));
        if (value.empty() || std::all_of(value.begin(), value.end(), [](unsigned char c) { return std::isspace(c); })) {
            // Native 0.3 GeneratePartitionVector canonicalizes these to its NULL directory.
            // Never silently collapse a nonnull SQL string partition into the NULL partition.
            return Status::NotSupported("Pinned Paimon SDK cannot preserve empty/whitespace-only string partitions");
        }
        if (value == default_name) {
            return Status::InvalidArgument("Paimon partition string equals the reserved default partition name");
        }
        return value;
    }
    default:
        return Status::NotSupported("Pinned Paimon SDK does not support this partition type: " +
                                    array->type()->ToString());
    }
}

StatusOr<int64_t> positive_option(const std::map<std::string, std::string>& options, const char* name, int64_t fallback,
                                  int64_t maximum) {
    auto it = options.find(name);
    if (it == options.end()) return fallback;
    int64_t value = 0;
    auto parsed = std::from_chars(it->second.data(), it->second.data() + it->second.size(), value);
    if (parsed.ec != std::errc() || parsed.ptr != it->second.data() + it->second.size() || value <= 0 ||
        value > maximum) {
        return Status::InvalidArgument(std::string("Invalid Paimon writer bound: ") + name);
    }
    return value;
}

StatusOr<int64_t> memory_size(std::string text) {
    auto first = text.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return Status::InvalidArgument("Invalid Paimon write-buffer-size");
    text.erase(0, first);
    size_t end = 0;
    while (end < text.size() && std::isdigit(static_cast<unsigned char>(text[end]))) ++end;
    if (end == 0) return Status::InvalidArgument("Invalid Paimon write-buffer-size");
    int64_t value = 0;
    auto parsed = std::from_chars(text.data(), text.data() + end, value);
    std::string unit;
    for (size_t i = end; i < text.size(); ++i) {
        if (!std::isspace(static_cast<unsigned char>(text[i])))
            unit += std::tolower(static_cast<unsigned char>(text[i]));
    }
    int64_t multiplier = 1;
    if (unit == "k" || unit == "kb" || unit == "kibibytes") multiplier = 1024;
    else if (unit == "m" || unit == "mb" || unit == "mebibytes") multiplier = 1024 * 1024;
    else if (unit == "g" || unit == "gb" || unit == "gibibytes") multiplier = 1024L * 1024 * 1024;
    else if (unit == "t" || unit == "tb" || unit == "tebibytes") multiplier = 1024L * 1024 * 1024 * 1024;
    else if (!unit.empty() && unit != "b" && unit != "bytes")
        return Status::InvalidArgument("Invalid Paimon memory size unit");
    if (parsed.ec != std::errc() || value <= 0 || value > std::numeric_limits<int64_t>::max() / multiplier) {
        return Status::InvalidArgument("Invalid Paimon write-buffer-size");
    }
    return value * multiplier;
}

class NativeSinkMemoryManager final : public connector::SinkOperatorMemoryManager {
public:
    explicit NativeSinkMemoryManager(std::shared_ptr<std::atomic<int64_t>> occupied) : _occupied(std::move(occupied)) {}
    bool kill_victim() override { return false; } // PrepareCommit is one-shot, NEVER a memory-pressure flush.
    int64_t update_releasable_memory() override { return 0; }
    int64_t update_writer_occupied_memory() override { return _occupied->load(); }
    int64_t releasable_memory() const override { return 0; }
    int64_t writer_occupied_memory() const override { return _occupied->load(); }

private:
    std::shared_ptr<std::atomic<int64_t>> _occupied;
};

} // namespace

StatusOr<std::vector<int32_t>> calculate_paimon_bucket_ids_in_shim(
        const TPaimonTableSink& sink, const ChunkPtr& chunk, const std::vector<ExprContext*>& output_exprs) {
    try {
        RETURN_IF_ERROR(validate_contract(sink, false));
        if (chunk == nullptr || output_exprs.size() != sink.column_names.size()) {
            return Status::InvalidArgument("Paimon routing requires a full-schema chunk expression list");
        }
        ASSIGN_OR_RETURN(
                std::shared_ptr<FileSystem> fs,
                FileSystemFactory::CreateUniqueFromString(sink.location, FSOptions(&sink.cloud_configuration)));
        auto native_fs = std::make_shared<PaimonFileSystem>(fs.get(), DataCacheOptions{});
        ASSIGN_OR_RETURN(auto schema, load_frozen_schema(sink, native_fs));
        auto* tracker = CurrentThread::mem_tracker();
        ASSIGN_OR_RETURN(auto max_memory, positive_option(sink.options, "starrocks.paimon.max-writer-memory-bytes",
                                                          256L * 1024 * 1024, 4L * 1024 * 1024 * 1024));
        auto budget = std::make_shared<WriteMemoryBudget>(max_memory);
        auto native_pool = std::make_shared<WriteNativePool>(tracker, budget);
        WriteArrowPool arrow_pool(tracker, budget);
        ASSIGN_OR_RETURN(auto batch, paimon_write_chunk_to_arrow(chunk, output_exprs, schema.arrow,
                                                                sink.time_zone, &arrow_pool));
        return bucket_ids(sink, schema, batch, native_pool);
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Paimon routing allocation failed");
    } catch (const std::exception&) {
        return Status::InternalError("Paimon native routing failed");
    }
}

namespace connector {

class PaimonChunkSink::Impl {
public:
    Impl(std::shared_ptr<PaimonChunkSinkContext> context, int32_t driver_id)
            : context(std::move(context)), driver_id(driver_id) {}
    ~Impl() {
        wait();
        // The operator owns the memory-manager registration; it may already be destroyed.
        memory = nullptr;
        try {
            if (!handed_off) cleanup();
            if (state() != nullptr) ExprExecutor::close(exprs, state());
        } catch (const std::exception&) {
            LOG(WARNING) << "Paimon unpublished writer cleanup failed during destruction";
        }
    }

    RuntimeState* state() const { return context ? context->runtime_state : nullptr; }

    Status init(RuntimeProfile* profile, SinkMemoryManager* manager) {
        if (!context || !state() || !context->executor || !manager || !profile) {
            return Status::InvalidArgument(
                    "Paimon sink requires runtime, bounded executor, profile and memory manager");
        }
        RETURN_IF_ERROR(validate_contract(context->sink, true));
        if (context->output_exprs.size() != context->sink.column_names.size()) {
            return Status::InvalidArgument("Paimon sink output must contain every native field in schema order");
        }
        if (paimon::CommitMessage::CurrentVersion() != 12) {
            return Status::NotSupported("Paimon writer requires pinned SDK commit serializer version 12");
        }
        this->profile = profile;
        profile->add_info_string("PaimonWriter", "native-cpp-0.3/protocol-1; FE publishes snapshots");
        profile->add_info_string("PaimonWriteOnly", "true (operation-local; existing files are never compacted)");
        profile->add_info_string("PaimonCommitPhase", "WRITING");
        write_time = ADD_TIMER(profile, "WriteFileTime");
        prepare_time = ADD_TIMER(profile, "CommitFileTime");
        convert_time = ADD_TIMER(profile, "PaimonArrowConvertTime");
        writers_counter = ADD_COUNTER(profile, "PartitionWriters", TUnit::UNIT);
        files_counter = ADD_COUNTER(profile, "WriteFiles", TUnit::UNIT);
        rows_counter = ADD_COUNTER(profile, "WriteFileRecords", TUnit::UNIT);
        bytes_counter = ADD_COUNTER(profile, "WriteFileBytes", TUnit::BYTES);
        COUNTER_SET(ADD_COUNTER(profile, "NativeBackgroundThreads", TUnit::UNIT), 5);
        profile->add_info_string("NativeBackgroundThreadsMeaning",
                                 "per-driver bound: SDK compaction 4 + supplied executor 1");
        auto memory_manager = std::make_unique<NativeSinkMemoryManager>(occupied);
        memory = memory_manager.get();
        manager->register_child_manager(std::move(memory_manager));
        ASSIGN_OR_RETURN(max_writers,
                         positive_option(context->sink.options, "starrocks.paimon.max-active-writers", 64, 4096));
        ASSIGN_OR_RETURN(max_memory, positive_option(context->sink.options, "starrocks.paimon.max-writer-memory-bytes",
                                                     256L * 1024 * 1024, 4L * 1024 * 1024 * 1024));
        profile->add_info_string("PaimonMaxActiveWriters", std::to_string(max_writers));
        ASSIGN_OR_RETURN(fs, FileSystemFactory::CreateUniqueFromString(context->sink.location,
                                                                       FSOptions(&context->sink.cloud_configuration)));
        files = std::make_shared<PaimonWriteFileTracker>(fs.get());
        native_fs = PaimonFileSystem::create_for_write(files);
        ASSIGN_OR_RETURN(schema, load_frozen_schema(context->sink, native_fs));
        if (max_memory < 2L * 1024 * 1024) {
            return Status::InvalidArgument("Paimon per-driver writer memory bound must be at least 2 MiB");
        }
        auto budget = std::make_shared<WriteMemoryBudget>(max_memory);
        native_pool = std::make_shared<WriteNativePool>(state()->query_mem_tracker_ptr().get(), budget);
        arrow_pool = std::make_unique<WriteArrowPool>(state()->query_mem_tracker_ptr().get(), budget);
        // Do not use a global native executor. Public 0.3 also creates a fixed four-thread
        // compaction executor per writer, even in write-only mode; profile includes those four.
        ASSIGN_OR_RETURN(native_executor,
                         native_result(paimon::CreateDefaultExecutor(1), "create bounded native executor"));
        RETURN_IF_ERROR(ExprFactory::create_expr_trees(&expr_pool, context->output_exprs, &exprs, state()));
        RETURN_IF_ERROR(ExprExecutor::prepare(exprs, state()));
        RETURN_IF_ERROR(ExprExecutor::open(exprs, state()));
        initialized = true;
        return Status::OK();
    }

    Status ensure_writer() {
        if (writer) return Status::OK();
        // FileStoreWrite::Create reopens LatestSchema itself. Check before AND after its creation,
        // and again before first Write, so it cannot silently adopt an altered native schema.
        ASSIGN_OR_RETURN(auto fresh, load_frozen_schema(context->sink, native_fs));
        std::map<std::string, std::string> options = schema.native->Options();
        for (const auto& [key, value] : context->sink.options) {
            if (key.rfind("starrocks.paimon.", 0) == 0) continue;
            // Java's table constructor injects its exact location into operation options.
            // Never allow a different path to redirect native data files.
            if (key == "path" && value == context->sink.location) continue;
            auto it = options.find(key);
            if (it == options.end() || it->second != value) {
                return Status::InvalidArgument("Paimon operation options may not override frozen table options");
            }
        }
        int64_t buffer = 64L * 1024 * 1024;
        auto it = options.find("write-buffer-size");
        if (it != options.end()) {
            ASSIGN_OR_RETURN(buffer, memory_size(it->second));
        }
        buffer = std::min(buffer, max_memory / 2);
        options["write-buffer-size"] = std::to_string(buffer) + " b";
        options["write-only"] = "true";
        options["write-buffer-spillable"] = "false"; // No uncontrolled native spill threads/temp paths.
        profile->add_info_string("PaimonEffectiveWriteBufferBytes", std::to_string(buffer));
        profile->add_info_string("PaimonWriterMemoryLimitBytes", std::to_string(max_memory));
        paimon::WriteContextBuilder builder(context->sink.location, context->sink.commit_user);
        builder.WithMemoryPool(native_pool)
                .WithFileSystem(native_fs)
                .WithExecutor(native_executor)
                .WithIgnorePreviousFiles(context->sink.overwrite_mode != "APPEND" ||
                                         context->sink.primary_key_columns.empty())
                .WithStreamingMode(false)
                .SetWriteBufferSpillThreadNumber(0)
                .SetOptions(options);
        ASSIGN_OR_RETURN(auto write_context, native_result(builder.Finish(), "build write context"));
        ASSIGN_OR_RETURN(writer,
                         native_result(paimon::FileStoreWrite::Create(std::move(write_context)), "create writer"));
        ASSIGN_OR_RETURN(fresh, load_frozen_schema(context->sink, native_fs));
        return Status::OK();
    }

    Status add(const ChunkPtr& chunk) {
        RETURN_IF_ERROR(get_status());
        if (!initialized || finishing) return Status::InvalidArgument("Paimon add outside the writing phase");
        if (!chunk || chunk->num_rows() == 0) return Status::OK();
        RETURN_IF_CANCELLED(state());
        RETURN_IF_ERROR(ensure_writer());
        std::shared_ptr<arrow::RecordBatch> batch;
        {
            SCOPED_TIMER(convert_time);
            ASSIGN_OR_RETURN(batch, paimon_write_chunk_to_arrow(chunk, exprs, schema.arrow,
                                                                context->sink.time_zone, arrow_pool.get()));
        }
        RETURN_IF_ERROR(check_memory());
        ASSIGN_OR_RETURN(auto ids, bucket_ids(context->sink, schema, batch, native_pool));
        if (context->sink.bucket_count > 0) {
            const auto slot = context->sink.routing_bucket_slot_id;
            if (!chunk->is_slot_exist(slot)) return Status::InvalidArgument("Paimon routed bucket column is missing");
            const auto& route = chunk->get_column_by_slot_id(slot);
            const auto* data = ColumnHelper::get_data_column(route.get());
            if (dynamic_cast<const Int32Column*>(data) == nullptr)
                return Status::InvalidArgument("Paimon bucket route must be INT");
            for (size_t row = 0; row < ids.size(); ++row) {
                const auto value = route->get(row);
                if (value.is_null() || value.get_int32() != ids[row]) {
                    return Status::InvalidArgument("Paimon native bucket differs from pre-exchange routing");
                }
            }
        }
        using Group = std::pair<std::map<std::string, std::string>, int32_t>;
        std::vector<Group> row_groups;
        if (batch->num_rows() > max_memory / static_cast<int64_t>(sizeof(Group))) {
            return Status::MemoryLimitExceeded("Paimon routing groups exceed the per-driver writer bound");
        }
        row_groups.reserve(batch->num_rows());
        auto all_groups = active_groups;
        std::string default_name = "__DEFAULT_PARTITION__";
        auto default_it = schema.native->Options().find("partition.default-name");
        if (default_it != schema.native->Options().end()) default_name = default_it->second;
        if (default_name.empty()) return Status::InvalidArgument("Paimon default partition name cannot be empty");
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            std::map<std::string, std::string> partition;
            for (const auto& name : context->sink.partition_columns) {
                const auto index = batch->schema()->GetFieldIndex(name);
                if (index < 0) return Status::InvalidArgument("Paimon partition column is missing");
                ASSIGN_OR_RETURN(auto value, partition_value(batch->column(index), row, default_name));
                partition.emplace(name, std::move(value));
            }
            for (const auto& [name, value] : context->sink.static_partition) {
                auto it = partition.find(name);
                if (it == partition.end() || it->second != value) {
                    return Status::InvalidArgument("Paimon input does not match the static partition specification");
                }
            }
            Group group{std::move(partition), ids[row]};
            all_groups.insert(group);
            if (all_groups.size() > max_writers) {
                return Status::MemoryLimitExceeded(
                        "Paimon max-active-writers exceeded; increase the per-driver bound or sink DOP");
            }
            row_groups.push_back(std::move(group));
        }
        // Group adjacent full rows without Arrow copies. Nonadjacent groups reuse the same SDK
        // partition/bucket writer; no first-row partition assumption and no loss of row ordering.
        if (row_count == 0) {
            ASSIGN_OR_RETURN(auto fresh, load_frozen_schema(context->sink, native_fs));
        }
        SCOPED_TIMER(write_time);
        for (int64_t start = 0; start < batch->num_rows();) {
            int64_t end = start + 1;
            while (end < batch->num_rows() && row_groups[end] == row_groups[start]) ++end;
            auto slice = batch->Slice(start, end - start);
            ArrowArray array{};
            RETURN_IF_ERROR(arrow_status(arrow::ExportRecordBatch(*slice, &array)));
            paimon::RecordBatchBuilder builder(&array); // Takes ownership of the C export.
            builder.SetPartition(row_groups[start].first).SetBucket(row_groups[start].second);
            ASSIGN_OR_RETURN(auto native_batch, native_result(builder.Finish(), "build full-row batch"));
            RETURN_IF_ERROR(native_status(writer->Write(std::move(native_batch)), "write full-row batch"));
            row_count += end - start;
            start = end;
            RETURN_IF_ERROR(check_memory());
        }
        active_groups = std::move(all_groups);
        COUNTER_SET(writers_counter, active_groups.size());
        return Status::OK();
    }

    Status check_memory() {
        const auto bytes = static_cast<int64_t>(native_pool->CurrentUsage()) + arrow_pool->bytes_allocated();
        occupied->store(bytes);
        if (bytes > max_memory)
            return Status::MemoryLimitExceeded("Paimon per-driver native writer memory bound exceeded");
        return state()->check_mem_limit("Paimon native writer");
    }

    Status prepare() {
        RETURN_IF_CANCELLED(state());
        RETURN_IF_ERROR(ensure_writer()); // Empty drivers must still validate schema and emit a terminal marker.
        ASSIGN_OR_RETURN(auto fresh, load_frozen_schema(context->sink, native_fs));
        SCOPED_TIMER(prepare_time);
        ASSIGN_OR_RETURN(auto messages, native_result(writer->PrepareCommit(), "prepare commit"));
        ASSIGN_OR_RETURN(auto serialized,
                         native_result(paimon::CommitMessage::SerializeList(messages, native_pool),
                                       "serialize commit list"));
        RETURN_IF_ERROR(native_status(writer->Close(), "close prepared writer"));
        writer.reset();
        native_executor->ShutdownNow();
        // Cancellation before handoff is still unambiguously unpublished. Once recorded below,
        // FE owns abort/commit: a later client cancellation must NEVER delete these data files.
        RETURN_IF_CANCELLED(state());
        TPaimonCommitInfo info;
        info.__set_operation_id(context->sink.operation_id);
        info.__set_producer_id(print_id(state()->fragment_instance_id()) + ":" + std::to_string(driver_id));
        info.__set_attempt_id(0);
        info.__set_serializer_version(paimon::CommitMessage::CurrentVersion());
        info.__set_commit_messages(serialized);
        info.__set_schema_id(context->sink.schema_id);
        info.__set_table_identity(context->sink.table_identity);
        info.__set_row_count(row_count);
        auto owned = files->owned_files();
        info.__set_owned_files(owned);
        info.__set_prepare_complete(true);
        int64_t bytes = 0;
        for (const auto& path : owned) {
            auto size = fs->get_file_size(path);
            if (!size.ok()) return Status::IOError("Paimon prepared file size lookup failed");
            bytes += size.value();
        }
        TSinkCommitInfo commit;
        commit.__set_paimon_commit_info(info);
        state()->add_sink_commit_info(commit);
        handed_off = true; // Deliberately NOT mark_published(): publication only happens in FE.
        state()->update_num_rows_load_sink(row_count);
        state()->update_num_bytes_load_sink(bytes);
        COUNTER_SET(files_counter, owned.size());
        COUNTER_SET(rows_counter, row_count);
        COUNTER_SET(bytes_counter, bytes);
        occupied->store(0);
        profile->add_info_string("PaimonCommitPhase", "PREPARED_FE_OWNS_COMMIT_OR_ABORT");
        return Status::OK();
    }

    Status finish() {
        if (finishing) return get_status();
        finishing = true; // Even an already-failed driver must leave pending_finish once cancelled.
        RETURN_IF_ERROR(get_status());
        if (!initialized) return Status::InvalidArgument("Paimon finish before initialization");
        {
            std::lock_guard<std::mutex> lock(mutex);
            pending = true;
        }
        bool accepted = false;
        try {
            accepted = context->executor->try_offer([this]() {
                DeferOp completion([this]() {
                    std::lock_guard<std::mutex> lock(mutex);
                    pending = false;
                    condition.notify_all(); // No callback access to this after releasing the lock.
                });
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state()->query_mem_tracker_ptr().get());
                Status result;
                try {
                    result = prepare();
                } catch (const std::bad_alloc&) {
                    result = Status::MemoryLimitExceeded("Paimon prepare allocation failed");
                } catch (const std::exception&) {
                    result = Status::InternalError("Paimon prepare failed");
                }
                if (!result.ok()) {
                    set_status(result);
                    try {
                        cleanup();
                        profile->add_info_string("PaimonCommitPhase", "FAILED_BE_ROLLBACK");
                    } catch (const std::exception&) {
                        // completion still drains pending_finish; destructor retries unpublished cleanup.
                        set_status(Status::IOError("Paimon unpublished cleanup failed"));
                    }
                }
            });
        } catch (const std::bad_alloc&) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                pending = false;
            }
            condition.notify_all();
            auto failure = Status::MemoryLimitExceeded("Cannot schedule Paimon prepare task");
            set_status(failure);
            return failure;
        } catch (const std::exception&) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                pending = false;
            }
            condition.notify_all();
            auto failure = Status::InternalError("Cannot schedule Paimon prepare task");
            set_status(failure);
            return failure;
        }
        if (!accepted) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                pending = false;
            }
            condition.notify_all();
            auto failure = Status::ServiceUnavailable("Paimon sink I/O executor queue is full or stopped");
            set_status(failure);
            return failure;
        }
        return Status::OK();
    }

    void wait() {
        std::unique_lock<std::mutex> lock(mutex);
        condition.wait(lock, [this]() { return !pending; });
    }
    bool finished() {
        std::lock_guard<std::mutex> lock(mutex);
        return finishing && !pending;
    }
    Status get_status() {
        std::lock_guard<std::mutex> lock(mutex);
        return failure;
    }
    void set_status(const Status& status) {
        std::lock_guard<std::mutex> lock(mutex);
        if (failure.ok()) failure = status;
    }
    void cleanup() {
        if (handed_off) return;
        if (writer) {
            auto status = native_status(writer->Close(), "close unpublished writer");
            writer.reset();
            if (!status.ok()) set_status(status);
        }
        if (native_executor) native_executor->ShutdownNow();
        if (files) {
            auto status = files->rollback();
            if (!status.ok()) {
                set_status(Status::IOError("Paimon unpublished-file rollback failed"));
                LOG(WARNING) << "Paimon unpublished-file rollback failed; retry cleanup through the attempt owner";
            }
        }
        occupied->store(0);
    }

    std::shared_ptr<PaimonChunkSinkContext> context;
    int32_t driver_id;
    ObjectPool expr_pool;
    std::vector<ExprContext*> exprs;
    std::shared_ptr<FileSystem> fs; // Own credential-configured SR filesystem for every borrowed SDK use.
    std::shared_ptr<PaimonWriteFileTracker> files;
    std::shared_ptr<PaimonFileSystem> native_fs;
    FrozenSchema schema;
    std::shared_ptr<paimon::MemoryPool> native_pool;
    std::unique_ptr<WriteArrowPool> arrow_pool;
    std::shared_ptr<paimon::Executor> native_executor;
    std::unique_ptr<paimon::FileStoreWrite> writer;
    NativeSinkMemoryManager* memory = nullptr;
    std::shared_ptr<std::atomic<int64_t>> occupied = std::make_shared<std::atomic<int64_t>>(0);
    RuntimeProfile* profile = nullptr;
    RuntimeProfile::Counter* write_time = nullptr;
    RuntimeProfile::Counter* prepare_time = nullptr;
    RuntimeProfile::Counter* convert_time = nullptr;
    RuntimeProfile::Counter* writers_counter = nullptr;
    RuntimeProfile::Counter* files_counter = nullptr;
    RuntimeProfile::Counter* rows_counter = nullptr;
    RuntimeProfile::Counter* bytes_counter = nullptr;
    std::set<std::pair<std::map<std::string, std::string>, int32_t>> active_groups;
    int64_t max_writers = 0;
    int64_t max_memory = 0;
    int64_t row_count = 0;
    bool initialized = false;
    bool finishing = false;
    bool handed_off = false; // Only read after waiting for the single finish task.
    std::mutex mutex;
    std::condition_variable condition;
    bool pending = false;
    Status failure;
};

PaimonChunkSink::PaimonChunkSink(std::shared_ptr<PaimonChunkSinkContext> context, int32_t driver_id)
        : _impl(std::make_unique<Impl>(std::move(context), driver_id)) {}
PaimonChunkSink::~PaimonChunkSink() = default;

Status PaimonChunkSink::init(formats::AsyncFlushStreamPoller*, RuntimeProfile* profile, SinkMemoryManager* manager) {
    try {
        auto result = _impl->init(profile, manager);
        if (!result.ok()) _impl->set_status(result);
        return result;
    } catch (const std::exception&) {
        auto result = Status::InternalError("Paimon writer initialization failed");
        _impl->set_status(result);
        return result;
    }
}
Status PaimonChunkSink::add(const ChunkPtr& chunk) {
    try {
        auto result = _impl->add(chunk);
        if (!result.ok()) _impl->set_status(result);
        return result;
    } catch (const std::bad_alloc&) {
        auto result = Status::MemoryLimitExceeded("Paimon write allocation failed");
        _impl->set_status(result);
        return result;
    } catch (const std::exception&) {
        auto result = Status::InternalError("Paimon native write failed");
        _impl->set_status(result);
        return result;
    }
}
Status PaimonChunkSink::finish() {
    try {
        return _impl->finish();
    } catch (const std::bad_alloc&) {
        auto result = Status::MemoryLimitExceeded("Paimon finish scheduling allocation failed");
        _impl->set_status(result);
        return result;
    }
}
void PaimonChunkSink::rollback() {
    _impl->wait();
    try {
        _impl->cleanup();
    } catch (const std::exception&) {
        _impl->set_status(Status::IOError("Paimon unpublished writer cleanup failed"));
    }
}
bool PaimonChunkSink::is_finished() { return _impl->finished(); }
Status PaimonChunkSink::status() { return _impl->get_status(); }
SinkOperatorMemoryManager* PaimonChunkSink::op_mem_mgr() const { return _impl->memory; }

} // namespace connector
} // namespace starrocks
