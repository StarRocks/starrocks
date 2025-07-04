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

#pragma once

#include <utility>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/olap_common.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exprs/expr_context.h"
#include "jni.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "udf/java/java_udf.h"

namespace starrocks {

struct RedisScanContext {
    std::string tbl_name;
    std::string db_name;
    std::string redis_url;
    std::string user;
    std::string passwd;
    std::string value_data_format;
    std::map<std::string, std::string> properties;
    std::vector<std::string> column_names;
    std::vector<std::string> column_types;
};

struct RedisScannerProfile {
    RuntimeProfile::Counter* rows_read_counter = nullptr;
    RuntimeProfile::Counter* io_timer = nullptr;
    RuntimeProfile::Counter* io_counter = nullptr;
    RuntimeProfile::Counter* fill_chunk_timer = nullptr;
};

class RedisScanner {
public:
    RedisScanner(RedisScanContext context, const TupleDescriptor* tuple_desc, RuntimeProfile* runtime_profile)
            : _scan_ctx(std::move(context)), _slot_descs(tuple_desc->slots()), _runtime_profile(runtime_profile) {}

    ~RedisScanner() = default;

    Status open(RuntimeState* state);

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos);

    Status close(RuntimeState* state);

private:
    void _init_profile();

    StatusOr<LogicalType> _precheck_data_type(const std::string& java_class, SlotDescriptor* slot_desc);

    Status _init_redis_bridge();

    jobject ConvertVectorToJavaList(JNIEnv* env, const vector<std::string>& vector);

    Status _init_redis_scan_context(RuntimeState* state);

    Status _init_redis_scanner();

    Status _init_column_class_name(RuntimeState* state);

    Status _has_next(bool* result);

    Status _get_next_chunk(jobject* chunk, size_t* num_rows);

    Status _fill_chunk(jobject jchunk, size_t num_rows, ChunkPtr* chunk);

    Status _close_redis_scanner();

    RedisScanContext _scan_ctx;
    // result column slot desc
    std::vector<SlotDescriptor*> _slot_descs;
    std::vector<LogicalType> _result_column_types;
    std::vector<ExprContext*> _cast_exprs;
    ChunkPtr _result_chunk;

    std::unique_ptr<JVMClass> _redis_scanner_cls;

    jmethodID _scanner_has_next;
    jmethodID _scanner_get_next_chunk;
    jmethodID _scanner_result_rows;
    jmethodID _scanner_close;

    JavaGlobalRef _redis_scan_context = nullptr;
    JavaGlobalRef _redis_scanner = nullptr;

    RuntimeProfile* _runtime_profile = nullptr;
    RedisScannerProfile _profile;

    ObjectPool _pool;

    static constexpr const char* REDIS_SCAN_CONTEXT_CLASS_NAME = "com/starrocks/redis/reader/RedisScanContext";
    static constexpr const char* REDIS_SCANNER_CLASS_NAME = "com/starrocks/redis/reader/RedisScanner";
};
} // namespace starrocks
