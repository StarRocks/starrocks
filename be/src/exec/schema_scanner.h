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

#include <string>

#include "column/chunk.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace starrocks {
// forehead declar class, because jni function init in StarRocksServer.
class StarRocksServer;
class RuntimeState;
} // namespace starrocks

namespace starrocks {

// scanner parameter from frontend
struct SchemaScannerParam {
    const std::string* catalog{nullptr};
    const std::string* db{nullptr};
    const std::string* table{nullptr};
    const std::string* wild{nullptr};
    const std::string* user{nullptr};
    const std::string* user_ip{nullptr};
    const TUserIdentity* current_user_ident{nullptr}; // to replace the user and user ip
    const std::string* ip{nullptr};                   // frontend ip
    int32_t port{0};                                  // frontend thrift port
    int64_t thread_id = 0;
    // set limit only when there is no predicate
    int64_t limit{0};
    // true only when there is no predicate and limit parameter is set,
    // if true, then for SchemaColumnsScanner, call describeTable() once,
    // and no longer call get_db_names() and get_table_names().
    bool without_db_table{false};

    const std::string* label = nullptr;
    int64_t job_id = -1;

    int64_t table_id{-1};
    int64_t partition_id{-1};
    int64_t tablet_id{-1};
    int64_t txn_id{-1};
    const std::string* type{nullptr};
    const std::string* state{nullptr};
    int64_t log_start_ts{-1};
    int64_t log_end_ts{-1};
    const std::string* log_level{nullptr};
    const std::string* log_pattern{nullptr};
    int64_t log_limit{-1};

    RuntimeProfile::Counter* _rpc_timer = nullptr;
    RuntimeProfile::Counter* _fill_chunk_timer = nullptr;

    SchemaScannerParam() = default;
};

// virtual scanner for all schema table
class SchemaScanner {
public:
    struct ColumnDesc {
        const char* name;
        LogicalType type;
        int size;
        bool is_null;
    };
    SchemaScanner(ColumnDesc* columns, int column_num);
    virtual ~SchemaScanner();

    // init object need information, schema etc.
    virtual Status init(SchemaScannerParam* param, ObjectPool* pool);
    // Start to work
    virtual Status start(RuntimeState* state);
    // Must only return one row at most each time
    virtual Status get_next(ChunkPtr* chunk, bool* eos);
    // factory function
    static std::unique_ptr<SchemaScanner> create(TSchemaTableType::type type);

    static void set_starrocks_server(StarRocksServer* starrocks_server) { _s_starrocks_server = starrocks_server; }

    const std::vector<SlotDescriptor*>& get_slot_descs() { return _slot_descs; }

protected:
    Status _create_slot_descs(ObjectPool* pool);

    bool _is_init;
    // this is used for sub class
    SchemaScannerParam* _param;
    // pointer to schema table's column desc
    ColumnDesc* _columns;
    int _column_num;

    std::vector<SlotDescriptor*> _slot_descs;

    static StarRocksServer* _s_starrocks_server;
    RuntimeState* _runtime_state = nullptr;
};

} // namespace starrocks
