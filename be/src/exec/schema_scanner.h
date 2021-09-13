// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/schema_scanner.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef STARROCKS_BE_SRC_QUERY_EXEC_SCHEMA_SCANNER_H
#define STARROCKS_BE_SRC_QUERY_EXEC_SCHEMA_SCANNER_H

#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"

namespace starrocks {

// forehead declar class, because jni function init in StarRocksServer.
class StarRocksServer;
class RuntimeState;

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

// scanner parameter from frontend
struct SchemaScannerParam {
    const std::string* db;
    const std::string* table;
    const std::string* wild;
    const std::string* user;                 // deprecated
    const std::string* user_ip;              // deprecated
    const TUserIdentity* current_user_ident; // to replace the user and user ip
    const std::string* ip;                   // frontend ip
    int32_t port;                            // frontend thrift port
    int64_t thread_id = 0;

    SchemaScannerParam()
            : db(NULL),
              table(NULL),
              wild(NULL),
              user(NULL),
              user_ip(NULL),
              current_user_ident(NULL),
              ip(NULL),
              port(0) {}
};

// virtual scanner for all schema table
class SchemaScanner {
public:
    struct ColumnDesc {
        const char* name;
        PrimitiveType type;
        int size;
        bool is_null;
    };
    SchemaScanner(ColumnDesc* columns, int column_num);
    virtual ~SchemaScanner();

    // init object need infomation, schema etc.
    virtual Status init(SchemaScannerParam* param, ObjectPool* pool);
    // Start to work
    virtual Status start(RuntimeState* state);
    virtual Status get_next_row(Tuple* tuple, MemPool* pool, bool* eos);
    // factory function
    static SchemaScanner* create(TSchemaTableType::type type);

    const TupleDescriptor* tuple_desc() const { return _tuple_desc; }

    static void set_starrocks_server(StarRocksServer* starrocks_server) { _s_starrocks_server = starrocks_server; }

protected:
    Status create_tuple_desc(ObjectPool* pool);

    bool _is_init;
    // this is used for sub class
    SchemaScannerParam* _param;
    // pointer to schema table's column desc
    ColumnDesc* _columns;
    // num of columns
    int _column_num;
    TupleDescriptor* _tuple_desc;

    static StarRocksServer* _s_starrocks_server;
};

} // namespace starrocks

#endif
