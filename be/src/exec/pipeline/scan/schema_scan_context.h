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

#include <unordered_map>

#include "common/object_pool.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class SchemaScannerParam;
class SchemaScanner;

namespace pipeline {
class SchemaScanContext;
using SchemaScanContextPtr = std::shared_ptr<SchemaScanContext>;

class SchemaScanContext {
public:
    SchemaScanContext(const TPlanNode& tnode, BalancedChunkBuffer& chunk_buffer)
            : _tnode(tnode),
              _table_name(tnode.schema_scan_node.table_name),
              _tuple_id(tnode.schema_scan_node.tuple_id),
              _chunk_buffer(chunk_buffer) {}

    ~SchemaScanContext() = default;

    Status prepare(RuntimeState* state);

    BalancedChunkBuffer& get_chunk_buffer() { return _chunk_buffer; }

    TupleId tuple_id() { return _tuple_id; }

    ObjectPool* const object_pool() { return &_obj_pool; }

    std::vector<ExprContext*>& conjunct_ctxs() { return _conjunct_ctxs; }

    SchemaScannerParam* param() { return _param.get(); }

private:
    Status _prepare_params(RuntimeState* state);

    TPlanNode _tnode;
    std::string _table_name;
    TupleId _tuple_id;

    std::vector<ExprContext*> _conjunct_ctxs;
    std::shared_ptr<SchemaScannerParam> _param;

    BalancedChunkBuffer& _chunk_buffer;
    ObjectPool _obj_pool;
};
} // namespace pipeline
} // namespace starrocks
