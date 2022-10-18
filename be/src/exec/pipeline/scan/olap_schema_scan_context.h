// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <unordered_map>

#include "common/object_pool.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace vectorized {
class SchemaScannerParam;
class SchemaScanner;
} // namespace vectorized

namespace pipeline {
class OlapSchemaScanContext;
using OlapSchemaScanContextPtr = std::shared_ptr<OlapSchemaScanContext>;

class OlapSchemaScanContext {
public:
    OlapSchemaScanContext(const TPlanNode& tnode, BalancedChunkBuffer& chunk_buffer)
            : _tnode(tnode),
              _table_name(tnode.schema_scan_node.table_name),
              _tuple_id(tnode.schema_scan_node.tuple_id),
              _chunk_buffer(chunk_buffer) {}

    ~OlapSchemaScanContext() = default;

    Status prepare(RuntimeState* state);

    BalancedChunkBuffer& get_chunk_buffer() { return _chunk_buffer; }

    TupleId tuple_id() { return _tuple_id; }

    ObjectPool* object_pool() { return &_obj_pool; }

    std::vector<ExprContext*>& conjunct_ctxs() { return _conjunct_ctxs; }

    vectorized::SchemaScannerParam* param() { return _param.get(); }

private:
    Status _prepare_params(RuntimeState* state);

    TPlanNode _tnode;
    std::string _table_name;
    TupleId _tuple_id;

    std::vector<ExprContext*> _conjunct_ctxs;
    std::shared_ptr<vectorized::SchemaScannerParam> _param;

    BalancedChunkBuffer& _chunk_buffer;
    ObjectPool _obj_pool;
};
} // namespace pipeline
} // namespace starrocks