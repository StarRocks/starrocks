// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/merge_join_node.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXEC_MERGE_JOIN_NODE_H
#define STARROCKS_BE_SRC_QUERY_EXEC_MERGE_JOIN_NODE_H

#include <boost/thread.hpp>
#include <boost/unordered_set.hpp>
#include <string>

#include "exec/exec_node.h"
#include "gen_cpp/PlanNodes_types.h" // for TJoinOp
#include "runtime/row_batch.h"

namespace starrocks {

class MemPool;
class TupleRow;

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30
class MergeJoinNode : public ExecNode {
public:
    MergeJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    ~MergeJoinNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

protected:
    void debug_string(int indentation_level, std::stringstream* out) const;

private:
    // our equi-join predicates "<lhs> = <rhs>" are separated into
    // _left_exprs (over child(0)) and _right_exprs (over child(1))
    // check which expr is min
    std::vector<ExprContext*> _left_expr_ctxs;
    std::vector<ExprContext*> _right_expr_ctxs;

    // non-equi-join conjuncts from the JOIN clause
    std::vector<ExprContext*> _other_join_conjunct_ctxs;

    bool _eos = false; // if true, nothing left to return in get_next()

    struct ChildReaderContext {
        RowBatch batch;
        int row_idx;
        bool is_eos;
        TupleRow* current_row;
        ChildReaderContext(const RowDescriptor& desc, int batch_size, MemTracker* mem_tracker)
                : batch(desc, batch_size, mem_tracker), row_idx(0), is_eos(false), current_row(NULL) {}
    };
    // _left_batch must be cleared before calling get_next().  used cache child(0)'s data
    // _rigth_batch must be cleared before calling get_next().  used cache child(1)'s data
    // does not initialize all tuple ptrs in the row, only the ones that it
    // is responsible for.
    std::unique_ptr<ChildReaderContext> _left_child_ctx;
    std::unique_ptr<ChildReaderContext> _right_child_ctx;
    // _build_tuple_idx[i] is the tuple index of child(1)'s tuple[i] in the output row
    std::vector<int> _right_tuple_idx;
    int _right_tuple_size = 0;
    int _left_tuple_size = 0;
    RowBatch* _out_batch;

    typedef int (*CompareFn)(const void*, const void*);
    std::vector<CompareFn> _cmp_func;

    void create_output_row(TupleRow* out, TupleRow* left, TupleRow* right);
    Status compare_row(TupleRow* left_row, TupleRow* right_row, bool* is_lt);
    Status get_next_row(RuntimeState* state, TupleRow* out_row, bool* eos);
    Status get_input_row(RuntimeState* state, int child_idx);
};

} // namespace starrocks

#endif
