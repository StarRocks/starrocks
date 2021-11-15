// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include <memory>
#include <vector>

#include "exec/vectorized/hash_joiner.h"
namespace starrocks {
namespace pipeline {
using HashJoiner = starrocks::vectorized::HashJoiner;
using HashJoinerPtr = std::shared_ptr<HashJoiner>;
using HashJoiners = std::vector<HashJoinerPtr>;
class HashJoinerFactory;
using HashJoinerFactoryPtr = std::shared_ptr<HashJoinerFactory>;
class HashJoinerFactory {
public:
    HashJoinerFactory(const THashJoinNode& hash_join_node, TPlanNodeId node_id, TPlanNodeType::type node_type,
                      int64_t limit, std::vector<bool>&& is_null_safes, std::vector<ExprContext*> build_expr_ctxs,
                      std::vector<ExprContext*> probe_expr_ctxs, std::vector<ExprContext*>&& other_join_conjunct_ctxs,
                      std::vector<ExprContext*>&& conjunct_ctxs, const RowDescriptor& build_row_descriptor,
                      const RowDescriptor& probe_row_descriptor, const RowDescriptor& row_descriptor, int dop)
            : _hash_join_node(hash_join_node),
              _node_id(node_id),
              _node_type(node_type),
              _limit(limit),
              _is_null_safes(std::move(is_null_safes)),
              _build_expr_ctxs(std::move(build_expr_ctxs)),
              _probe_expr_ctxs(std::move(probe_expr_ctxs)),
              _other_join_conjunct_ctxs(std::move(other_join_conjunct_ctxs)),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _build_row_descriptor(build_row_descriptor),
              _probe_row_descriptor(probe_row_descriptor),
              _row_descriptor(row_descriptor),
              _hash_joiners(dop) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state);

    HashJoinerPtr create(int i) {
        if (!_hash_joiners[i]) {
            _hash_joiners[i] = std::make_shared<HashJoiner>(
                    _hash_join_node, _node_id, _node_type, _limit, _is_null_safes, _build_expr_ctxs, _probe_expr_ctxs,
                    _other_join_conjunct_ctxs, _conjunct_ctxs, _build_row_descriptor, _probe_row_descriptor,
                    _row_descriptor);
        }
        return _hash_joiners[i];
    }

private:
    const THashJoinNode& _hash_join_node;
    TPlanNodeId _node_id;
    TPlanNodeType::type _node_type;
    int64_t _limit;
    std::vector<bool> _is_null_safes;
    std::vector<ExprContext*> _build_expr_ctxs;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _other_join_conjunct_ctxs;
    std::vector<ExprContext*> _conjunct_ctxs;
    const RowDescriptor _build_row_descriptor;
    const RowDescriptor _probe_row_descriptor;
    const RowDescriptor _row_descriptor;
    HashJoiners _hash_joiners;
};
} // namespace pipeline
} // namespace starrocks