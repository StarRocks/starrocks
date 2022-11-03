// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/global_types.h"
#include "exec/olap_common.h"
#include "exec/pipeline/operator.h"
#include "runtime/global_dict/decoder.h"
#include "runtime/global_dict/parser.h"

namespace starrocks::pipeline {

using vectorized::GlobalDictDecoderPtr;
using vectorized::DictOptimizeContext;
using vectorized::DictOptimizeParser;
using vectorized::Columns;

class DictDecodeOperator final : public Operator {
public:
    DictDecodeOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                       std::vector<int32_t>& encode_column_cids, std::vector<int32_t>& decode_column_cids,
                       std::vector<GlobalDictDecoderPtr>& decoders)
            : Operator(factory, id, "dict_decode", plan_node_id, driver_sequence),
              _encode_column_cids(encode_column_cids),
              _decode_column_cids(decode_column_cids),
              _decoders(decoders) {}

    ~DictDecodeOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return _cur_chunk != nullptr; }

    bool need_input() const override { return !_is_finished && _cur_chunk == nullptr; }

    bool is_finished() const override { return _is_finished && _cur_chunk == nullptr; }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    const std::vector<int32_t>& _encode_column_cids;
    const std::vector<int32_t>& _decode_column_cids;
    const std::vector<GlobalDictDecoderPtr>& _decoders;

    bool _is_finished = false;
    vectorized::ChunkPtr _cur_chunk = nullptr;
};

class DictDecodeOperatorFactory final : public OperatorFactory {
public:
    DictDecodeOperatorFactory(int32_t id, int32_t plan_node_id, std::vector<int32_t>&& encode_column_cids,
                              std::vector<int32_t>&& decode_column_cids, std::vector<ExprContext*>&& expr_ctxs,
                              std::map<SlotId, std::pair<ExprContext*, DictOptimizeContext>>&& string_functions)
            : OperatorFactory(id, "dict_decode", plan_node_id),
              _encode_column_cids(std::move(encode_column_cids)),
              _decode_column_cids(std::move(decode_column_cids)),
              _expr_ctxs(std::move(expr_ctxs)),
              _string_functions(std::move(string_functions)) {}
    ~DictDecodeOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<DictDecodeOperator>(this, _id, _plan_node_id, driver_sequence, _encode_column_cids,
                                                    _decode_column_cids, _decoders);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::vector<int32_t> _encode_column_cids;
    std::vector<int32_t> _decode_column_cids;
    std::vector<GlobalDictDecoderPtr> _decoders;

    std::vector<ExprContext*> _expr_ctxs;
    std::map<SlotId, std::pair<ExprContext*, DictOptimizeContext>> _string_functions;
    DictOptimizeParser _dict_optimize_parser;
};

} // namespace starrocks::pipeline
