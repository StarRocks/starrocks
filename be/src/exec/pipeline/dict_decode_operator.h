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

#include "common/global_types.h"
#include "exec/olap_common.h"
#include "exec/pipeline/operator.h"
#include "runtime/global_dict/decoder.h"
#include "runtime/global_dict/parser.h"

namespace starrocks::pipeline {

class DictDecodeOperator final : public Operator {
public:
    DictDecodeOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                       std::vector<int32_t>& encode_column_cids, std::vector<int32_t>& decode_column_cids,
                       std::vector<TypeDescriptor*>& decode_column_types, std::vector<GlobalDictDecoderPtr>& decoders)
            : Operator(factory, id, "dict_decode", plan_node_id, false, driver_sequence),
              _encode_column_cids(encode_column_cids),
              _decode_column_cids(decode_column_cids),
              _decode_column_types(decode_column_types),
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

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    const std::vector<int32_t>& _encode_column_cids;
    const std::vector<int32_t>& _decode_column_cids;
    const std::vector<TypeDescriptor*>& _decode_column_types;
    const std::vector<GlobalDictDecoderPtr>& _decoders;

    bool _is_finished = false;
    ChunkPtr _cur_chunk = nullptr;
};

class DictDecodeOperatorFactory final : public OperatorFactory {
public:
    DictDecodeOperatorFactory(int32_t id, int32_t plan_node_id, std::vector<int32_t>&& encode_column_cids,
                              std::vector<int32_t>&& decode_column_cids,
                              std::vector<TypeDescriptor*>&& decode_column_types, std::vector<ExprContext*>&& expr_ctxs,
                              std::map<SlotId, std::pair<ExprContext*, DictOptimizeContext>>&& string_functions)
            : OperatorFactory(id, "dict_decode", plan_node_id),
              _encode_column_cids(std::move(encode_column_cids)),
              _decode_column_cids(std::move(decode_column_cids)),
              _decode_column_types(std::move(decode_column_types)),
              _expr_ctxs(std::move(expr_ctxs)),
              _string_functions(std::move(string_functions)) {}
    ~DictDecodeOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<DictDecodeOperator>(this, _id, _plan_node_id, driver_sequence, _encode_column_cids,
                                                    _decode_column_cids, _decode_column_types, _decoders);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::vector<int32_t> _encode_column_cids;
    std::vector<int32_t> _decode_column_cids;
    std::vector<TypeDescriptor*> _decode_column_types;
    std::vector<GlobalDictDecoderPtr> _decoders;

    std::vector<ExprContext*> _expr_ctxs;
    std::map<SlotId, std::pair<ExprContext*, DictOptimizeContext>> _string_functions;
};

} // namespace starrocks::pipeline
