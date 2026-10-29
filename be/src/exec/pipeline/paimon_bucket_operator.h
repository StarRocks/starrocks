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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "exec_primitive/pipeline/operator_factory.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class ExprContext;

inline constexpr int32_t kPaimonBucketRoutingProtocolVersion = 1;
inline constexpr size_t kMaxPaimonRoutingPayloadBytes = 1024 * 1024;

// Validates the bounded, same-release TBinaryProtocol descriptor without logging credentials.
StatusOr<TPaimonTableSink> decode_paimon_bucket_routing(const TPaimonBucketRouting& routing);

// Appends a native-calculated, nonnullable INT column; never substitutes a SQL hash/constant.
Status append_paimon_bucket_column(const ChunkPtr& chunk, SlotId slot_id, int32_t bucket_count,
                                   const std::vector<int32_t>& bucket_ids);

namespace pipeline {

class PaimonBucketOperator final : public Operator {
public:
    PaimonBucketOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                         const TPaimonTableSink& sink, const std::vector<ExprContext*>& schema_exprs)
            : Operator(factory, id, "paimon_bucket", plan_node_id, false, driver_sequence),
              _sink(sink),
              _schema_exprs(schema_exprs) {}

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    bool has_output() const override { return _chunk != nullptr; }
    bool need_input() const override { return !_finishing && _chunk == nullptr; }
    bool is_finished() const override { return _finishing && _chunk == nullptr; }
    bool ignore_empty_eos() const override { return false; }
    Status set_finishing(RuntimeState* state) override {
        _finishing = true;
        return Status::OK();
    }
    Status set_finished(RuntimeState* state) override {
        _finishing = true;
        _chunk.reset();
        return Status::OK();
    }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    const TPaimonTableSink& _sink;
    const std::vector<ExprContext*>& _schema_exprs;
    bool _finishing = false;
    ChunkPtr _chunk;
    RuntimeProfile::Counter* _bucket_compute_timer = nullptr;
};

class PaimonBucketOperatorFactory final : public OperatorFactory {
public:
    PaimonBucketOperatorFactory(int32_t id, int32_t plan_node_id, TPaimonTableSink sink,
                                std::vector<ExprContext*> schema_exprs)
            : OperatorFactory(id, "paimon_bucket", plan_node_id),
              _sink(std::move(sink)),
              _schema_exprs(std::move(schema_exprs)) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<PaimonBucketOperator>(this, _id, _plan_node_id, driver_sequence, _sink, _schema_exprs);
    }
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    TPaimonTableSink _sink;
    std::vector<ExprContext*> _schema_exprs;
};

} // namespace pipeline
} // namespace starrocks
