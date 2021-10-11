// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>

#include "exec/pipeline/source_operator.h"

namespace starrocks {
class DataStreamRecvr;
class RowDescriptor;
namespace pipeline {
class ExchangeSourceOperator : public SourceOperator {
public:
    ExchangeSourceOperator(int32_t id, int32_t plan_node_id, int32_t num_sender, const RowDescriptor& row_desc)
            : SourceOperator(id, "exchange_source", plan_node_id), _num_sender(num_sender), _row_desc(row_desc) {}

    ~ExchangeSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    void finish(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    int32_t _num_sender;
    const RowDescriptor& _row_desc;
    std::shared_ptr<DataStreamRecvr> _stream_recvr;
    std::atomic<bool> _is_finishing{false};
};

class ExchangeSourceOperatorFactory final : public SourceOperatorFactory {
public:
    ExchangeSourceOperatorFactory(int32_t id, int32_t plan_node_id, int32_t num_sender, const RowDescriptor& row_desc)
            : SourceOperatorFactory(id, "exchange_source", plan_node_id),
              _num_sender(num_sender),
              _row_desc(row_desc) {}

    ~ExchangeSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<ExchangeSourceOperator>(_id, _plan_node_id, _num_sender, _row_desc);
    }

private:
    int32_t _num_sender;
    const RowDescriptor& _row_desc;
};

} // namespace pipeline
} // namespace starrocks
