// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {

namespace stream_load {
class OlapTableSink;
}

namespace pipeline {
class OlapTableSinkOperator final : public Operator {
public:
    OlapTableSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                          int32_t sender_id, starrocks::stream_load::OlapTableSink* sink,
                          FragmentContext* const fragment_ctx, std::atomic<int32_t>& num_sinkers)
            : Operator(factory, id, "olap_table_sink", plan_node_id, driver_sequence),
              _sink(sink),
              _fragment_ctx(fragment_ctx),
              _num_sinkers(num_sinkers),
              _sender_id(sender_id) {}

    ~OlapTableSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    bool pending_finish() const override;

    Status set_finishing(RuntimeState* state) override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    starrocks::stream_load::OlapTableSink* _sink;
    FragmentContext* const _fragment_ctx;
    std::atomic<int32_t>& _num_sinkers;

    bool _is_finished = false;
    mutable bool _is_open_done = false;
    int32_t _sender_id;

    bool _is_audit_report_done = true;
};

class OlapTableSinkOperatorFactory final : public OperatorFactory {
public:
    OlapTableSinkOperatorFactory(int32_t id, std::unique_ptr<starrocks::DataSink>& sink,
                                 FragmentContext* const fragment_ctx, int32_t start_sender_id, size_t tablet_sink_dop,
                                 std::vector<std::unique_ptr<starrocks::stream_load::OlapTableSink>>& tablet_sinks)
            : OperatorFactory(id, "olap_table_sink", Operator::s_pseudo_plan_node_id_for_olap_table_sink),
              _data_sink(std::move(sink)),
              _sink0(down_cast<starrocks::stream_load::OlapTableSink*>(_data_sink.get())),
              _fragment_ctx(fragment_ctx),
              _cur_sender_id(start_sender_id),
              _sinks(std::move(tablet_sinks)) {}

    ~OlapTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    void _increment_num_sinkers_no_barrier() { _num_sinkers.fetch_add(1, std::memory_order_relaxed); }

    std::unique_ptr<starrocks::DataSink> _data_sink;
    starrocks::stream_load::OlapTableSink* _sink0;
    FragmentContext* const _fragment_ctx;
    std::atomic<int32_t> _num_sinkers = 0;
    int32_t _cur_sender_id;
    std::vector<std::unique_ptr<starrocks::stream_load::OlapTableSink>> _sinks;
};

} // namespace pipeline
} // namespace starrocks
