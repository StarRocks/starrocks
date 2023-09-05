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

#include "exec/pipeline/query_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/pipeline/stream_epoch_manager.h"

namespace starrocks::stream {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;

struct GeneratorStreamSourceParam {
    int64_t num_column;
    int64_t start;
    int64_t step;
    int64_t chunk_size;
    int64_t ndv_count{100};
};

using SourceOperator = pipeline::SourceOperator;
using Operator = pipeline::Operator;

class GeneratorStreamSourceOperator final : public SourceOperator {
public:
    GeneratorStreamSourceOperator(pipeline::OperatorFactory* factory, int32_t id, const std::string& name,
                                  int32_t plan_node_id, int32_t driver_sequence, GeneratorStreamSourceParam param)
            : SourceOperator(factory, id, name, plan_node_id, false, driver_sequence),
              _param(param),
              _tablet_id(driver_sequence) {}

    ~GeneratorStreamSourceOperator() override = default;

    // Use mv epoch manager to interact with FE
    Status prepare(RuntimeState* state) override {
        auto st = SourceOperator::prepare(state);
        st.permit_unchecked_error();
        _stream_epoch_manager = state->query_ctx()->stream_epoch_manager();
        DCHECK(_stream_epoch_manager);
        return Status::OK();
    }

    int64_t tablet_id() { return _tablet_id; }
    bool is_trigger_finished(const EpochInfo& epoch_info);

    // never finished until mv epoch manager set it finished
    bool is_finished() const override { return _stream_epoch_manager && _stream_epoch_manager->is_finished(); }
    bool has_output() const override { return !_is_epoch_finished; }

    bool is_epoch_finished() const override { return _is_epoch_finished; }
    Status set_epoch_finishing(RuntimeState* state) override { return Status::OK(); }

    Status reset_epoch(RuntimeState* state) override {
        _current_epoch_info = _stream_epoch_manager->epoch_info();
        _processed_chunks = 1;
        _is_epoch_finished = false;
        VLOG_ROW << "reset_epoch:" << _current_epoch_info.debug_string();
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(starrocks::RuntimeState* state) override;

private:
    GeneratorStreamSourceParam _param;
    int64_t _tablet_id;
    starrocks::EpochInfo _current_epoch_info;
    pipeline::StreamEpochManager* _stream_epoch_manager;
    bool _is_epoch_finished{true};
    int64_t _processed_chunks{1};
};

class GeneratorStreamSourceOperatorFactory final : public pipeline::SourceOperatorFactory {
public:
    GeneratorStreamSourceOperatorFactory(int32_t id, int32_t plan_node_id, GeneratorStreamSourceParam param)
            : SourceOperatorFactory(id, "generator_stream_source", plan_node_id), _param(param) {}
    ~GeneratorStreamSourceOperatorFactory() override = default;
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<GeneratorStreamSourceOperator>(this, _id, _name, _plan_node_id, driver_sequence,
                                                               _param);
    }

private:
    GeneratorStreamSourceParam _param;
};

class PrinterStreamSinkOperator final : public Operator {
public:
    PrinterStreamSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "printer_stream_sink", plan_node_id, false, driver_sequence) {}

    ~PrinterStreamSinkOperator() override = default;

    // Use mv epoch manager to interact with FE
    Status prepare(RuntimeState* state) override {
        auto st = Operator::prepare(state);
        st.permit_unchecked_error();
        return Status::OK();
    }

    bool need_input() const override { return true; }
    bool has_output() const override { return !_is_finished; }

    // output chunk and reset
    const std::vector<ChunkPtr> output_chunks() const { return _output_chunks; }

    bool is_finished() const override { return _is_finished; }
    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }
    bool is_epoch_finished() const override { return _is_epoch_finished; }
    Status set_epoch_finishing(RuntimeState* state) override {
        _is_epoch_finished = true;
        return Status::OK();
    }
    Status reset_epoch(RuntimeState* runtime_state) override {
        _is_epoch_finished = false;
        _output_chunks.clear();
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::NotSupported("pull_chunk in StreamSinkOperator is not supported.");
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    bool _is_epoch_finished = false;
    std::vector<ChunkPtr> _output_chunks;
};

class PrinterStreamSinkOperatorFactory final : public OperatorFactory {
public:
    PrinterStreamSinkOperatorFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "printer_stream_sink", plan_node_id) {}

    ~PrinterStreamSinkOperatorFactory() override = default;

    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<PrinterStreamSinkOperator>(this, _id, _plan_node_id, driver_sequence);
    }

private:
};

} // namespace starrocks::stream
