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

#include "exec/pipeline/nljoin/nljoin_context.h"

#include <algorithm>
#include <memory>
#include <numeric>

#include "exec/cross_join_node.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/spill/executor.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/spill_components.h"
#include "exec/spill/spiller.hpp"
#include "exprs/expr.h"
#include "fmt/format.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

Status NJJoinBuildInputChannel::add_chunk(ChunkPtr build_chunk) {
    if (build_chunk == nullptr || build_chunk->is_empty()) {
        return Status::OK();
    }
    _num_rows += build_chunk->num_rows();
    RETURN_IF_ERROR(_accumulator.push(std::move(build_chunk)));
    return Status::OK();
}

Status NJJoinBuildInputChannel::add_chunk_to_spill_buffer(RuntimeState* state, ChunkPtr build_chunk,
                                                          spill::IOTaskExecutor& executor) {
    if (build_chunk == nullptr || build_chunk->is_empty()) {
        return Status::OK();
    }

    _num_rows += build_chunk->num_rows();
    RETURN_IF_ERROR(_accumulator.push(std::move(build_chunk)));
    if (auto chunk = _accumulator.pull()) {
        RETURN_IF_ERROR(_spiller->spill(state, chunk, TRACKER_WITH_SPILLER_GUARD(state, _spiller)));
    }

    return Status::OK();
}

void NJJoinBuildInputChannel::finalize() {
    _accumulator.finalize();
    while (ChunkPtr output = _accumulator.pull()) {
        _input_chunks.emplace_back(std::move(output));
    }
}

void NJJoinBuildInputChannel::close() {
    _accumulator.reset();
    _input_chunks.clear();
    _spiller.reset();
}

Status SpillableNLJoinChunkStream::prefetch(RuntimeState* state, spill::IOTaskExecutor& executor) {
    return _reader->trigger_restore(state, RESOURCE_TLS_MEMTRACER_GUARD(state, std::weak_ptr(_reader)));
}

bool SpillableNLJoinChunkStream::has_output() {
    return _reader && _reader->has_output_data();
}

StatusOr<ChunkPtr> SpillableNLJoinChunkStream::get_next(RuntimeState* state, spill::IOTaskExecutor& executor) {
    return _reader->restore(state, RESOURCE_TLS_MEMTRACER_GUARD(state, std::weak_ptr(_reader)));
}

Status SpillableNLJoinChunkStream::reset(RuntimeState* state, spill::Spiller* dummy_spiller) {
    std::vector<spill::InputStreamPtr> spilled_input_streams;

    auto stream = spill::SpillInputStream::as_stream(_build_chunks, dummy_spiller);
    spilled_input_streams.emplace_back(std::move(stream));

    //
    for (auto& spiller : _spillers) {
        spill::InputStreamPtr input_stream;
        RETURN_IF_ERROR(spiller->writer()->acquire_stream(&input_stream));
        spilled_input_streams.emplace_back(std::move(input_stream));
    }

    stream = spill::SpillInputStream::union_all(spilled_input_streams);
    _reader = std::make_shared<spill::SpillerReader>(dummy_spiller);
    _reader->set_stream(std::move(stream));

    return Status::OK();
}

Status NLJoinBuildChunkStreamBuilder::init(RuntimeState* state,
                                           std::vector<std::unique_ptr<NJJoinBuildInputChannel>>& channels) {
    for (auto& channel : channels) {
        if (channel->has_spilled()) {
            _spillers.emplace_back(channel->spiller());
        }
    }

    // normalize all incomplete chunks
    ChunkAccumulator accumulator(state->chunk_size());
    for (auto& sink : channels) {
        if (auto chunk = sink->incomplete_chunk()) {
            RETURN_IF_ERROR(accumulator.push(std::move(chunk)));
        }
    }
    accumulator.finalize();

    // collect all complete chunks
    for (auto& sink : channels) {
        sink->for_each_complete_chunk([&](auto&& chunk) { _build_chunks.emplace_back(std::move(chunk)); });
    }

    while (ChunkPtr output = accumulator.pull()) {
        _build_chunks.emplace_back(std::move(output));
    }

    return Status::OK();
}

std::vector<ChunkPtr> NLJoinBuildChunkStreamBuilder::build() {
    return _build_chunks;
}

std::unique_ptr<SpillableNLJoinChunkStream> NLJoinBuildChunkStreamBuilder::build_stream() {
    return std::make_unique<SpillableNLJoinChunkStream>(_build_chunks, _spillers);
}

void NLJoinContext::close(RuntimeState* state) {
    _build_chunks.clear();
    _build_stream_builder.close();
}

void NLJoinContext::incr_builder(RuntimeState* state) {
    ++_num_right_sinkers;
    _input_channel.emplace_back(std::make_unique<NJJoinBuildInputChannel>(state->chunk_size()));
}
void NLJoinContext::incr_prober() {
    ++_num_left_probers;
}
void NLJoinContext::decr_prober(RuntimeState* state) {
    // NlJoinProbeOperator may be instantiated lazily, so context is ref for prober
    // in NLJoinProbeOperatorFactory::prepare and unref when all the probers are closed here.
    if (++_num_closed_left_probers == _num_left_probers) {
        unref(state);
    }
}

Status NLJoinContext::_init_runtime_filter(RuntimeState* state) {
    ChunkPtr one_row_chunk = nullptr;
    size_t num_rows = 0;
    for (auto& chunk_ptr : _build_chunks) {
        if (chunk_ptr != nullptr) {
            if (chunk_ptr->num_rows() == 1) {
                one_row_chunk = chunk_ptr;
            }
            num_rows += chunk_ptr->num_rows();
        }
    }
    // build runtime filter for cross join
    if (num_rows == 1) {
        DCHECK(one_row_chunk != nullptr);
        auto* pool = state->obj_pool();
        ASSIGN_OR_RETURN(auto rfs, CrossJoinNode::rewrite_runtime_filter(pool, _rf_descs, one_row_chunk.get(),
                                                                         _rf_conjuncts_ctx));
        _rf_hub->set_collector(_plan_node_id,
                               std::make_unique<RuntimeFilterCollector>(std::move(rfs), RuntimeBloomFilterList{}));
    } else {
        // notify cross join left child
        _rf_hub->set_collector(_plan_node_id, std::make_unique<RuntimeFilterCollector>(RuntimeInFilterList{},
                                                                                       RuntimeBloomFilterList{}));
    }
    return Status::OK();
}

void NLJoinContext::_notify_runtime_filter_collector(RuntimeState* state) {
    _rf_hub->set_collector(_plan_node_id,
                           std::make_unique<RuntimeFilterCollector>(RuntimeInFilterList{}, RuntimeBloomFilterList{}));
}

bool NLJoinContext::finish_probe(int32_t driver_seq, const std::vector<uint8_t>& build_match_flags) {
    std::lock_guard guard(_join_stage_mutex);

    ++_num_post_probers;
    VLOG(3) << fmt::format("CrossJoin operator {} finish probe {}/{}: self_match_flags: {} \n shared_match_flags: {}",
                           driver_seq, _num_post_probers, _num_left_probers, fmt::join(build_match_flags, ","),
                           fmt::join(_shared_build_match_flag, ","));
    bool is_last = _num_post_probers == _num_left_probers;

    // Merge all build_match_flag from all probers
    if (build_match_flags.empty()) {
        return is_last;
    }
    if (_shared_build_match_flag.empty()) {
        _shared_build_match_flag.resize(build_match_flags.size(), 0);
    }
    DCHECK_EQ(build_match_flags.size(), _shared_build_match_flag.size());
    ColumnHelper::or_two_filters(&_shared_build_match_flag, build_match_flags.data());

    return is_last;
}

const std::vector<uint8_t> NLJoinContext::get_shared_build_match_flag() const {
    DCHECK_EQ(_num_post_probers, _num_left_probers) << "all probers should share their states";
    std::lock_guard guard(_join_stage_mutex);
    return _shared_build_match_flag;
}

Status NLJoinContext::append_build_chunk(int32_t sinker_id, const ChunkPtr& chunk) {
    return _input_channel[sinker_id]->add_chunk(chunk);
}

size_t NLJoinContext::channel_num_rows(int32_t sinker_id) {
    return _input_channel[sinker_id]->num_rows();
}

Status NLJoinContext::finish_one_right_sinker(int32_t sinker_id, RuntimeState* state) {
    _input_channel[sinker_id]->finalize();

    if (_num_right_sinkers - 1 == _num_finished_right_sinkers.fetch_add(1)) {
        _build_chunk_desired_size = state->chunk_size();
        for (auto& channel : _input_channel) {
            _num_build_rows += channel->num_rows();
        }

        RETURN_IF_ERROR(_build_stream_builder.init(state, _input_channel));

        if (!_build_stream_builder.has_spilled()) {
            _build_chunks = _build_stream_builder.build();
            RETURN_IF_ERROR(_init_runtime_filter(state));
        } else {
            _notify_runtime_filter_collector(state);
        }

        for (auto& channel : _input_channel) {
            channel->close();
        }

        _all_right_finished = true;
    }
    return Status::OK();
}

Status NLJoinContext::finish_one_left_prober(RuntimeState* state) {
    if (_num_left_probers == _num_finished_left_probers.fetch_add(1) + 1) {
        // All the probers have finished, so the builders can be short-circuited.
        RETURN_IF_ERROR(set_finished());
    }
    return Status::OK();
}

} // namespace starrocks::pipeline
