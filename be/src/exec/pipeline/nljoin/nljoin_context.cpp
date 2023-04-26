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
#include <numeric>

#include "exec/cross_join_node.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exprs/expr.h"
#include "fmt/format.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

void NLJoinContext::close(RuntimeState* state) {
    _build_chunks.clear();
}

void NLJoinContext::incr_builder() {
    ++_num_right_sinkers;
    _input_chunks.emplace_back();
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

void NLJoinContext::append_build_chunk(int32_t sinker_id, const ChunkPtr& chunk) {
    _input_chunks[sinker_id].push_back(chunk);
}

Status NLJoinContext::finish_one_right_sinker(RuntimeState* state) {
    if (_num_right_sinkers - 1 == _num_finished_right_sinkers.fetch_add(1)) {
        // Accumulate chunks
        ChunkAccumulator accumulator(state->chunk_size());
        for (auto& sink_chunks : _input_chunks) {
            for (auto& tmp_chunk : sink_chunks) {
                if (tmp_chunk && !tmp_chunk->is_empty()) {
                    _num_build_rows += tmp_chunk->num_rows();
                    RETURN_IF_ERROR(accumulator.push(std::move(tmp_chunk)));
                }
            }
        }
        accumulator.finalize();
        while (ChunkPtr output = accumulator.pull()) {
            _build_chunks.emplace_back(std::move(output));
        }
        _input_chunks.clear();
        _input_chunks.shrink_to_fit();

        RETURN_IF_ERROR(_init_runtime_filter(state));
        _build_chunk_desired_size = state->chunk_size();
        _all_right_finished = true;
    }
    return Status::OK();
}

} // namespace starrocks::pipeline
