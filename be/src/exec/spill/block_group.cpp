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

#include "exec/spill/block_group.h"

#include <algorithm>
#include <unordered_set>

#include "common/config_exec_flow_fwd.h"
#include "exec/spill/input_stream_internal.h"
#include "exec/spill/spiller.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {

static const int chunk_buffer_max_size = 2;

std::vector<BlockGroupPtr> BlockGroupSet::select_compaction_block_groups() {
    std::vector<BlockGroupPtr> result;
    {
        std::lock_guard guard(_mutex);
        const int min_compact_groups = 8;
        const int level_size_factor = 8;
        if (_groups.size() <= min_compact_groups) {
            return {};
        }
        std::stable_sort(_groups.begin(), _groups.end(), [](const BlockGroupPtr& a, const BlockGroupPtr& b) {
            return a->data_size() > b->data_size();
        });

        // assign level to each group
        std::vector<int> levels(_groups.size(), 0);
        int current_level = 0;
        int need_compaction_level = -1;
        int current_level_nums = 0;
        size_t current_size = _groups.back()->data_size();

        for (int i = _groups.size() - 1; i >= 0; --i) {
            if (_groups[i]->data_size() > current_size * level_size_factor) {
                current_size = _groups[i]->data_size();
                current_level++;
                current_level_nums = 0;
            }
            levels[i] = current_level;
            current_level_nums++;
            if (current_level_nums > min_compact_groups && need_compaction_level < 0) {
                need_compaction_level = current_level;
            }
        }

        if (need_compaction_level >= 0) {
            // compact the groups in the same level
            std::unordered_set<BlockGroup*> removed_groups;
            for (int i = _groups.size() - 1; i >= 0; --i) {
                if (levels[i] == need_compaction_level) {
                    result.emplace_back(_groups[i]);
                    removed_groups.insert(_groups[i].get());
                }
                if (result.size() > min_compact_groups) {
                    break;
                }
            }

            auto it = std::remove_if(_groups.begin(), _groups.end(), [&removed_groups](const BlockGroupPtr& group) {
                return removed_groups.count(group.get()) > 0;
            });

            _groups.erase(it, _groups.end());
        } else {
            // select the smallest N block group for compact
            for (size_t i = 0; i < min_compact_groups; ++i) {
                result.emplace_back(_groups.back());
                _groups.pop_back();
            }
        }
    }
    return result;
}

StatusOr<InputStreamPtr> BlockGroupSet::as_unordered_stream(const SerdePtr& serde, Spiller* spiller) {
    BlockReaderOptions read_options;
    if (spiller->options().enable_buffer_read) {
        read_options.enable_buffer_read = true;
        read_options.max_buffer_bytes = spiller->options().max_read_buffer_bytes;
    }
    std::vector<BlockPtr> blocks;
    // collect block for each group
    for (const auto& group : _groups) {
        blocks.insert(blocks.end(), group->blocks().begin(), group->blocks().end());
    }
    auto stream = detail::make_sequence_input_stream(std::move(blocks), serde, read_options);
    return detail::make_buffered_input_stream(chunk_buffer_max_size, std::move(stream), spiller);
}

StatusOr<InputStreamPtr> BlockGroupSet::as_ordered_stream(RuntimeState* state, const SerdePtr& serde, Spiller* spiller,
                                                          const SortExecExprs* sort_exprs,
                                                          const SortDescs* sort_descs) {
    return build_ordered_stream(_groups, state, serde, spiller, sort_exprs, sort_descs);
}

StatusOr<InputStreamPtr> BlockGroupSet::build_ordered_stream(std::vector<BlockGroupPtr>& block_groups,
                                                             RuntimeState* state, const SerdePtr& serde,
                                                             Spiller* spiller, const SortExecExprs* sort_exprs,
                                                             const SortDescs* sort_descs) {
    BlockReaderOptions read_options;
    if (spiller->options().enable_buffer_read && block_groups.size() > 0) {
        size_t max_buffer_bytes = spiller->options().max_read_buffer_bytes / block_groups.size();
        if (max_buffer_bytes > config::spill_read_buffer_min_bytes) {
            read_options.enable_buffer_read = true;
            read_options.max_buffer_bytes = max_buffer_bytes;
        }
    }
    std::vector<InputStreamPtr> streams;
    for (const auto& group : block_groups) {
        auto stream = detail::make_sequence_input_stream(group->blocks(), serde, read_options);
        streams.emplace_back(detail::make_buffered_input_stream(chunk_buffer_max_size, stream, spiller));
    }

    InputStreamPtr res;
    if (streams.empty() || state->is_cancelled()) {
        res = detail::make_raw_chunk_input_stream(std::vector<ChunkPtr>(), spiller);
    } else {
        ASSIGN_OR_RETURN(res, detail::make_ordered_input_stream(std::move(streams), state, serde, sort_exprs,
                                                                sort_descs, spiller));
    }

    return res;
}

} // namespace starrocks::spill
