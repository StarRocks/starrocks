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

#include "exec/spill/input_stream.h"

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "exec/workgroup/work_group.h"
#include "runtime/sorted_chunks_merger.h"
#include "util/blocking_queue.hpp"
#include "util/defer_op.h"

namespace starrocks::spill {

static const int chunk_buffer_max_size = 2;

Status YieldableRestoreTask::do_read(workgroup::YieldContext& yield_ctx, SerdeContext& context) {
    size_t num_eos = 0;
    yield_ctx.total_yield_point_cnt = _sub_stream.size();
    auto wg = yield_ctx.wg;
    while (yield_ctx.yield_point < yield_ctx.total_yield_point_cnt) {
        {
            SCOPED_RAW_TIMER(&yield_ctx.time_spent_ns);
            size_t i = yield_ctx.yield_point;
            if (!_sub_stream[i]->eof()) {
                DCHECK(_sub_stream[i]->enable_prefetch());
                auto status = _sub_stream[i]->prefetch(yield_ctx, context);
                if (!status.ok() && !status.is_end_of_file() && !status.is_yield()) {
                    return status;
                }
                if (status.is_yield()) {
                    yield_ctx.need_yield = true;
                    return Status::OK();
                }
            }
            yield_ctx.yield_point++;
            num_eos += _sub_stream[i]->eof();
        }

        BREAK_IF_YIELD(wg, &yield_ctx.need_yield, yield_ctx.time_spent_ns);
    }

    if (num_eos == _sub_stream.size()) {
        _input_stream->mark_is_eof();
        return Status::EndOfFile("eos");
    }
    return Status::OK();
}

class UnionAllSpilledInputStream final : public SpillInputStream {
public:
    UnionAllSpilledInputStream(InputStreamPtr left, InputStreamPtr right) {
        _streams.emplace_back(std::move(left));
        _streams.emplace_back(std::move(right));
    }

    UnionAllSpilledInputStream(std::vector<InputStreamPtr> streams) : _streams(std::move(streams)) {}

    ~UnionAllSpilledInputStream() override = default;

    StatusOr<ChunkUniquePtr> get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) override;

    void get_io_stream(std::vector<SpillInputStream*>* io_stream) override {
        for (auto& stream : _streams) {
            stream->get_io_stream(io_stream);
        }
    }

    bool is_ready() override {
        return std::all_of(_streams.begin(), _streams.end(), [](auto& stream) { return stream->is_ready(); });
    }

    void close() override {
        for (const auto& stream : _streams) {
            stream->close();
        }
    };

private:
    size_t _current_process_idx = 0;
    std::vector<InputStreamPtr> _streams;
};

StatusOr<ChunkUniquePtr> UnionAllSpilledInputStream::get_next(workgroup::YieldContext& yield_ctx,
                                                              SerdeContext& context) {
    if (_current_process_idx < _streams.size()) {
        auto chunk_st = _streams[_current_process_idx]->get_next(yield_ctx, context);
        if (chunk_st.ok()) {
            return std::move(chunk_st.value());
        }
        if (chunk_st.status().is_end_of_file()) {
            _current_process_idx++;
            return std::make_unique<Chunk>();
        } else {
            return chunk_st.status();
        }
    }
    return Status::EndOfFile("eos");
}

// The raw chunk input stream. all chunks are in memory.
class RawChunkInputStream final : public SpillInputStream {
public:
    RawChunkInputStream(std::vector<ChunkPtr> chunks, Spiller* spiller) : _chunks(std::move(chunks)) {
        for (const auto& chunk : _chunks) {
            _total_rows += chunk->num_rows();
        }
    }
    StatusOr<ChunkUniquePtr> get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) override;

    bool is_ready() override { return true; };
    void close() override{};

private:
    size_t _total_rows{};
    size_t _read_rows{};
    size_t _read_idx{};
    std::vector<ChunkPtr> _chunks;
    DECLARE_RACE_DETECTOR(detect_get_next)
};

StatusOr<ChunkUniquePtr> RawChunkInputStream::get_next(workgroup::YieldContext& yield_ctx, SerdeContext& context) {
    RACE_DETECT(detect_get_next);
    if (_read_idx >= _chunks.size()) {
        DCHECK_EQ(_total_rows, _read_rows);
        return Status::EndOfFile("eos");
    }
    // TODO: make ChunkPtr could convert to ChunkUniquePtr to avoid unused memory copy
    auto res = std::move(_chunks[_read_idx++])->clone_unique();
    _read_rows += res->num_rows();
    _chunks[_read_idx - 1].reset();

    return res;
}

// method for create input stream
InputStreamPtr SpillInputStream::union_all(const InputStreamPtr& left, const InputStreamPtr& right) {
    return std::make_shared<UnionAllSpilledInputStream>(left, right);
}

InputStreamPtr SpillInputStream::union_all(std::vector<InputStreamPtr>& _streams) {
    return std::make_shared<UnionAllSpilledInputStream>(_streams);
}

InputStreamPtr SpillInputStream::as_stream(const std::vector<ChunkPtr>& chunks, Spiller* spiller) {
    return std::make_shared<RawChunkInputStream>(chunks, spiller);
}

class BufferedInputStream : public SpillInputStream {
public:
    BufferedInputStream(int capacity, InputStreamPtr stream, Spiller* spiller)
            : _capacity(capacity), _input_stream(std::move(stream)), _spiller(spiller) {}
    ~BufferedInputStream() override = default;

    bool is_buffer_full() { return _chunk_buffer.get_size() >= _capacity; }
    // The ChunkProvider in sort operator needs to use has_chunk to check whether the data is ready,
    // if the InputStream is in the eof state, it also needs to return true to driver ChunkSortCursor into the stage of obtaining data.
    bool has_chunk() { return !_chunk_buffer.empty() || eof(); }

    StatusOr<ChunkUniquePtr> get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) override;
    bool is_ready() override { return has_chunk(); }
    void close() override {}

    bool enable_prefetch() const override { return true; }

    void get_io_stream(std::vector<SpillInputStream*>* io_stream) override { io_stream->emplace_back(this); }

    Status prefetch(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) override;

    StatusOr<ChunkUniquePtr> read_from_buffer();

private:
    bool _acquire() {
        bool expected = false;
        return _is_prefetching.compare_exchange_strong(expected, true);
    }
    void _release() {
        bool expected = true;
        bool result = _is_prefetching.compare_exchange_strong(expected, false);
        // _release is only invoked when _acquire successes, here add a DCHECK to check it.
        DCHECK(result);
    }

private:
    int _capacity;
    InputStreamPtr _input_stream;
    UnboundedBlockingQueue<ChunkUniquePtr> _chunk_buffer;
    std::atomic_bool _is_prefetching = false;
    Spiller* _spiller = nullptr;
};

StatusOr<ChunkUniquePtr> BufferedInputStream::read_from_buffer() {
    if (_chunk_buffer.empty()) {
        CHECK(eof());
        return Status::EndOfFile("end of reading spilled BufferedInputStream");
    }
    ChunkUniquePtr res;
    CHECK(_chunk_buffer.try_get(&res));
    COUNTER_ADD(_spiller->metrics().input_stream_peak_memory_usage, -res->memory_usage());
    return res;
}

StatusOr<ChunkUniquePtr> BufferedInputStream::get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) {
    if (has_chunk()) {
        return read_from_buffer();
    }
    // if prefetch failed, return empty chunk
    DCHECK(false);
    return std::make_unique<Chunk>();
}

Status BufferedInputStream::prefetch(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) {
    if (is_buffer_full() || eof()) {
        return Status::OK();
    }
    // concurrent prefetch is not allowed, should call _acquire and _release before and after prefetch
    // to ensure that it doesn't happen.
    if (!_acquire()) {
        return Status::OK();
    }
    DeferOp defer([this]() { _release(); });

    auto res = _input_stream->get_next(yield_ctx, ctx);
    if (res.ok()) {
        COUNTER_ADD(_spiller->metrics().input_stream_peak_memory_usage, res.value()->memory_usage());
        _chunk_buffer.put(std::move(res.value()));
        return Status::OK();
    } else if (res.status().is_end_of_file()) {
        mark_is_eof();
        return Status::OK();
    }
    return res.status();
}

class SequenceInputStream : public SpillInputStream {
public:
    SequenceInputStream(std::vector<BlockPtr> input_blocks, SerdePtr serde, BlockReaderOptions options)
            : _input_blocks(std::move(input_blocks)), _serde(std::move(serde)), _options(std::move(options)) {}
    ~SequenceInputStream() override = default;

    StatusOr<ChunkUniquePtr> get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) override;

    bool is_ready() override { return false; }

    void close() override {}

private:
    std::vector<BlockPtr> _input_blocks;
    std::shared_ptr<BlockReader> _current_reader;
    size_t _current_idx = 0;
    size_t _block_read_rows = 0;
    SerdePtr _serde;
    BlockReaderOptions _options;
    DECLARE_RACE_DETECTOR(detect_get_next)
};

StatusOr<ChunkUniquePtr> SequenceInputStream::get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) {
    RACE_DETECT(detect_get_next);
    if (_current_idx >= _input_blocks.size()) {
        return Status::EndOfFile("end of reading spilled UnorderedInputStream");
    }
    auto io_ctx = std::any_cast<SpillIOTaskContextPtr>(yield_ctx.task_context_data);

    while (true) {
        if (_current_reader == nullptr) {
            bool is_remote = _input_blocks[_current_idx]->is_remote();
            _options.read_io_bytes = GET_METRICS(is_remote, _serde->parent()->metrics(), restore_bytes);
            _options.read_io_timer = GET_METRICS(is_remote, _serde->parent()->metrics(), read_io_timer);
            _options.read_io_count = GET_METRICS(is_remote, _serde->parent()->metrics(), read_io_count);
            _current_reader = _input_blocks[_current_idx]->get_reader(_options);
        }
        auto& block = _input_blocks[_current_idx];
        if (!(block->is_remote() ^ io_ctx->use_local_io_executor)) {
            TRACE_SPILL_LOG << fmt::format("block[{}], use_local_io_executor[{}], should yield", block->debug_string(),
                                           io_ctx->use_local_io_executor);
            io_ctx->use_local_io_executor = !block->is_remote();
            return Status::Yield();
        }

        auto res = _serde->deserialize(ctx, _current_reader.get());
        if (res.status().is_end_of_file()) {
            DCHECK_EQ(_block_read_rows, _input_blocks[_current_idx]->num_rows());
            _input_blocks[_current_idx].reset();
            _current_reader.reset();
            _current_idx++;
            _block_read_rows = 0;
            if (_current_idx >= _input_blocks.size()) {
                return Status::EndOfFile("end of stream");
            }
            // move to the next block
            continue;
        }
        RETURN_IF_ERROR(res.status());
        if (res.status().ok() && res.value()->is_empty()) {
            continue;
        }
        _block_read_rows += res.value()->num_rows();
        return res;
    }
    __builtin_unreachable();
}

class OrderedInputStream : public SpillInputStream {
public:
    OrderedInputStream(std::vector<InputStreamPtr> input_streams, RuntimeState* state)
            : _input_streams(std::move(input_streams)), _merger(state) {}

    ~OrderedInputStream() override = default;

    Status init(const SerdePtr& serde, const SortExecExprs* sort_exprs, const SortDescs* descs, Spiller* spiller);

    StatusOr<ChunkUniquePtr> get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) override;

    void get_io_stream(std::vector<SpillInputStream*>* io_stream) override {
        for (auto& stream : _input_streams) {
            stream->get_io_stream(io_stream);
        }
    }
    bool is_ready() override {
        return _merger.is_data_ready() && std::all_of(_input_streams.begin(), _input_streams.end(),
                                                      [](auto& stream) { return stream->is_ready(); });
    }
    void close() override {}

private:
    // multiple buffered stream
    std::vector<InputStreamPtr> _input_streams;
    starrocks::CascadeChunkMerger _merger;
    Status _status;
};

Status OrderedInputStream::init(const SerdePtr& serde, const SortExecExprs* sort_exprs, const SortDescs* descs,
                                Spiller* spiller) {
    std::vector<starrocks::ChunkProvider> chunk_providers;
    DCHECK(!_input_streams.empty());

    for (auto& input_stream : _input_streams) {
        auto chunk_provider = [input_stream, this](ChunkUniquePtr* output, bool* eos) {
            if (output == nullptr || eos == nullptr) {
                return input_stream->is_ready();
            }
            if (!input_stream->is_ready()) {
                return false;
            }
            SerdeContext ctx;
            workgroup::YieldContext mock_ctx;
            auto res = input_stream->get_next(mock_ctx, ctx);
            if (!res.status().ok()) {
                if (!res.status().is_end_of_file()) {
                    _status.update(res.status());
                }
                input_stream->mark_is_eof();
                *eos = true;
                return false;
            }
            *output = std::move(res.value());
            return true;
        };
        chunk_providers.emplace_back(std::move(chunk_provider));
    }
    RETURN_IF_ERROR(_merger.init(chunk_providers, &(sort_exprs->lhs_ordering_expr_ctxs()), *descs));
    return Status::OK();
}

StatusOr<ChunkUniquePtr> OrderedInputStream::get_next(workgroup::YieldContext& yield_ctx, SerdeContext& ctx) {
    ChunkUniquePtr chunk;
    bool should_exit = false;
    std::atomic_bool eos = false;
    RETURN_IF_ERROR(_merger.get_next(&chunk, &eos, &should_exit));
    if (chunk && !chunk->is_empty()) {
        return std::move(chunk);
    }
    if (eos) {
        return Status::EndOfFile("end of reading spilled OrderedInputStream");
    }
    DCHECK(should_exit);
    return std::make_unique<Chunk>();
}

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
    auto stream = std::make_shared<SequenceInputStream>(std::move(blocks), serde, read_options);
    return std::make_shared<BufferedInputStream>(chunk_buffer_max_size, std::move(stream), spiller);
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
        auto stream = std::make_shared<SequenceInputStream>(group->blocks(), serde, read_options);
        streams.emplace_back(std::make_shared<BufferedInputStream>(chunk_buffer_max_size, stream, spiller));
    }

    InputStreamPtr res;
    if (streams.empty() || state->is_cancelled()) {
        res = std::make_shared<RawChunkInputStream>(std::vector<ChunkPtr>(), spiller);
    } else {
        auto stream = std::make_shared<OrderedInputStream>(std::move(streams), state);
        RETURN_IF_ERROR(stream->init(serde, sort_exprs, sort_descs, spiller));
        res = std::move(stream);
    }

    return res;
}

} // namespace starrocks::spill