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

#include "compute_env/spill/input_stream.h"

#include <glog/logging.h>

#include <algorithm>
#include <any>
#include <atomic>
#include <memory>
#include <utility>
#include <vector>

#include "base/concurrency/blocking_queue.hpp"
#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "common/status.h"
#include "compute_env/sorting/sorted_chunks_merger.h"
#include "compute_env/spill/block_manager.h"
#include "compute_env/spill/input_stream_internal.h"
#include "compute_env/spill/serde.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/task_executor.h"
#include "exprs/sort_exec_exprs.h"
#include "fmt/format.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {

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

namespace {
// Checks that no ColumnPtr anywhere in a column tree is shared with another
// holder, so the column can be handed out for in-place mutation without
// copying. Column types not listed here (struct, json, variant,
// adaptive-nullable, any future type) are conservatively treated as shared.
class ExclusiveOwnershipChecker final : public ColumnVisitorAdapter<ExclusiveOwnershipChecker> {
public:
    ExclusiveOwnershipChecker() : ColumnVisitorAdapter(this) {}

    template <typename PtrT>
    bool exclusive(const PtrT& column) {
        // a non-ok accept means the visitor base rejected an unknown type,
        // which counts as shared as well
        return column->use_count() == 1 && column->accept(this).ok() && !_shared_found;
    }

    Status do_visit(const NullableColumn& column) {
        _shared_found |= !exclusive(column.data_column()) || !exclusive(column.null_column());
        return Status::OK();
    }
    Status do_visit(const ConstColumn& column) {
        _shared_found |= !exclusive(column.data_column());
        return Status::OK();
    }
    Status do_visit(const ArrayColumn& column) {
        _shared_found |= !exclusive(column.elements_column()) || !exclusive(column.offsets_column());
        return Status::OK();
    }
    Status do_visit(const MapColumn& column) {
        _shared_found |= !exclusive(column.keys_column()) || !exclusive(column.values_column()) ||
                         !exclusive(column.offsets_column());
        return Status::OK();
    }
    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        return Status::OK();
    }
    // the leaf overloads must take the exact dispatched types: a base-class
    // template like FixedLengthColumnBase<T> only matches with conversion
    // rank and would lose overload resolution to the generic catch-all below
    template <typename T>
    Status do_visit(const FixedLengthColumn<T>& column) {
        return Status::OK();
    }
    template <typename T>
    Status do_visit(const DecimalV3Column<T>& column) {
        return Status::OK();
    }
    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        return Status::OK();
    }
    template <typename ColumnT>
    Status do_visit(const ColumnT&) {
        _shared_found = true;
        return Status::OK();
    }

private:
    bool _shared_found = false;
};
} // namespace

// The raw chunk input stream. all chunks are in memory.
class RawChunkInputStream final : public SpillInputStream {
public:
    RawChunkInputStream(std::vector<ChunkPtr> chunks, Spiller* spiller)
            : _chunks(std::move(chunks)), _spiller(spiller) {
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
    Spiller* _spiller = nullptr;
    DECLARE_RACE_DETECTOR(detect_get_next)
};

StatusOr<ChunkUniquePtr> RawChunkInputStream::get_next(workgroup::YieldContext& yield_ctx, SerdeContext& context) {
    RACE_DETECT(detect_get_next);
    if (_read_idx >= _chunks.size()) {
        DCHECK_EQ(_total_rows, _read_rows);
        return Status::EndOfFile("eos");
    }
    auto chunk = std::move(_chunks[_read_idx++]);
    // consumers are allowed to mutate the returned chunk in place, so the
    // chunk can only be handed over without copying when its whole column
    // tree is exclusively owned
    bool exclusive = chunk.use_count() == 1;
    if (exclusive) {
        ExclusiveOwnershipChecker checker;
        for (const auto& column : chunk->columns()) {
            if (!checker.exclusive(column)) {
                exclusive = false;
                break;
            }
        }
    }
    ChunkUniquePtr res;
    if (exclusive) {
        res = std::make_unique<Chunk>(std::move(*chunk));
    } else {
        res = chunk->clone_unique();
    }
    _read_rows += res->num_rows();
    if (_spiller != nullptr) {
        const auto& metrics = _spiller->metrics();
        if (metrics.restore_from_mem_table_rows != nullptr) {
            COUNTER_UPDATE(metrics.restore_from_mem_table_rows, res->num_rows());
            COUNTER_UPDATE(metrics.restore_from_mem_table_bytes, res->bytes_usage());
        }
    }

    return res;
}

InputStreamPtr detail::make_raw_chunk_input_stream(std::vector<ChunkPtr> chunks, Spiller* spiller) {
    return std::make_shared<RawChunkInputStream>(std::move(chunks), spiller);
}

// method for create input stream
InputStreamPtr SpillInputStream::union_all(const InputStreamPtr& left, const InputStreamPtr& right) {
    return std::make_shared<UnionAllSpilledInputStream>(left, right);
}

InputStreamPtr SpillInputStream::union_all(std::vector<InputStreamPtr>& _streams) {
    return std::make_shared<UnionAllSpilledInputStream>(_streams);
}

InputStreamPtr SpillInputStream::as_stream(std::vector<ChunkPtr> chunks, Spiller* spiller) {
    return detail::make_raw_chunk_input_stream(std::move(chunks), spiller);
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

InputStreamPtr detail::make_buffered_input_stream(int capacity, InputStreamPtr stream, Spiller* spiller) {
    return std::make_shared<BufferedInputStream>(capacity, std::move(stream), spiller);
}

class SequenceInputStream : public SpillInputStream {
public:
    SequenceInputStream(std::vector<BlockPtr> input_blocks, SerdePtr serde, BlockReaderOptions options)
            : _input_blocks(std::move(input_blocks)), _serde(std::move(serde)), _options(options) {}
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
            const auto& metrics = _serde->parent()->metrics();
            _options.read_io_bytes = GET_METRICS(is_remote, metrics, restore_bytes);
            _options.read_io_timer = GET_METRICS(is_remote, metrics, read_io_timer);
            _options.read_io_count = GET_METRICS(is_remote, metrics, read_io_count);
            _options.global_read_bytes = nullptr;
            _options.global_read_io_duration_ns = nullptr;
            if (auto* g = metrics.global(is_remote); g != nullptr) {
                _options.global_read_bytes = g->bytes_read_total.get();
                _options.global_read_io_duration_ns = g->read_io_duration_ns_total.get();
                g->blocks_read_total->increment(1);
            }
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

InputStreamPtr detail::make_sequence_input_stream(std::vector<BlockPtr> input_blocks, SerdePtr serde,
                                                  BlockReaderOptions options) {
    return std::make_shared<SequenceInputStream>(std::move(input_blocks), std::move(serde), options);
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

StatusOr<InputStreamPtr> detail::make_ordered_input_stream(std::vector<InputStreamPtr> input_streams,
                                                           RuntimeState* state, const SerdePtr& serde,
                                                           const SortExecExprs* sort_exprs, const SortDescs* descs,
                                                           Spiller* spiller) {
    auto stream = std::make_shared<OrderedInputStream>(std::move(input_streams), state);
    RETURN_IF_ERROR(stream->init(serde, sort_exprs, descs, spiller));
    return stream;
}

} // namespace starrocks::spill
