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

#include <algorithm>
#include <memory>
#include <utility>

#include "exec/spill/block_manager.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "runtime/sorted_chunks_merger.h"
#include "util/blocking_queue.hpp"
#include "util/defer_op.h"

namespace starrocks::spill {

static const int chunk_buffer_max_size = 2;

class UnionAllSpilledInputStream final : public SpillInputStream {
public:
    UnionAllSpilledInputStream(InputStreamPtr left, InputStreamPtr right) {
        _streams.emplace_back(std::move(left));
        _streams.emplace_back(std::move(right));
    }

    UnionAllSpilledInputStream(std::vector<InputStreamPtr> streams) : _streams(std::move(streams)) {}

    ~UnionAllSpilledInputStream() override = default;

    StatusOr<ChunkUniquePtr> get_next(SerdeContext& ctx) override;

    bool enable_prefetch() const override { return true; }

    Status prefetch(SerdeContext& ctx) override;

    bool is_ready() override {
        if (_current_process_idx < _streams.size()) {
            return _streams[_current_process_idx]->is_ready();
        }
        return true;
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

Status UnionAllSpilledInputStream::prefetch(SerdeContext& ctx) {
    size_t num_eos = 0;
    for (auto& _stream : _streams) {
        if (!_stream->eof()) {
            _stream->prefetch(ctx);
        }
        num_eos += _stream->eof();
    }
    if (num_eos == _streams.size()) {
        mark_is_eof();
        return Status::EndOfFile("eos");
    }
    return Status::OK();
}

StatusOr<ChunkUniquePtr> UnionAllSpilledInputStream::get_next(SerdeContext& context) {
    if (_current_process_idx < _streams.size()) {
        auto chunk_st = _streams[_current_process_idx]->get_next(context);
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
    RawChunkInputStream(std::vector<ChunkPtr> chunks, Spiller* spiller) : _chunks(std::move(chunks)) {}
    StatusOr<ChunkUniquePtr> get_next(SerdeContext& ctx) override;

    bool is_ready() override { return true; };
    void close() override{};

    bool enable_prefetch() const override { return true; }

    Status prefetch(SerdeContext& ctx) override {
        mark_is_eof();
        return Status::EndOfFile("eos");
    }

private:
    size_t read_idx{};
    std::vector<ChunkPtr> _chunks;
    DECLARE_RACE_DETECTOR(detect_get_next)
};

StatusOr<ChunkUniquePtr> RawChunkInputStream::get_next(SerdeContext& context) {
    RACE_DETECT(detect_get_next, var1);
    if (read_idx >= _chunks.size()) {
        return Status::EndOfFile("eos");
    }
    // TODO: make ChunkPtr could convert to ChunkUniquePtr to avoid unused memory copy
    auto res = std::move(_chunks[read_idx++])->clone_unique();
    _chunks[read_idx - 1].reset();

    return res;
}

// method for create input stream
InputStreamPtr SpillInputStream::union_all(const InputStreamPtr& left, const InputStreamPtr& right) {
    return std::make_shared<UnionAllSpilledInputStream>(left, right);
}

InputStreamPtr SpillInputStream::union_all(std::vector<InputStreamPtr>& _streams) {
    return std::make_shared<UnionAllSpilledInputStream>(_streams);
}

InputStreamPtr SpillInputStream::as_stream(std::vector<ChunkPtr> chunks, Spiller* spiller) {
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

    StatusOr<ChunkUniquePtr> get_next(SerdeContext& ctx) override;
    bool is_ready() override { return has_chunk(); }
    void close() override {}

    bool enable_prefetch() const override { return true; }

    Status prefetch(SerdeContext& ctx) override;

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

StatusOr<ChunkUniquePtr> BufferedInputStream::get_next(SerdeContext& ctx) {
    if (has_chunk()) {
        return read_from_buffer();
    }
    CHECK(!_is_prefetching);
    return _input_stream->get_next(ctx);
}

Status BufferedInputStream::prefetch(SerdeContext& ctx) {
    if (is_buffer_full() || eof()) {
        return Status::OK();
    }
    // concurrent prefetch is not allowed, should call _acquire and _release before and after prefetch
    // to ensure that it doesn't happen.
    if (!_acquire()) {
        return Status::OK();
    }
    DeferOp defer([this]() { _release(); });

    auto res = _input_stream->get_next(ctx);
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

class UnorderedInputStream : public SpillInputStream {
public:
    UnorderedInputStream(std::vector<BlockPtr> input_blocks, SerdePtr serde)
            : _input_blocks(std::move(input_blocks)), _serde(std::move(serde)) {}
    ~UnorderedInputStream() override = default;

    StatusOr<ChunkUniquePtr> get_next(SerdeContext& ctx) override;

    bool is_ready() override { return false; }

    void close() override;

private:
    std::vector<BlockPtr> _input_blocks;
    std::shared_ptr<BlockReader> _current_reader;
    size_t _current_idx = 0;
    SerdePtr _serde;
};

StatusOr<ChunkUniquePtr> UnorderedInputStream::get_next(SerdeContext& ctx) {
    if (_current_idx >= _input_blocks.size()) {
        return Status::EndOfFile("end of reading spilled UnorderedInputStream");
    }

    while (true) {
        if (_current_reader == nullptr) {
            _current_reader = _input_blocks[_current_idx]->get_reader();
        }
        auto res = _serde->deserialize(ctx, _current_reader.get());
        if (res.status().is_end_of_file()) {
            _input_blocks[_current_idx].reset();
            _current_reader.reset();
            _current_idx++;
            if (_current_idx >= _input_blocks.size()) {
                return Status::EndOfFile("end of stream");
            }
            // move to the next block
            continue;
        }
        if (res.status().ok() && res.value()->is_empty()) {
            continue;
        }
        if (!res.status().is_end_of_file()) {
            return res;
        }
    }
    __builtin_unreachable();
}

void UnorderedInputStream::close() {}

class OrderedInputStream : public SpillInputStream {
public:
    OrderedInputStream(std::vector<BlockPtr> blocks, RuntimeState* state)
            : _input_blocks(std::move(blocks)), _merger(state) {}

    ~OrderedInputStream() override = default;

    Status init(SerdePtr serde, const SortExecExprs* sort_exprs, const SortDescs* descs, Spiller* spiller);

    StatusOr<ChunkUniquePtr> get_next(SerdeContext& ctx) override;
    bool is_ready() override {
        return _merger.is_data_ready() && std::all_of(_input_streams.begin(), _input_streams.end(),
                                                      [](auto& stream) { return stream->is_ready(); });
    }
    void close() override {}

    bool enable_prefetch() const override { return true; }

    Status prefetch(SerdeContext& ctx) override;

private:
    std::vector<BlockPtr> _input_blocks;
    // multiple buffered stream
    std::vector<InputStreamPtr> _input_streams;
    starrocks::CascadeChunkMerger _merger;
    Status _status;
};

Status OrderedInputStream::init(SerdePtr serde, const SortExecExprs* sort_exprs, const SortDescs* descs,
                                Spiller* spiller) {
    std::vector<starrocks::ChunkProvider> chunk_providers;
    DCHECK(!_input_blocks.empty());

    for (auto& block : _input_blocks) {
        std::vector<BlockPtr> blocks{block};
        auto stream = std::make_shared<BufferedInputStream>(
                chunk_buffer_max_size, std::make_shared<UnorderedInputStream>(blocks, serde), spiller);
        _input_streams.emplace_back(std::move(stream));
        auto input_stream = _input_streams.back();
        auto chunk_provider = [input_stream, this](ChunkUniquePtr* output, bool* eos) {
            if (output == nullptr || eos == nullptr) {
                return input_stream->is_ready();
            }
            if (!input_stream->is_ready()) {
                return false;
            }
            // @TODO(silverbullet233): reuse ctx
            SerdeContext ctx;
            auto res = input_stream->get_next(ctx);
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

StatusOr<ChunkUniquePtr> OrderedInputStream::get_next(SerdeContext& ctx) {
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

Status OrderedInputStream::prefetch(SerdeContext& ctx) {
    // prefetch all stream
    size_t eof_num = 0;
    for (auto& input_stream : _input_streams) {
        RETURN_IF_ERROR(input_stream->prefetch(ctx));
        eof_num += input_stream->eof();
    }
    if (eof_num == _input_streams.size()) {
        mark_is_eof();
        return Status::EndOfFile("end of reading spilled OrderedInputStream");
    }
    return Status::OK();
}

StatusOr<InputStreamPtr> BlockGroup::as_unordered_stream(const SerdePtr& serde, Spiller* spiller) {
    auto stream = std::make_shared<UnorderedInputStream>(_blocks, serde);
    return std::make_shared<BufferedInputStream>(chunk_buffer_max_size, std::move(stream), spiller);
}

StatusOr<InputStreamPtr> BlockGroup::as_ordered_stream(RuntimeState* state, const SerdePtr& serde, Spiller* spiller,
                                                       const SortExecExprs* sort_exprs, const SortDescs* sort_descs) {
    if (_blocks.empty()) {
        return as_unordered_stream(serde, spiller);
    }

    auto stream = std::make_shared<OrderedInputStream>(_blocks, state);
    RETURN_IF_ERROR(stream->init(serde, sort_exprs, sort_descs, spiller));
    return stream;
}

} // namespace starrocks::spill