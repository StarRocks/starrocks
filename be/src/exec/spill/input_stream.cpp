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

#include "runtime/sorted_chunks_merger.h"
#include "util/blocking_queue.hpp"

namespace starrocks {
namespace spill {

class BufferedInputStream : public InputStream {
public:
    BufferedInputStream(int capacity, InputStreamPtr stream) : _capacity(capacity), _input_stream(stream) {}
    ~BufferedInputStream() override = default;

    bool is_buffer_full() { return _chunk_buffer.get_size() == _capacity || eof(); }
    bool has_chunk() { return !_chunk_buffer.empty() || eof(); }

    StatusOr<ChunkUniquePtr> get_next() override;
    bool is_ready() override { return has_chunk(); }
    void close() override {}

    bool enable_prefetch() const override { return true; }

    Status prefetch() override;

    StatusOr<ChunkUniquePtr> read_from_buffer();

private:
    int _capacity;
    InputStreamPtr _input_stream;
    UnboundedBlockingQueue<ChunkUniquePtr> _chunk_buffer;
};

StatusOr<ChunkUniquePtr> BufferedInputStream::read_from_buffer() {
    if (_chunk_buffer.empty()) {
        CHECK(eof());
        return Status::EndOfFile("end of stream");
    }
    ChunkUniquePtr res;
    CHECK(_chunk_buffer.try_get(&res));
    return res;
}

StatusOr<ChunkUniquePtr> BufferedInputStream::get_next() {
    if (has_chunk()) {
        return read_from_buffer();
    }
    return _input_stream->get_next();
}

Status BufferedInputStream::prefetch() {
    if (is_buffer_full()) {
        return Status::OK();
    }

    auto res = _input_stream->get_next();
    if (res.ok()) {
        _chunk_buffer.put(std::move(res.value()));
        return Status::OK();
    } else if (res.status().is_end_of_file()) {
        mark_is_eof();
        return Status::OK();
    }
    return res.status();
}

class UnorderedInputStream : public InputStream {
public:
    UnorderedInputStream(const std::vector<BlockPtr>& input_blocks, Formatter* formatter)
            : InputStream(input_blocks), _formatter(formatter) {}

    ~UnorderedInputStream() = default;

    StatusOr<ChunkUniquePtr> get_next() override;

    // @TODO need this?
    bool is_ready() override { return false; }

    void close() override;

private:
    size_t _current_idx = 0;
    Formatter* _formatter;
};

StatusOr<ChunkUniquePtr> UnorderedInputStream::get_next() {
    if (_current_idx >= _input_blocks.size()) {
        return Status::EndOfFile("end of stream");
    }
    // @TODO keep readable here?
    FormatterContext ctx;
    auto res = _formatter->deserialize(ctx, _input_blocks[_current_idx]);
    if (res.status().is_end_of_file()) {
        LOG(INFO) << "read block done, " << _input_blocks[_current_idx]->debug_string();
        _input_blocks[_current_idx].reset();
        _current_idx++;
    } else if (!res.status().ok()) {
        LOG(INFO) << "read block error, " << res.status().to_string();
    }
    if (!res.status().is_end_of_file()) {
        return res;
    }
    return nullptr;
}

void UnorderedInputStream::close() {
    // @TODO what?
    // @TODO clean block
}

// @TODO should be buffered stream?
class OrderedInputStream : public InputStream {
public:
    OrderedInputStream(const std::vector<BlockPtr>& blocks, RuntimeState* state)
            : InputStream(blocks), _merger(state) {}

    ~OrderedInputStream() = default;

    Status init(Formatter* formatter, const SortExecExprs* sort_exprs, const SortDescs* descs);

    StatusOr<ChunkUniquePtr> get_next() override;
    bool is_ready() override { return _merger.is_data_ready(); }
    void close() override {}

    bool enable_prefetch() const override { return true; }

    Status prefetch() override;

private:
    // multiple buffered stream
    std::vector<InputStreamPtr> _input_streams;
    starrocks::CascadeChunkMerger _merger;
    Status _status;
};

Status OrderedInputStream::init(Formatter* formatter, const SortExecExprs* sort_exprs, const SortDescs* descs) {
    // @TODO chunk provider
    std::vector<starrocks::ChunkProvider> chunk_providers;
    for (auto& block : _input_blocks) {
        std::vector<BlockPtr> blocks{block};
        auto stream =
                std::make_shared<BufferedInputStream>(1, std::make_shared<UnorderedInputStream>(blocks, formatter));
        _input_streams.emplace_back(std::move(stream));
        auto input_stream = _input_streams.back();
        auto chunk_provider = [input_stream, this](ChunkUniquePtr* output, bool* eos) {
            if (output == nullptr || eos == nullptr) {
                // @TODO
                return input_stream->is_ready();
            }
            if (!input_stream->is_ready()) {
                return false;
            }
            auto res = input_stream->get_next();
            if (!res.status().ok()) {
                if (!res.status().is_end_of_file()) {
                    // @TODO
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

StatusOr<ChunkUniquePtr> OrderedInputStream::get_next() {
    ChunkUniquePtr chunk;
    bool should_exit = false;
    std::atomic_bool eos = false;
    RETURN_IF_ERROR(_merger.get_next(&chunk, &eos, &should_exit));
    if (should_exit) {
        return std::make_unique<Chunk>();
    }
    if (eos) {
        return Status::EndOfFile("eos");
    }
    return chunk;
}

Status OrderedInputStream::prefetch() {
    // prefetch all stream
    size_t eof_num = 0;
    for (auto& input_stream : _input_streams) {
        RETURN_IF_ERROR(input_stream->prefetch());
        eof_num += input_stream->eof();
    }
    if (eof_num == _input_streams.size()) {
        mark_is_eof();
        return Status::EndOfFile("end of stream");
    }
    return Status::OK();
}

StatusOr<InputStreamPtr> BlockGroup::as_unordered_stream() {
    return std::make_shared<UnorderedInputStream>(_blocks, _formatter);
}

StatusOr<InputStreamPtr> BlockGroup::as_ordered_stream(RuntimeState* state, const SortExecExprs* sort_exprs,
                                                       const SortDescs* sort_descs) {
    auto stream = std::make_shared<OrderedInputStream>(_blocks, state);
    RETURN_IF_ERROR(stream->init(_formatter, sort_exprs, sort_descs));
    return stream;
}

} // namespace spill
} // namespace starrocks