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

#include "exec/spill/serde.h"

#include "exec/spill/options.h"
#include "exec/spill/spiller.h"
#include "gen_cpp/types.pb.h"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"
#include "serde/encode_context.h"

namespace starrocks::spill {

class ColumnarSerde : public Serde {
public:
    ColumnarSerde(Spiller* parent, ChunkBuilder chunk_builder, std::shared_ptr<serde::EncodeContext> encode_context)
            : Serde(parent), _chunk_builder(std::move(chunk_builder)), _encode_context(std::move(encode_context)) {}
    ~ColumnarSerde() override = default;

    Status prepare() override {
        if (_encode_context == nullptr) {
            auto column_number = _parent->chunk_builder().column_number();
            auto encode_level = _parent->options().encode_level;
            _encode_context = serde::EncodeContext::get_encode_context_shared_ptr(column_number, encode_level);
        }
        return Status::OK();
    }

    Status serialize_to_block(SerdeContext& ctx, const ChunkPtr& chunk, BlockPtr block) override;
    StatusOr<ChunkUniquePtr> deserialize(SerdeContext& ctx, BlockReader* reader) override;

private:
    size_t _max_serialized_size(const ChunkPtr& chunk) const;

    inline const std::vector<uint32_t>& _get_encode_levels() {
        DCHECK(_encode_context != nullptr);
        std::shared_lock l(_mutex);
        return _encode_context->get_encode_levels();
    }

    inline void _update_encode_stats(const std::vector<std::pair<uint64_t, uint64_t>>& column_stats) {
        DCHECK(_encode_context != nullptr);
        std::unique_lock l(_mutex);
        for (size_t i = 0; i < column_stats.size(); i++) {
            _encode_context->update(i, column_stats[i].first, column_stats[i].second);
        }
        _encode_context->adjust_encode_levels();
    }

    ChunkBuilder _chunk_builder;
    // assuming that the chunks processed by the same Spiller are similar,
    // so we maintain a context for each ColumnarSerde, which may be accessed by multiple threads.
    // here a std::shared_mutex is used to ensure concurrency safety.
    std::shared_mutex _mutex;
    std::shared_ptr<serde::EncodeContext> _encode_context;
};

size_t ColumnarSerde::_max_serialized_size(const ChunkPtr& chunk) const {
    size_t total_size = 0;
    const auto& columns = chunk->columns();
    if (_encode_context == nullptr) {
        for (const auto& column : columns) {
            total_size += serde::ColumnArraySerde::max_serialized_size(*column);
        }
    } else {
        for (size_t i = 0; i < columns.size(); i++) {
            total_size +=
                    serde::ColumnArraySerde::max_serialized_size(*columns[i], _encode_context->get_encode_level(i));
        }
    }
    return total_size;
}

Status ColumnarSerde::serialize_to_block(SerdeContext& ctx, const ChunkPtr& chunk, BlockPtr block) {
    const auto& columns = chunk->columns();
    size_t max_serialized_size = _max_serialized_size(chunk);
    auto& serialize_buffer = ctx.aligned_buffer;
    // DIRECT write require ALIGN TO page size
    const size_t PAGE_SIZE = 4096;
    serialize_buffer.resize(
            ALIGN_UP(max_serialized_size + columns.size() * sizeof(uint32_t) + sizeof(size_t), PAGE_SIZE));

    uint8_t* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());

    size_t meta_len = 0;

    if (_encode_context == nullptr) {
        SCOPED_TIMER(_parent->metrics().serialize_timer);
        meta_len = sizeof(size_t);
        uint8_t* head = buf = buf + meta_len;
        for (const auto& column : columns) {
            buf = serde::ColumnArraySerde::serialize(*column, buf);
            if (UNLIKELY(buf == nullptr)) {
                return Status::InternalError("unsupported column occurs in spill serialize phase");
            }
        }
        size_t content_length = buf - head;
        auto align_size = ALIGN_UP(content_length + meta_len, PAGE_SIZE);
        serialize_buffer.resize(align_size);
        // only 8 bytes for serialized size if encoding is disable
        UNALIGNED_STORE64(serialize_buffer.data(), align_size - meta_len);
    } else {
        SCOPED_TIMER(_parent->metrics().serialize_timer);
        // 8 bytes for encoded size, 4 bytes for each column's encode level
        // @TODO(silverbullet233): encode levels can be further encoded to save space if necessary.
        meta_len = sizeof(size_t) + columns.size() * sizeof(uint32_t);
        uint8_t* head = buf = buf + meta_len;

        auto encode_levels = _get_encode_levels();
        // used to record raw_bytes and encoded_bytes for each column
        std::vector<std::pair<uint64_t, uint64_t>> column_stats;
        column_stats.reserve(columns.size());
        int padding_size = 0;
        for (size_t i = 0; i < columns.size(); i++) {
            uint8_t* begin = buf;
            buf = serde::ColumnArraySerde::serialize(*columns[i], buf, false, encode_levels[i]);
            if (UNLIKELY(buf == nullptr)) {
                return Status::InternalError("unsupported column occurs in spill serialize phase");
            }
            // raw_bytes and encoded_bytes
            column_stats.emplace_back(columns[i]->byte_size(), buf - begin);
            if (serde::EncodeContext::enable_encode_integer(encode_levels[i])) {
                padding_size = serde::EncodeContext::STREAMVBYTE_PADDING_SIZE;
            }
        }
        _update_encode_stats(column_stats);
        size_t content_length = buf - head;
        auto align_size = ALIGN_UP(content_length + meta_len + padding_size, PAGE_SIZE);
        serialize_buffer.resize(align_size);

        // fill encoded size
        auto* tmp_buf = serialize_buffer.data();
        UNALIGNED_STORE64(tmp_buf, serialize_buffer.size() - meta_len);
        tmp_buf += sizeof(size_t);
        // fill encode level
        for (auto encode_level : encode_levels) {
            UNALIGNED_STORE32(tmp_buf, encode_level);
            tmp_buf += sizeof(uint32_t);
        }
    }

    std::vector<Slice> data;
    data.emplace_back(Slice(serialize_buffer.data(), serialize_buffer.size()));
    {
        SCOPED_TIMER(_parent->metrics().write_io_timer);
        RETURN_IF_ERROR(block->append(data));
    }
    COUNTER_UPDATE(_parent->metrics().flush_bytes, meta_len + serialize_buffer.size());
    _parent->metrics().total_spill_bytes->fetch_add(meta_len + serialize_buffer.size());
    TRACE_SPILL_LOG << "serialize chunk to block: " << block->debug_string()
                    << ", original size: " << chunk->bytes_usage() << ", encoded size: " << serialize_buffer.size();
    return Status::OK();
}

StatusOr<ChunkUniquePtr> ColumnarSerde::deserialize(SerdeContext& ctx, BlockReader* reader) {
    size_t encoded_size;
    {
        SCOPED_TIMER(_parent->metrics().read_io_timer);
        RETURN_IF_ERROR(reader->read_fully(&encoded_size, sizeof(size_t)));
    }
    size_t read_bytes = sizeof(size_t) + encoded_size;
    std::vector<uint32_t> encode_levels;
    auto chunk = _chunk_builder();
    auto& columns = chunk->columns();
    if (_encode_context != nullptr) {
        // decode encode levels for each column
        SCOPED_TIMER(_parent->metrics().read_io_timer);
        for (size_t i = 0; i < columns.size(); i++) {
            uint32_t encode_level;
            RETURN_IF_ERROR(reader->read_fully(&encode_level, sizeof(uint32_t)));
            encode_levels.push_back(encode_level);
        }
        read_bytes += columns.size() * sizeof(uint32_t);
    }
    auto& serialize_buffer = ctx.serialize_buffer;
    serialize_buffer.resize(encoded_size);

    auto buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    {
        SCOPED_TIMER(_parent->metrics().read_io_timer);
        RETURN_IF_ERROR(reader->read_fully(buf, encoded_size));
    }

    const uint8_t* read_cursor = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    if (_encode_context == nullptr) {
        SCOPED_TIMER(_parent->metrics().deserialize_timer);
        for (auto& column : columns) {
            read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, column.get());
        }
    } else {
        SCOPED_TIMER(_parent->metrics().deserialize_timer);
        for (size_t i = 0; i < columns.size(); i++) {
            read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, columns[i].get(), false, encode_levels[i]);
        }
    }
    COUNTER_UPDATE(_parent->metrics().restore_bytes, read_bytes);
    TRACE_SPILL_LOG << "deserialize chunk from block: " << reader->debug_string() << ", encoded size: " << encoded_size
                    << ", original size: " << chunk->bytes_usage();
    return chunk;
}

StatusOr<SerdePtr> Serde::create_serde(Spiller* parent) {
    return std::make_shared<ColumnarSerde>(parent, parent->chunk_builder(), nullptr);
}
} // namespace starrocks::spill
