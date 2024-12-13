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

<<<<<<< HEAD
=======
#include <cstring>

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
#include "exec/spill/options.h"
#include "exec/spill/spiller.h"
#include "gen_cpp/types.pb.h"
#include "gutil/port.h"
#include "runtime/runtime_state.h"
#include "serde/column_array_serde.h"
#include "serde/encode_context.h"
<<<<<<< HEAD
=======
#include "util/raw_container.h"
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

namespace starrocks::spill {

class ColumnarSerde : public Serde {
public:
<<<<<<< HEAD
    ColumnarSerde(Spiller* parent, ChunkBuilder chunk_builder, std::shared_ptr<serde::EncodeContext> encode_context)
            : Serde(parent), _chunk_builder(std::move(chunk_builder)), _encode_context(std::move(encode_context)) {}
    ~ColumnarSerde() override = default;

    Status prepare() override {
=======
    ColumnarSerde(Spiller* parent, ChunkBuilder chunk_builder)
            : Serde(parent), _chunk_builder(std::move(chunk_builder)) {}
    ~ColumnarSerde() override = default;

    Status prepare() override {
        RACE_DETECT(detect_prepare);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (_encode_context == nullptr) {
            auto column_number = _parent->chunk_builder().column_number();
            auto encode_level = _parent->options().encode_level;
            _encode_context = serde::EncodeContext::get_encode_context_shared_ptr(column_number, encode_level);
        }
        return Status::OK();
    }

<<<<<<< HEAD
    Status serialize(SerdeContext& ctx, const ChunkPtr& chunk, BlockPtr block) override;
    StatusOr<ChunkUniquePtr> deserialize(SerdeContext& ctx, BlockReader* reader) override;

private:
=======
    StatusOr<ChunkUniquePtr> deserialize(SerdeContext& ctx, BlockReader* reader) override;
    Status serialize(RuntimeState* state, SerdeContext& ctx, const ChunkPtr& chunk,
                     const SpillOutputDataStreamPtr& output, bool aligned) override;

private:
    // data format
    // header|encode levels|attachment...
    // header:
    // i32 sequence_id|i64 attachment size
    static constexpr int32_t SEQUENCE_OFFSET = 0;
    static constexpr int32_t ATTACHMENT_SIZE_OFFSET = SEQUENCE_OFFSET + sizeof(int32_t);
    static constexpr int32_t HEADER_SIZE = ATTACHMENT_SIZE_OFFSET + sizeof(int64_t);
    static constexpr int32_t SEQUENCE_MAGIC_ID = 0xface;

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
=======
    DECLARE_RACE_DETECTOR(detect_prepare)
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
Status ColumnarSerde::serialize(SerdeContext& ctx, const ChunkPtr& chunk, BlockPtr block) {
    size_t max_serialized_size = _max_serialized_size(chunk);
    auto& serialize_buffer = ctx.serialize_buffer;
    serialize_buffer.resize(max_serialized_size);

    uint8_t* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    uint8_t* head = buf;

    const auto& columns = chunk->columns();

    std::unique_ptr<uint8_t[]> meta_buf;
    size_t meta_len;
    if (_encode_context == nullptr) {
        SCOPED_TIMER(_parent->metrics().serialize_timer);
        for (const auto& column : columns) {
            buf = serde::ColumnArraySerde::serialize(*column, buf);
            if (UNLIKELY(buf == nullptr)) {
                return Status::InternalError("unsupported column occurs in spill serialize phase");
            }
        }
        serialize_buffer.resize(buf - head);
        // only 8 bytes for serialized size if encoding is disable
        meta_len = sizeof(size_t);
        meta_buf.reset(new uint8_t[meta_len]);
        uint8_t* tmp_buf = meta_buf.get();
        UNALIGNED_STORE64(tmp_buf, serialize_buffer.size());
    } else {
        SCOPED_TIMER(_parent->metrics().serialize_timer);
        auto encode_levels = _get_encode_levels();
        std::vector<std::pair<uint64_t, uint64_t>>
                column_stats; // used to record raw_bytes and encoded_bytes for each column
        column_stats.reserve(columns.size());
=======
Status ColumnarSerde::serialize(RuntimeState* state, SerdeContext& ctx, const ChunkPtr& chunk,
                                const SpillOutputDataStreamPtr& output, bool aligned) {
    raw::RawString& serialize_buffer = ctx.serialize_buffer;
    {
        SCOPED_TIMER(_parent->metrics().serialize_timer);
        size_t ALIGNED_SIZE = 1;
        if (aligned) {
            ALIGNED_SIZE = AlignedBuffer::PAGE_SIZE;
        }
        ctx.serialize_buffer.clear();
        const auto& columns = chunk->columns();
        // header|attachment...
        // i32 sequence_id|i64 chunk size|encode level|attachment(column data)...
        char header_buffer[HEADER_SIZE];
        UNALIGNED_STORE32(header_buffer + SEQUENCE_OFFSET, SEQUENCE_MAGIC_ID);

        size_t encode_level_sizes = columns.size() * sizeof(int32_t);
        size_t max_serialized_size = _max_serialized_size(chunk);
        ctx.serialize_buffer.resize(ALIGN_UP(HEADER_SIZE + encode_level_sizes + max_serialized_size, ALIGNED_SIZE));
        uint8_t* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
        const uint8_t* head = buf;

        // acquire encode level
        auto encode_levels = _get_encode_levels();
        {
            buf = buf + HEADER_SIZE;
            for (auto encode_level : encode_levels) {
                UNALIGNED_STORE32(buf, encode_level);
                buf += sizeof(uint32_t);
            }
        }

        // used to record raw_bytes and encoded_bytes for each column
        std::vector<std::pair<uint64_t, uint64_t>> column_stats;
        column_stats.reserve(columns.size());
        // serialize to io buffer
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        int padding_size = 0;
        for (size_t i = 0; i < columns.size(); i++) {
            uint8_t* begin = buf;
            buf = serde::ColumnArraySerde::serialize(*columns[i], buf, false, encode_levels[i]);
            if (UNLIKELY(buf == nullptr)) {
                return Status::InternalError("unsupported column occurs in spill serialize phase");
            }
<<<<<<< HEAD
            // raw_bytes and encoded_bytes
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            column_stats.emplace_back(columns[i]->byte_size(), buf - begin);
            if (serde::EncodeContext::enable_encode_integer(encode_levels[i])) {
                padding_size = serde::EncodeContext::STREAMVBYTE_PADDING_SIZE;
            }
        }
        _update_encode_stats(column_stats);
<<<<<<< HEAD

        serialize_buffer.resize(buf - head + padding_size);
        // 8 bytes for encoded size, 4 bytes for each column's encode level
        // @TODO(silverbullet233): encode levels can be further encoded to save space if necessary.
        meta_len = sizeof(size_t) + columns.size() * sizeof(uint32_t);
        meta_buf.reset(new uint8_t[meta_len]);
        // fill encoded size
        uint8_t* tmp_buf = meta_buf.get();
        UNALIGNED_STORE64(tmp_buf, serialize_buffer.size());
        tmp_buf += sizeof(size_t);
        // fill encode level
        for (auto encode_level : encode_levels) {
            UNALIGNED_STORE32(tmp_buf, encode_level);
            tmp_buf += sizeof(uint32_t);
        }
    }

    std::vector<Slice> data;
    data.emplace_back(Slice(meta_buf.get(), meta_len));
    data.emplace_back(Slice(serialize_buffer.data(), serialize_buffer.size()));
    {
        SCOPED_TIMER(_parent->metrics().write_io_timer);
        RETURN_IF_ERROR(block->append(data));
    }
    COUNTER_UPDATE(_parent->metrics().flush_bytes, meta_len + serialize_buffer.size());
    _parent->metrics().total_spill_bytes->fetch_add(meta_len + serialize_buffer.size());
    TRACE_SPILL_LOG << "serialize chunk to block: " << block->debug_string()
                    << ", original size: " << chunk->bytes_usage() << ", encoded size: " << serialize_buffer.size();
=======
        // total serialized size
        size_t content_length = buf - head;
        auto align_size = ALIGN_UP(content_length + padding_size, ALIGNED_SIZE);
        serialize_buffer.resize(align_size);
        UNALIGNED_STORE64(header_buffer + ATTACHMENT_SIZE_OFFSET, align_size - HEADER_SIZE);
        memcpy(serialize_buffer.data(), header_buffer, HEADER_SIZE);
    }
    size_t written_bytes = serialize_buffer.size();
    RETURN_IF_ERROR(
            output->append(state, {Slice(serialize_buffer.data(), written_bytes)}, written_bytes, chunk->num_rows()));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    return Status::OK();
}

StatusOr<ChunkUniquePtr> ColumnarSerde::deserialize(SerdeContext& ctx, BlockReader* reader) {
<<<<<<< HEAD
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
=======
    char header_buffer[HEADER_SIZE];

    RETURN_IF_ERROR(reader->read_fully(header_buffer, HEADER_SIZE));

    int32_t sequence_id = UNALIGNED_LOAD32(header_buffer + SEQUENCE_OFFSET);
    int32_t attachment_size = UNALIGNED_LOAD32(header_buffer + ATTACHMENT_SIZE_OFFSET);
    if (sequence_id != SEQUENCE_MAGIC_ID) {
        return Status::InternalError(fmt::format("sequence id mismatch {} vs {}", sequence_id, SEQUENCE_MAGIC_ID));
    }

    auto chunk = _chunk_builder();
    auto& columns = chunk->columns();

    auto& serialize_buffer = ctx.serialize_buffer;
    serialize_buffer.resize(attachment_size);

    auto buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    {
        auto st = reader->read_fully(buf, attachment_size);
        RETURN_IF(st.is_end_of_file(), Status::InternalError("not found enough data in block"));
        RETURN_IF_ERROR(st);
    }

    const uint32_t* encode_levels = nullptr;
    const uint8_t* read_cursor = buf;
    encode_levels = reinterpret_cast<uint32_t*>(serialize_buffer.data());

    read_cursor += columns.size() * sizeof(uint32_t);
    SCOPED_TIMER(_parent->metrics().deserialize_timer);
    for (size_t i = 0; i < columns.size(); i++) {
        read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, columns[i].get(), false, encode_levels[i]);
    }

    TRACE_SPILL_LOG << "deserialize chunk from block: " << reader->debug_string()
                    << ", encoded size: " << attachment_size << ", original size: " << chunk->bytes_usage();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    return chunk;
}

StatusOr<SerdePtr> Serde::create_serde(Spiller* parent) {
<<<<<<< HEAD
    return std::make_shared<ColumnarSerde>(parent, parent->chunk_builder(), nullptr);
=======
    return std::make_shared<ColumnarSerde>(parent, parent->chunk_builder());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
} // namespace starrocks::spill
