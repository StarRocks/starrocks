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

#include "serde/protobuf_serde.h"

#include <utility>

#include "column/chunk_extra_data.h"
#include "column/column_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "util/coding.h"
#include "util/raw_container.h"

namespace starrocks::serde {

int64_t ProtobufChunkSerde::max_serialized_size(const Chunk& chunk, const std::shared_ptr<EncodeContext>& context) {
    int64_t serialized_size = 8; // 4 bytes version plus 4 bytes row number

    if (context == nullptr) {
        for (const auto& column : chunk.columns()) {
            serialized_size += ColumnArraySerde::max_serialized_size(*column);
        }
    } else {
        for (auto i = 0; i < chunk.columns().size(); ++i) {
            serialized_size += ColumnArraySerde::max_serialized_size(*chunk.columns()[i], context->get_encode_level(i));
        }
    }
    return serialized_size;
}

StatusOr<ChunkPB> ProtobufChunkSerde::serialize(const Chunk& chunk, const std::shared_ptr<EncodeContext>& context) {
    StatusOr<ChunkPB> res = serialize_without_meta(chunk, std::move(context));
    if (!res.ok()) return res.status();

    const auto& slot_id_to_index = chunk.get_slot_id_to_index_map();
    const auto& tuple_id_to_index = chunk.get_tuple_id_to_index_map();
    const auto& columns = chunk.columns();

    res->mutable_slot_id_map()->Reserve(static_cast<int>(slot_id_to_index.size()) * 2);
    for (const auto& kv : slot_id_to_index) {
        res->mutable_slot_id_map()->Add(kv.first);
        res->mutable_slot_id_map()->Add(static_cast<int>(kv.second));
    }

    res->mutable_tuple_id_map()->Reserve(static_cast<int>(tuple_id_to_index.size()) * 2);
    for (const auto& kv : tuple_id_to_index) {
        res->mutable_tuple_id_map()->Add(kv.first);
        res->mutable_tuple_id_map()->Add(static_cast<int>(kv.second));
    }

    res->mutable_is_nulls()->Reserve(static_cast<int>(columns.size()));
    for (const auto& column : columns) {
        res->mutable_is_nulls()->Add(column->is_nullable());
    }

    res->mutable_is_consts()->Reserve(static_cast<int>(columns.size()));
    for (const auto& column : columns) {
        res->mutable_is_consts()->Add(column->is_constant());
    }

    DCHECK_EQ(columns.size(), tuple_id_to_index.size() + slot_id_to_index.size());

    // serialize extra meta
    auto* chunk_extra_data =
            chunk.get_extra_data() ? dynamic_cast<ChunkExtraColumnsData*>(chunk.get_extra_data().get()) : nullptr;
    if (chunk_extra_data) {
        auto extra_data_metas = chunk_extra_data->chunk_data_metas();
        res->mutable_extra_data_metas()->Reserve(extra_data_metas.size());
        for (auto& data_meta : extra_data_metas) {
            auto* extra_data_meta_pb = res->add_extra_data_metas();
            *(extra_data_meta_pb->mutable_type_desc()) = data_meta.type.to_protobuf();
            extra_data_meta_pb->set_is_const(data_meta.is_const);
            extra_data_meta_pb->set_is_null(data_meta.is_null);
        }
    }
    return res;
}

StatusOr<ChunkPB> ProtobufChunkSerde::serialize_without_meta(const Chunk& chunk,
                                                             const std::shared_ptr<EncodeContext>& context) {
    ChunkPB chunk_pb;
    chunk_pb.set_compress_type(CompressionTypePB::NO_COMPRESSION);

    std::string* serialized_data = chunk_pb.mutable_data();
    auto max_serialized_size = ProtobufChunkSerde::max_serialized_size(chunk, context);
    auto* chunk_extra_data =
            chunk.get_extra_data() ? dynamic_cast<ChunkExtraColumnsData*>(chunk.get_extra_data().get()) : nullptr;
    if (chunk_extra_data) {
        max_serialized_size += chunk_extra_data->max_serialized_size(0);
    }
    raw::stl_string_resize_uninitialized(serialized_data, max_serialized_size);
    auto* buff = reinterpret_cast<uint8_t*>(serialized_data->data());
    encode_fixed32_le(buff + 0, 1);
    encode_fixed32_le(buff + 4, chunk.num_rows());
    buff = buff + 8;

    int padding_size = 0; // as streamvbyte may read up to 16 extra bytes from the input.
    if (context == nullptr) {
        for (const auto& column : chunk.columns()) {
            buff = ColumnArraySerde::serialize(*column, buff);
            if (UNLIKELY(buff == nullptr)) return Status::InternalError("has unsupported column");
        }
    } else {
        for (auto i = 0; i < chunk.columns().size(); ++i) {
            auto buff_begin = buff;
            buff = ColumnArraySerde::serialize(*chunk.columns()[i], buff, false, context->get_encode_level(i));
            if (UNLIKELY(buff == nullptr)) return Status::InternalError("has unsupported column");
            context->update(i, chunk.columns()[i]->byte_size(), buff - buff_begin);
            if (EncodeContext::enable_encode_integer(context->get_encode_level(i))) { // may be use streamvbyte
                padding_size = context->STREAMVBYTE_PADDING_SIZE;
            }
        }
    }

    // do serialize extra data
    if (chunk_extra_data) {
        buff = chunk_extra_data->serialize(buff);
    }
    chunk_pb.set_serialized_size(buff - reinterpret_cast<const uint8_t*>(serialized_data->data()));
    serialized_data->resize(chunk_pb.serialized_size() + padding_size);
    chunk_pb.set_uncompressed_size(serialized_data->size());
    if (context) {
        VLOG_ROW << "pb serialize data, memory bytes = " << chunk.bytes_usage()
                 << " serialized size = " << chunk_pb.serialized_size()
                 << " uncompressed size = " << chunk_pb.uncompressed_size()
                 << " serialize ratio = " << chunk_pb.serialized_size() * 1.0 / chunk.bytes_usage();
    }
    return std::move(chunk_pb);
}

StatusOr<Chunk> ProtobufChunkSerde::deserialize(const RowDescriptor& row_desc, const ChunkPB& chunk_pb,
                                                const int encode_level) {
    auto res = build_protobuf_chunk_meta(row_desc, chunk_pb);
    if (!res.ok()) {
        return res.status();
    }
    if (!chunk_pb.has_data()) {
        return Status::InvalidArgument("not data in ChunkPB");
    }
    int64_t deserialized_size = 0;
    ProtobufChunkDeserializer deserializer(*res, &chunk_pb, encode_level);
    StatusOr<Chunk> chunk = Status::OK();
    TRY_CATCH_BAD_ALLOC(chunk = deserializer.deserialize(chunk_pb.data(), &deserialized_size));
    if (!chunk.ok()) return chunk;

    // The logic is a bit confusing here.
    // `chunk_pb.data().size()` and `expected` are both "estimated" serialized size. it could be larger than real
    // serialized size.
    // `serialized_size` and `deserialized_size` are both "real" serialized size. it's exactly how much bytes are
    // written into buffer. For some object column types like bitmap/hll/percentile, "estimated" and "real" are not
    // always the same. And for bitmap, sometimes `chunk_pb.data().size()` and `expected` are different. So to fix
    // that problem, we fallback to compare "real" serialized size.
    // We compare "real" serialized size first. It may fails because of backward compatibility. For old version of BE,
    // there is no "serialized_size" this field(which means the value is zero), and we fallback to compare "estimated"
    // serialized size. And for new version of BE, the "real" serialized size always matches, and we can save the cost
    // of calling `ProtobufChunkSerde::max_serialized_size()`.
    if (UNLIKELY(deserialized_size != chunk_pb.serialized_size())) {
        size_t expected = ProtobufChunkSerde::max_serialized_size(*chunk);
        // if encode_level != 0, chunk_pb.data().size() is usually not equal to expected.
        if (encode_level == 0 && UNLIKELY(chunk_pb.data().size() != expected)) {
            return Status::InternalError(strings::Substitute(
                    "deserialize chunk data failed. len: $0, expected: $1, ser_size: $2, deser_size: $3",
                    chunk_pb.data().size(), expected, chunk_pb.serialized_size(), deserialized_size));
        }
    }
    return chunk;
}

StatusOr<Chunk> deserialize_chunk_pb_with_schema(const Schema& schema, std::string_view buff) {
    using ColumnHelper = ColumnHelper;
    using Chunk = Chunk;

    auto* cur = reinterpret_cast<const uint8_t*>(buff.data());

    uint32_t version = decode_fixed32_le(cur);
    if (version != 1) {
        return Status::Corruption("invalid version");
    }
    cur += 4;

    uint32_t rows = decode_fixed32_le(cur);
    cur += 4;

    auto chunk = ChunkHelper::new_chunk(schema, rows);
    for (auto& column : chunk->columns()) {
        cur = ColumnArraySerde::deserialize(cur, column.get());
    }
    return Chunk(std::move(*chunk));
}

static SlotId get_slot_id_by_index(const Chunk::SlotHashMap& slot_id_to_index, int target_index) {
    for (const auto& [slot_id, index] : slot_id_to_index) {
        if (index == target_index) {
            return slot_id;
        }
    }
    return -1;
}

StatusOr<Chunk> ProtobufChunkDeserializer::deserialize(std::string_view buff, int64_t* deserialized_bytes) {
    using ColumnHelper = ColumnHelper;
    using Chunk = Chunk;

    auto* cur = reinterpret_cast<const uint8_t*>(buff.data());

    uint32_t version = decode_fixed32_le(cur);
    if (version != 1) {
        return Status::Corruption(fmt::format("invalid version: {}", version));
    }
    cur += 4;

    uint32_t rows = decode_fixed32_le(cur);
    cur += 4;

    std::vector<ColumnPtr> columns;
    columns.resize(_meta.slot_id_to_index.size() + _meta.tuple_id_to_index.size());
    for (size_t i = 0, sz = _meta.is_nulls.size(); i < sz; ++i) {
        columns[i] = ColumnHelper::create_column(_meta.types[i], _meta.is_nulls[i], _meta.is_consts[i], rows);
    }

    if (_encode_level.empty()) {
        for (auto& column : columns) {
            cur = ColumnArraySerde::deserialize(cur, column.get());
        }
    } else {
        DCHECK(_encode_level.size() == columns.size());
        for (auto i = 0; i < columns.size(); ++i) {
            cur = ColumnArraySerde::deserialize(cur, columns[i].get(), false, _encode_level[i]);
        }
    }

    for (int i = 0; i < columns.size(); ++i) {
        size_t col_num_rows = columns[i]->size();
        if (col_num_rows != rows) {
            SlotId slot_id = get_slot_id_by_index(_meta.slot_id_to_index, i);
            return Status::Corruption(
                    fmt::format("Internal error. Detail: deserialize chunk data failed. column slot id: {}, column row "
                                "count: {}, expected row count: {}. There is probably a bug here.",
                                slot_id, col_num_rows, rows));
        }
    }

    // deserialize extra data
    ChunkExtraDataPtr chunk_extra_data;
    if (!_meta.extra_data_metas.empty()) {
        std::vector<ColumnPtr> extra_columns;
        extra_columns.resize(_meta.extra_data_metas.size());
        for (size_t i = 0, sz = _meta.extra_data_metas.size(); i < sz; ++i) {
            auto extra_meta = _meta.extra_data_metas[i];
            extra_columns[i] =
                    ColumnHelper::create_column(extra_meta.type, extra_meta.is_null, extra_meta.is_const, rows);
        }
        for (auto& column : extra_columns) {
            cur = ColumnArraySerde::deserialize(cur, column.get());
        }
        for (int i = 0; i < extra_columns.size(); ++i) {
            size_t col_num_rows = extra_columns[i]->size();
            if (col_num_rows != rows) {
                return Status::Corruption(
                        fmt::format("Internal error. Detail: deserialize chunk data failed. extra column index: {}, "
                                    "column row count: {}, expected "
                                    "row count: {}. There is probably a bug here.",
                                    i, col_num_rows, rows));
            }
        }
        chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(_meta.extra_data_metas, std::move(extra_columns));
    }

    if (deserialized_bytes != nullptr) *deserialized_bytes = cur - reinterpret_cast<const uint8_t*>(buff.data());
    return Chunk(std::move(columns), _meta.slot_id_to_index, _meta.tuple_id_to_index, std::move(chunk_extra_data));
}

StatusOr<ProtobufChunkMeta> build_protobuf_chunk_meta(const RowDescriptor& row_desc, const ChunkPB& chunk_pb) {
    ProtobufChunkMeta chunk_meta;
    if (UNLIKELY(chunk_pb.is_nulls().empty() || chunk_pb.slot_id_map().empty())) {
        return Status::InternalError("chunk_pb _meta could not be empty");
    }

    for (int i = 0; i < chunk_pb.slot_id_map().size(); i += 2) {
        chunk_meta.slot_id_to_index[chunk_pb.slot_id_map()[i]] = chunk_pb.slot_id_map()[i + 1];
    }

    chunk_meta.is_nulls.resize(chunk_pb.is_nulls().size());
    for (int i = 0; i < chunk_pb.is_nulls().size(); ++i) {
        chunk_meta.is_nulls[i] = chunk_pb.is_nulls()[i];
    }
    chunk_meta.is_consts.resize(chunk_pb.is_nulls().size(), false);

    size_t column_index = 0;
    chunk_meta.types.resize(chunk_pb.is_nulls().size());
    for (auto* tuple_desc : row_desc.tuple_descriptors()) {
        const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();
        for (const auto& kv : chunk_meta.slot_id_to_index) {
            for (auto slot : slots) {
                if (kv.first == slot->id()) {
                    chunk_meta.types[kv.second] = slot->type();
                    ++column_index;
                    break;
                }
            }
        }
    }

    if (UNLIKELY(column_index != chunk_meta.is_nulls.size())) {
        return Status::InternalError("build chunk _meta error");
    }
    return std::move(chunk_meta);
}

} // namespace starrocks::serde
