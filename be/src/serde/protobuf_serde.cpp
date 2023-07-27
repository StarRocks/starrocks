// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "serde/protobuf_serde.h"

#include <utility>

#include "column/column_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "serde/column_array_serde.h"
#include "util/coding.h"
#include "util/raw_container.h"

namespace starrocks::serde {

EncodeContext::EncodeContext(const int col_num, const int encode_level) : _session_encode_level(encode_level) {
    for (auto i = 0; i < col_num; ++i) {
        _column_encode_level.emplace_back(_session_encode_level);
        _raw_bytes.emplace_back(0);
        _encoded_bytes.emplace_back(0);
    }
    DCHECK(_session_encode_level != 0);
    // the lowest bit is set and other bits are not zero, then enable adjust.
    if (_session_encode_level & 1 && (_session_encode_level >> 1)) {
        _enable_adjust = true;
    }
}

void EncodeContext::update(const int col_id, uint64_t mem_bytes, uint64_t encode_byte) {
    DCHECK(_session_encode_level != 0);
    if (!_enable_adjust) {
        return;
    }
    // decide to encode or not by the encoding ratio of the first EncodeSamplingNum of every _frequency chunks
    if (_times % _frequency < EncodeSamplingNum) {
        _raw_bytes[col_id] += mem_bytes;
        _encoded_bytes[col_id] += encode_byte;
    }
}

// if encode ratio < EncodeRatioLimit, encode it, otherwise not.
void EncodeContext::_adjust(const int col_id) {
    auto old_level = _column_encode_level[col_id];
    if (_encoded_bytes[col_id] < _raw_bytes[col_id] * EncodeRatioLimit) {
        _column_encode_level[col_id] = _session_encode_level;
    } else {
        _column_encode_level[col_id] = 0;
    }
    if (old_level != _column_encode_level[col_id] || _session_encode_level < -1) {
        VLOG_ROW << "Old encode level " << old_level << " is changed to " << _column_encode_level[col_id]
                 << " because the first " << EncodeSamplingNum << " of " << _frequency << " in total " << _times
                 << " chunks' compression ratio is " << _encoded_bytes[col_id] * 1.0 / _raw_bytes[col_id]
                 << " higher than limit " << EncodeRatioLimit;
    }
    _encoded_bytes[col_id] = 0;
    _raw_bytes[col_id] = 0;
}

void EncodeContext::set_encode_levels_in_pb(ChunkPB* const res) {
    res->mutable_encode_level()->Reserve(static_cast<int>(_column_encode_level.size()));
    for (const auto& level : _column_encode_level) {
        res->mutable_encode_level()->Add(level);
    }
    ++_times;
    // must adjust after writing the current encode_level
    if (_enable_adjust && (_times % _frequency == EncodeSamplingNum)) {
        for (auto col_id = 0; col_id < _column_encode_level.size(); ++col_id) {
            _adjust(col_id);
        }
        _frequency = _frequency > 1000000000 ? _frequency : _frequency * 2;
    }
}

int64_t ProtobufChunkSerde::max_serialized_size(const vectorized::Chunk& chunk,
                                                const std::shared_ptr<EncodeContext>& context) {
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

StatusOr<ChunkPB> ProtobufChunkSerde::serialize(const vectorized::Chunk& chunk,
                                                const std::shared_ptr<EncodeContext>& context) {
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
    return res;
}

StatusOr<ChunkPB> ProtobufChunkSerde::serialize_without_meta(const vectorized::Chunk& chunk,
                                                             const std::shared_ptr<EncodeContext>& context) {
    ChunkPB chunk_pb;
    chunk_pb.set_compress_type(CompressionTypePB::NO_COMPRESSION);

    std::string* serialized_data = chunk_pb.mutable_data();
    raw::stl_string_resize_uninitialized(serialized_data, ProtobufChunkSerde::max_serialized_size(chunk, context));
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

StatusOr<vectorized::Chunk> ProtobufChunkSerde::deserialize(const RowDescriptor& row_desc, const ChunkPB& chunk_pb,
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
    StatusOr<vectorized::Chunk> chunk = Status::OK();
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

StatusOr<vectorized::Chunk> ProtobufChunkDeserializer::deserialize(std::string_view buff, int64_t* deserialized_bytes) {
    using ColumnHelper = vectorized::ColumnHelper;
    using Chunk = vectorized::Chunk;

    auto* cur = reinterpret_cast<const uint8_t*>(buff.data());

    uint32_t version = decode_fixed32_le(cur);
    if (version != 1) {
        return Status::Corruption("invalid version");
    }
    cur += 4;

    uint32_t rows = decode_fixed32_le(cur);
    cur += 4;

<<<<<<< HEAD
    std::vector<vectorized::ColumnPtr> columns;
=======
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
>>>>>>> feb34d54dc ([Enhancement] Make "mismatched row count" more clear (#27969))
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
        size_t col_size = columns[i]->size();
        if (col_size != rows) {
            SlotId slot_id = get_slot_id_by_index(_meta.slot_id_to_index, i);
            return Status::Corruption(
                    fmt::format("Internal error. Detail: deserialize chunk data failed. column slot id: {}, column row "
                                "count: {}, expected row count: {}. There is probably a bug here.",
                                slot_id, col_size, rows));
        }
    }
<<<<<<< HEAD
=======

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
        for (int i = 0; i < columns.size(); ++i) {
            size_t col_size = columns[i]->size();
            if (col_size != rows) {
                return Status::Corruption(
                        fmt::format("Internal error. Detail: deserialize chunk data failed. extra column index: {}, "
                                    "column row count: {}, expected "
                                    "row count: {}. There is probably a bug here.",
                                    i, col_size, rows));
            }
        }
        chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(_meta.extra_data_metas, std::move(extra_columns));
    }

>>>>>>> feb34d54dc ([Enhancement] Make "mismatched row count" more clear (#27969))
    if (deserialized_bytes != nullptr) *deserialized_bytes = cur - reinterpret_cast<const uint8_t*>(buff.data());
    return Chunk(std::move(columns), _meta.slot_id_to_index, _meta.tuple_id_to_index);
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
