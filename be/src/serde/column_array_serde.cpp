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

#include "serde/column_array_serde.h"

#include <fmt/format.h>
#include <streamvbyte.h>
#include <streamvbytedelta.h>

#include <cstdint>
#include <limits>

#include "base/coding.h"
#include "base/status.h"
#include "base/statusor.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "serde/protobuf_serde.h"
#include "types/hll.h"
#include "types/json_value.h"
#include "types/percentile_value.h"
#include "util/compression/compression_headers.h"

namespace starrocks::serde {

static Status check_remaining_size(const uint8_t* current, const uint8_t* end, size_t expected_remains) {
    if (expected_remains > static_cast<size_t>(end - current)) {
        if (config::enable_dcheck_on_serde_failure) {
            DCHECK(false);
        }
        return Status::InternalError(
                fmt::format("Expected remains size {}, but get {}", expected_remains, end - current));
    }
    return Status::OK();
}

constexpr int ENCODE_SIZE_LIMIT = 256;
static uint8_t* write_little_endian_32(uint32_t value, uint8_t* buff) {
    encode_fixed32_le(buff, value);
    return buff + sizeof(value);
}

static StatusOr<const uint8_t*> read_little_endian_32(const uint8_t* buff, const uint8_t* end, uint32_t* value) {
    RETURN_IF_ERROR(check_remaining_size(buff, end, sizeof(uint32_t)));
    *value = decode_fixed32_le(buff);
    return buff + sizeof(*value);
}

static uint8_t* write_little_endian_64(uint64_t value, uint8_t* buff) {
    encode_fixed64_le(buff, value);
    return buff + sizeof(value);
}

static StatusOr<const uint8_t*> read_little_endian_64(const uint8_t* buff, const uint8_t* end, uint64_t* value) {
    RETURN_IF_ERROR(check_remaining_size(buff, end, sizeof(uint64_t)));
    *value = decode_fixed64_le(buff);
    return buff + sizeof(*value);
}

static uint8_t* write_little_endian_8(uint8_t value, uint8_t* buff) {
    *buff = value;
    return buff + sizeof(value);
}

static StatusOr<const uint8_t*> read_little_endian_8(const uint8_t* buff, const uint8_t* end, uint8_t* value) {
    RETURN_IF_ERROR(check_remaining_size(buff, end, sizeof(uint8_t)));
    *value = *buff;
    return buff + sizeof(*value);
}

static uint8_t* write_raw(const void* data, size_t size, uint8_t* buff) {
    strings::memcpy_inlined(buff, data, size);
    return buff + size;
}

static StatusOr<const uint8_t*> read_raw(const uint8_t* buff, const uint8_t* end, void* target, size_t size) {
    RETURN_IF_ERROR(check_remaining_size(buff, end, size));
    strings::memcpy_inlined(target, buff, size);
    return buff + size;
}

inline size_t upper_int32(size_t size) {
    return (3 + size) / 4.0;
}

template <bool sorted_32ints>
uint8_t* encode_integers(const void* data, size_t size, uint8_t* buff, int encode_level) {
    uint64_t encode_size = 0;
    if (sorted_32ints) { // only support sorted 32-bit integers
        encode_size = streamvbyte_delta_encode(reinterpret_cast<const uint32_t*>(data), upper_int32(size),
                                               buff + sizeof(uint64_t), 0);
    } else {
        encode_size =
                streamvbyte_encode(reinterpret_cast<const uint32_t*>(data), upper_int32(size), buff + sizeof(uint64_t));
    }
    buff = write_little_endian_64(encode_size, buff);

    VLOG_ROW << fmt::format("raw size = {}, encoded size = {}, integers compression ratio = {}\n", size, encode_size,
                            encode_size * 1.0 / size);
    return buff + encode_size;
}

template <bool sorted_32ints>
StatusOr<const uint8_t*> decode_integers(const uint8_t* buff, const uint8_t* end, void* target, size_t size) {
    uint64_t encode_size = 0;

    ASSIGN_OR_RETURN(buff, read_little_endian_64(buff, end, &encode_size));

    const uint64_t count64 = upper_int32(size);
    if (UNLIKELY(count64 > std::numeric_limits<uint32_t>::max())) {
        return Status::InternalError(
                fmt::format("streamvbyte count overflow, count = {}, raw size = {}", count64, size));
    }

    const uint32_t count = static_cast<uint32_t>(count64);
    const uint64_t key_len = (static_cast<uint64_t>(count) + 3) / 4;
    if (UNLIKELY(encode_size < key_len)) {
        return Status::InternalError(fmt::format(
                "invalid streamvbyte payload, encoded size {} is smaller than key size {} (count = {}, raw size = {})",
                encode_size, key_len, count, size));
    }

    const uint64_t max_compressed = streamvbyte_max_compressedbytes(count);
    if (UNLIKELY(encode_size > max_compressed)) {
        return Status::InternalError(
                fmt::format("invalid streamvbyte payload, encoded size {} exceeds max compressed size {} (count = {}, "
                            "raw size = {})",
                            encode_size, max_compressed, count, size));
    }

    RETURN_IF_ERROR(check_remaining_size(buff, end, encode_size));

    uint64_t decode_size = 0;
    if (sorted_32ints) {
        decode_size = streamvbyte_delta_decode(buff, (uint32_t*)target, count, 0);
    } else {
        decode_size = streamvbyte_decode(buff, (uint32_t*)target, count);
    }
    if (encode_size != decode_size) {
        return Status::InternalError(fmt::format(
                "encode size does not equal when decoding, encode size = {}, but decode get size = {}, raw size = {}.",
                encode_size, decode_size, size));
    }
    return buff + decode_size;
}

uint8_t* encode_string_lz4(const void* data, size_t size, uint8_t* buff, int encode_level) {
    if (size > LZ4_MAX_INPUT_SIZE) {
        throw std::runtime_error(
                fmt::format("The input size for compression should be less than {}", LZ4_MAX_INPUT_SIZE));
    }
    auto encode_size =
            LZ4_compress_fast(reinterpret_cast<const char*>(data), reinterpret_cast<char*>(buff + sizeof(uint64_t)),
                              size, LZ4_compressBound(size), std::max(1, std::abs(encode_level / 10000) % 100));
    if (encode_size <= 0) {
        throw std::runtime_error(
                fmt::format("lz4 compress failed: raw size = {}, compressed get encode size = {}.", size, encode_size));
    }
    buff = write_little_endian_64(encode_size, buff);

    VLOG_ROW << fmt::format("raw size = {}, encoded size = {}, lz4 compression ratio = {}\n", size, encode_size,
                            encode_size * 1.0 / size);

    return buff + encode_size;
}

StatusOr<const uint8_t*> decode_string_lz4(const uint8_t* buff, const uint8_t* end, void* target, size_t size) {
    uint64_t encode_size = 0;
    ASSIGN_OR_RETURN(buff, read_little_endian_64(buff, end, &encode_size));

    RETURN_IF_ERROR(check_remaining_size(buff, end, encode_size));
    auto decode_size = LZ4_decompress_safe(reinterpret_cast<const char*>(buff), reinterpret_cast<char*>(target),
                                           encode_size, size);
    if (decode_size <= 0) {
        return Status::InternalError(fmt::format(
                "lz4 decompress failed: encode size = {}, raw size = {}, decompressed get decode size = {}.",
                encode_size, size, decode_size));
    }
    if (size != decode_size) {
        return Status::InternalError(
                fmt::format("lz4 encode size does not equal when decoding, encode size = {}, but decode get size = {}, "
                            "raw size = {}.",
                            encode_size, decode_size, size));
    }
    return buff + encode_size;
}

template <typename T, bool sorted>
class FixedLengthColumnSerde {
public:
    static int64_t max_serialized_size(const FixedLengthColumnBase<T>& column, const int encode_level) {
        // NOTE that `serialize` and `deserialize` will store and load the size as uint32_t.
        // If you use `serialize` and `deserialize`, please make sure that the size of the column is less than 2^32.
        int64_t size = sizeof(T) * column.size();
        if (EncodeContext::enable_encode_integer(encode_level) && size >= ENCODE_SIZE_LIMIT) {
            return sizeof(uint32_t) + sizeof(uint64_t) +
                   std::max((int64_t)size, (int64_t)streamvbyte_max_compressedbytes(upper_int32(size)));
        } else {
            return sizeof(uint32_t) + size;
        }
    }

    static uint8_t* serialize(const FixedLengthColumnBase<T>& column, uint8_t* buff, const int encode_level) {
        uint32_t size = sizeof(T) * column.size();
        buff = write_little_endian_32(size, buff);
        if (EncodeContext::enable_encode_integer(encode_level) && size >= ENCODE_SIZE_LIMIT) {
            // sorted 32-bit integers have a better optimize branch
            buff = encode_integers<(sizeof(T) == 4 && sorted)>(column.raw_data(), size, buff, encode_level);
        } else {
            buff = write_raw(column.raw_data(), size, buff);
        }
        return buff;
    }

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end,
                                                FixedLengthColumnBase<T>* column, const int encode_level) {
        uint32_t size = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &size));
        auto& data = column->get_data();
        raw::make_room(&data, size / sizeof(T));
        if (EncodeContext::enable_encode_integer(encode_level) && size >= ENCODE_SIZE_LIMIT) {
            constexpr bool is_sorted_i32 = sizeof(T) == 4 && sorted;
            ASSIGN_OR_RETURN(buff, decode_integers<is_sorted_i32>(buff, end, data.data(), size));
        } else {
            ASSIGN_OR_RETURN(buff, read_raw(buff, end, data.data(), size));
        }
        return buff;
    }
};

class BinaryColumnSerde {
public:
    template <typename T>
    static int64_t max_serialized_size(const BinaryColumnBase<T>& column, const int encode_level) {
        auto bytes = column.get_immutable_bytes();
        const auto& offsets = column.get_offset();
        int64_t res = sizeof(T) * 2;
        int64_t offsets_size = offsets.size() * sizeof(typename BinaryColumnBase<T>::Offset);
        if (EncodeContext::enable_encode_integer(encode_level) && offsets_size >= ENCODE_SIZE_LIMIT) {
            res += sizeof(uint64_t) +
                   std::max((int64_t)offsets_size, (int64_t)streamvbyte_max_compressedbytes(upper_int32(offsets_size)));
        } else {
            res += offsets_size;
        }
        if (EncodeContext::enable_encode_string(encode_level) && bytes.size() >= ENCODE_SIZE_LIMIT) {
            res += sizeof(uint64_t) + std::max((int64_t)bytes.size(), (int64_t)LZ4_compressBound(bytes.size()));
        } else {
            res += bytes.size();
        }
        return res;
    }

    template <typename T>
    static uint8_t* serialize(const BinaryColumnBase<T>& column, uint8_t* buff, const int encode_level) {
        auto bytes = column.get_immutable_bytes();
        const auto& offsets = column.get_offset();

        T bytes_size = bytes.size() * sizeof(uint8_t);
        if constexpr (std::is_same_v<T, uint32_t>) {
            buff = write_little_endian_32(bytes_size, buff);
        } else {
            buff = write_little_endian_64(bytes_size, buff);
        }
        if (EncodeContext::enable_encode_string(encode_level) && bytes_size >= ENCODE_SIZE_LIMIT &&
            bytes_size <= LZ4_MAX_INPUT_SIZE) {
            buff = encode_string_lz4(bytes.data(), bytes_size, buff, encode_level);
        } else {
            buff = write_raw(bytes.data(), bytes_size, buff);
        }

        //TODO: if T is uint32_t, `offsets_size` may be overflow
        T offsets_size = offsets.size() * sizeof(typename BinaryColumnBase<T>::Offset);
        if constexpr (std::is_same_v<T, uint32_t>) {
            buff = write_little_endian_32(offsets_size, buff);
        } else {
            buff = write_little_endian_64(offsets_size, buff);
        }
        if (EncodeContext::enable_encode_integer(encode_level) && offsets_size >= ENCODE_SIZE_LIMIT) {
            if (sizeof(T) == 4) { // only support sorted 32-bit integers
                buff = encode_integers<true>(offsets.data(), offsets_size, buff, encode_level);
            } else {
                buff = encode_integers<false>(offsets.data(), offsets_size, buff, encode_level);
            }
        } else {
            buff = write_raw(offsets.data(), offsets_size, buff);
        }
        return buff;
    }

    template <typename T>
    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, BinaryColumnBase<T>* column,
                                                const int encode_level) {
        // deserialize bytes
        T bytes_size = 0;
        if constexpr (std::is_same_v<T, uint32_t>) {
            ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &bytes_size));
        } else {
            ASSIGN_OR_RETURN(buff, read_little_endian_64(buff, end, &bytes_size));
        }
        column->get_bytes().resize(bytes_size);

        auto* bytes_data = column->get_bytes().data();
        if (EncodeContext::enable_encode_string(encode_level) && bytes_size >= ENCODE_SIZE_LIMIT &&
            bytes_size <= LZ4_MAX_INPUT_SIZE) {
            ASSIGN_OR_RETURN(buff, decode_string_lz4(buff, end, bytes_data, bytes_size));
        } else {
            ASSIGN_OR_RETURN(buff, read_raw(buff, end, bytes_data, bytes_size));
        }

        T offset_bytes_size = 0;
        if constexpr (std::is_same_v<T, uint32_t>) {
            ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &offset_bytes_size));
        } else {
            ASSIGN_OR_RETURN(buff, read_little_endian_64(buff, end, &offset_bytes_size));
        }
        raw::make_room(&column->get_offset(), offset_bytes_size / sizeof(typename BinaryColumnBase<T>::Offset));

        if (EncodeContext::enable_encode_integer(encode_level) && offset_bytes_size >= ENCODE_SIZE_LIMIT) {
            constexpr bool is_i32 = sizeof(T) == 4;
            ASSIGN_OR_RETURN(buff, decode_integers<is_i32>(buff, end, column->get_offset().data(), offset_bytes_size));
        } else {
            ASSIGN_OR_RETURN(buff, read_raw(buff, end, column->get_offset().data(), offset_bytes_size));
        }
        return buff;
    }
};

template <typename T>
class ObjectColumnSerde {
public:
    static int64_t max_serialized_size(const ObjectColumn<T>& column) {
        const auto& pool = column.get_pool();
        int64_t size = sizeof(uint32_t);
        for (const auto& obj : pool) {
            size += sizeof(uint64_t);
            size += obj.serialize_size();
        }
        return size;
    }

    static uint8_t* serialize(const ObjectColumn<T>& column, uint8_t* buff) {
        buff = write_little_endian_32(column.get_pool().size(), buff);
        for (const auto& obj : column.get_pool()) {
            uint64_t actual = obj.serialize(buff + sizeof(uint64_t));
            buff = write_little_endian_64(actual, buff);
            buff += actual;
        }
        return buff;
    }

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, ObjectColumn<T>* column) {
        uint32_t num_objects = 0;

        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &num_objects));
        column->reset_column();
        auto& pool = column->get_pool();
        pool.reserve(num_objects);
        for (int i = 0; i < num_objects; i++) {
            uint64_t serialized_size = 0;
            ASSIGN_OR_RETURN(buff, read_little_endian_64(buff, end, &serialized_size));
            pool.emplace_back(Slice(buff, serialized_size));
            buff += serialized_size;
        }
        return buff;
    }
};

// TODO(mofei) embed the version into JsonColumn
// JsonColumnSerde: serialization of JSON column for network transimission
// The header include a format_version field, indicting the layout of column encoding
class JsonColumnSerde {
public:
    static int64_t max_serialized_size(const JsonColumn& column) {
        const auto& pool = column.get_pool();
        int64_t size = 0;
        size += sizeof(uint32_t); // format_version
        size += sizeof(uint32_t); // num_objects
        for (const auto& obj : pool) {
            size += sizeof(uint64_t);
            size += obj.serialize_size();
        }
        return size;
    }

    // Layout
    // uint32: format_version (currently is hard-coded)
    // uint32: number of datums
    // datums: [size1[payload1][size2][payload2]
    static uint8_t* serialize(const JsonColumn& column, uint8_t* buff) {
        buff = write_little_endian_32(kJsonMetaDefaultFormatVersion, buff);
        buff = write_little_endian_32(column.get_pool().size(), buff);
        for (const auto& obj : column.get_pool()) {
            constexpr uint64_t size_field_length = sizeof(uint64_t);
            uint64_t actual = obj.serialize(buff + size_field_length);
            buff = write_little_endian_64(actual, buff);
            buff += actual;
        }
        return buff;
    }

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, JsonColumn* column) {
        uint32_t actual_version = 0;
        uint32_t num_objects = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &actual_version));
        RETURN_IF_DCHECK_EQ_FAILED(actual_version, kJsonMetaDefaultFormatVersion);
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &num_objects));

        column->reset_column();
        auto& pool = column->get_pool();
        pool.reserve(num_objects);
        for (int i = 0; i < num_objects; i++) {
            uint64_t serialized_size = 0;
            ASSIGN_OR_RETURN(buff, read_little_endian_64(buff, end, &serialized_size));
            pool.emplace_back(Slice(buff, serialized_size));
            buff += serialized_size;
        }
        return buff;
    }
};

class VariantColumnSerde {
public:
    enum class VariantMode : uint8_t {
        kShreddedMode = 1,
    };

    static int64_t max_serialized_size(const VariantColumn& column) {
        int64_t size = 0;
        size += sizeof(uint8_t); // mode flag
        size += max_serialized_size_shredded(column);
        return size;
    }

    static uint8_t* serialize(const VariantColumn& column, uint8_t* buff) {
        buff = write_little_endian_8(static_cast<uint8_t>(VariantMode::kShreddedMode), buff);
        buff = serialize_shredded(column, buff);
        return buff;
    }

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, VariantColumn* column) {
        uint8_t mode_flag = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_8(buff, end, &mode_flag));
        column->reset_column();

        if (mode_flag == static_cast<uint8_t>(VariantMode::kShreddedMode)) {
            return deserialize_shredded(buff, end, column);
        }
        return Status::Corruption(fmt::format("Unknown variant column mode: {}", mode_flag));
    }

private:
    static int64_t max_serialized_size_shredded(const VariantColumn& column) {
        int64_t size = 0;

        // shredded_paths
        size += sizeof(uint32_t); // num_paths
        for (const auto& path : column.shredded_paths()) {
            size += sizeof(uint32_t); // path length
            size += path.size();
        }

        // shredded_types
        size += sizeof(uint32_t); // num_types
        for (const auto& type_desc : column.shredded_types()) {
            size += max_serialized_size_type_descriptor(type_desc);
        }

        // typed_columns
        size += sizeof(uint32_t); // num_typed_columns
        for (const auto& typed_col : column.typed_columns()) {
            size += sizeof(uint8_t); // is_nullable
            size += serde::ColumnArraySerde::max_serialized_size(*typed_col, 0);
        }

        // metadata_column (nullable)
        size += sizeof(uint8_t); // has_metadata
        if (column.has_metadata_column()) {
            size += sizeof(uint8_t); // is_nullable
            size += serde::ColumnArraySerde::max_serialized_size(*column.metadata_column(), 0);
        }

        // remain_value_column (nullable)
        size += sizeof(uint8_t); // has_remain
        if (column.has_remain_value()) {
            size += sizeof(uint8_t); // is_nullable
            size += serde::ColumnArraySerde::max_serialized_size(*column.remain_value_column(), 0);
        }

        return size;
    }

    static int64_t max_serialized_size_type_descriptor(const TypeDescriptor& type_desc) {
        int64_t size = 0;
        size += sizeof(uint32_t); // type (LogicalType)
        size += sizeof(int32_t);  // len
        size += sizeof(int32_t);  // precision
        size += sizeof(int32_t);  // scale
        size += sizeof(uint32_t); // num_children
        for (const auto& child : type_desc.children) {
            size += max_serialized_size_type_descriptor(child);
        }
        size += sizeof(uint32_t); // num_field_names
        for (const auto& name : type_desc.field_names) {
            size += sizeof(uint32_t); // name length
            size += name.size();
        }
        size += sizeof(uint32_t); // num_field_ids
        size += type_desc.field_ids.size() * sizeof(int32_t);
        size += sizeof(uint32_t); // num_field_physical_names
        for (const auto& name : type_desc.field_physical_names) {
            size += sizeof(uint32_t); // name length
            size += name.size();
        }
        return size;
    }

    static uint8_t* serialize_shredded(const VariantColumn& column, uint8_t* buff) {
        // shredded_paths
        const auto& paths = column.shredded_paths();
        buff = write_little_endian_32(paths.size(), buff);
        for (const auto& path : paths) {
            buff = write_little_endian_32(path.size(), buff);
            memcpy(buff, path.data(), path.size());
            buff += path.size();
        }

        // shredded_types
        const auto& types = column.shredded_types();
        buff = write_little_endian_32(types.size(), buff);
        for (const auto& type_desc : types) {
            buff = serialize_type_descriptor(type_desc, buff);
        }

        // typed_columns
        const auto& typed_cols = column.typed_columns();
        buff = write_little_endian_32(typed_cols.size(), buff);
        for (const auto& typed_col : typed_cols) {
            buff = write_little_endian_8(typed_col->is_nullable() ? 1 : 0, buff);
            auto result = serde::ColumnArraySerde::serialize(*typed_col, buff, false, 0);
            if (!result.ok()) {
                LOG(WARNING) << "Failed to serialize typed column: " << result.status();
                return buff;
            }
            buff = result.value();
        }

        // metadata_column
        buff = write_little_endian_8(column.has_metadata_column() ? 1 : 0, buff);
        if (column.has_metadata_column()) {
            buff = write_little_endian_8(column.metadata_column()->is_nullable() ? 1 : 0, buff);
            auto result = serde::ColumnArraySerde::serialize(*column.metadata_column(), buff, false, 0);
            if (!result.ok()) {
                LOG(WARNING) << "Failed to serialize metadata column: " << result.status();
                return buff;
            }
            buff = result.value();
        }

        // remain_value_column
        buff = write_little_endian_8(column.has_remain_value() ? 1 : 0, buff);
        if (column.has_remain_value()) {
            buff = write_little_endian_8(column.remain_value_column()->is_nullable() ? 1 : 0, buff);
            auto result = serde::ColumnArraySerde::serialize(*column.remain_value_column(), buff, false, 0);
            if (!result.ok()) {
                LOG(WARNING) << "Failed to serialize remain column: " << result.status();
                return buff;
            }
            buff = result.value();
        }

        return buff;
    }

    static uint8_t* serialize_type_descriptor(const TypeDescriptor& type_desc, uint8_t* buff) {
        buff = write_little_endian_32(static_cast<uint32_t>(type_desc.type), buff);
        buff = write_little_endian_32(static_cast<uint32_t>(type_desc.len), buff);
        buff = write_little_endian_32(static_cast<uint32_t>(type_desc.precision), buff);
        buff = write_little_endian_32(static_cast<uint32_t>(type_desc.scale), buff);

        // children
        buff = write_little_endian_32(type_desc.children.size(), buff);
        for (const auto& child : type_desc.children) {
            buff = serialize_type_descriptor(child, buff);
        }

        // field_names
        buff = write_little_endian_32(type_desc.field_names.size(), buff);
        for (const auto& name : type_desc.field_names) {
            buff = write_little_endian_32(name.size(), buff);
            memcpy(buff, name.data(), name.size());
            buff += name.size();
        }

        // field_ids
        buff = write_little_endian_32(type_desc.field_ids.size(), buff);
        for (int32_t fid : type_desc.field_ids) {
            buff = write_little_endian_32(static_cast<uint32_t>(fid), buff);
        }

        // field_physical_names
        buff = write_little_endian_32(type_desc.field_physical_names.size(), buff);
        for (const auto& name : type_desc.field_physical_names) {
            buff = write_little_endian_32(name.size(), buff);
            memcpy(buff, name.data(), name.size());
            buff += name.size();
        }

        return buff;
    }

    static StatusOr<const uint8_t*> deserialize_shredded(const uint8_t* buff, const uint8_t* end,
                                                         VariantColumn* column) {
        // shredded_paths
        uint32_t num_paths = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &num_paths));
        std::vector<std::string> paths;
        paths.reserve(num_paths);
        for (uint32_t i = 0; i < num_paths; ++i) {
            uint32_t path_len = 0;
            ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &path_len));
            RETURN_IF_ERROR(check_remaining_size(buff, end, path_len));
            std::string path(reinterpret_cast<const char*>(buff), path_len);
            paths.push_back(std::move(path));
            buff += path_len;
        }

        // shredded_types
        uint32_t num_types = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &num_types));
        if (num_types != num_paths) {
            return Status::Corruption(
                    fmt::format("Shredded type count {} does not match path count {}", num_types, num_paths));
        }
        std::vector<TypeDescriptor> types;
        types.reserve(num_types);
        for (uint32_t i = 0; i < num_types; ++i) {
            TypeDescriptor type_desc;
            ASSIGN_OR_RETURN(buff, deserialize_type_descriptor(buff, end, &type_desc));
            types.push_back(std::move(type_desc));
        }

        // typed_columns
        uint32_t num_typed_cols = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &num_typed_cols));
        if (num_typed_cols != num_paths) {
            return Status::Corruption(
                    fmt::format("Typed column count {} does not match path count {}", num_typed_cols, num_paths));
        }
        MutableColumns typed_cols;
        typed_cols.reserve(num_typed_cols);
        for (uint32_t i = 0; i < num_typed_cols; ++i) {
            uint8_t is_nullable = 0;
            ASSIGN_OR_RETURN(buff, read_little_endian_8(buff, end, &is_nullable));
            MutableColumnPtr col = ColumnHelper::create_column(types[i], is_nullable != 0);
            ASSIGN_OR_RETURN(buff, serde::ColumnArraySerde::deserialize(buff, end, col.get(), false, 0));
            typed_cols.push_back(std::move(col));
        }

        // metadata_column
        // metadata/remain are always BinaryColumn (nulls encoded as binary sentinel payloads).
        // The is_nullable byte is read for wire-format backward compatibility but always ignored.
        uint8_t has_metadata = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_8(buff, end, &has_metadata));
        BinaryColumn::MutablePtr metadata_col;
        if (has_metadata) {
            uint8_t is_nullable_metadata = 0;
            ASSIGN_OR_RETURN(buff, read_little_endian_8(buff, end, &is_nullable_metadata));
            metadata_col = BinaryColumn::create();
            ASSIGN_OR_RETURN(buff, serde::ColumnArraySerde::deserialize(buff, end, metadata_col.get(), false, 0));
        }

        // remain_value_column
        uint8_t has_remain = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_8(buff, end, &has_remain));
        BinaryColumn::MutablePtr remain_col;
        if (has_remain) {
            uint8_t is_nullable_remain = 0;
            ASSIGN_OR_RETURN(buff, read_little_endian_8(buff, end, &is_nullable_remain));
            remain_col = BinaryColumn::create();
            ASSIGN_OR_RETURN(buff, serde::ColumnArraySerde::deserialize(buff, end, remain_col.get(), false, 0));
        }

        column->set_shredded_columns(std::move(paths), std::move(types), std::move(typed_cols), std::move(metadata_col),
                                     std::move(remain_col));

        return buff;
    }

    static StatusOr<const uint8_t*> deserialize_type_descriptor(const uint8_t* buff, const uint8_t* end,
                                                                TypeDescriptor* type_desc) {
        uint32_t type_val = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &type_val));
        type_desc->type = static_cast<LogicalType>(type_val);

        uint32_t len_val = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &len_val));
        type_desc->len = static_cast<int32_t>(len_val);

        uint32_t precision_val = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &precision_val));
        type_desc->precision = static_cast<int32_t>(precision_val);

        uint32_t scale_val = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &scale_val));
        type_desc->scale = static_cast<int32_t>(scale_val);

        // children
        uint32_t num_children = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &num_children));
        type_desc->children.resize(num_children);
        for (uint32_t i = 0; i < num_children; ++i) {
            ASSIGN_OR_RETURN(buff, deserialize_type_descriptor(buff, end, &type_desc->children[i]));
        }

        // field_names
        uint32_t num_field_names = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &num_field_names));
        type_desc->field_names.resize(num_field_names);
        for (uint32_t i = 0; i < num_field_names; ++i) {
            uint32_t name_len = 0;
            ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &name_len));
            RETURN_IF_ERROR(check_remaining_size(buff, end, name_len));
            type_desc->field_names[i].assign(reinterpret_cast<const char*>(buff), name_len);
            buff += name_len;
        }

        // field_ids
        uint32_t num_field_ids = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &num_field_ids));
        type_desc->field_ids.resize(num_field_ids);
        for (uint32_t i = 0; i < num_field_ids; ++i) {
            uint32_t fid = 0;
            ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &fid));
            type_desc->field_ids[i] = static_cast<int32_t>(fid);
        }

        // field_physical_names
        uint32_t num_field_physical_names = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &num_field_physical_names));
        type_desc->field_physical_names.resize(num_field_physical_names);
        for (uint32_t i = 0; i < num_field_physical_names; ++i) {
            uint32_t name_len = 0;
            ASSIGN_OR_RETURN(buff, read_little_endian_32(buff, end, &name_len));
            RETURN_IF_ERROR(check_remaining_size(buff, end, name_len));
            type_desc->field_physical_names[i].assign(reinterpret_cast<const char*>(buff), name_len);
            buff += name_len;
        }

        return buff;
    }
};

class NullableColumnSerde {
public:
    using Serde = serde::ColumnArraySerde;
    static int64_t max_serialized_size(const NullableColumn& column, const int encode_level) {
        return Serde::max_serialized_size(*column.null_column(), encode_level) +
               Serde::max_serialized_size(*column.data_column(), encode_level);
    }

    static StatusOr<uint8_t*> serialize(const NullableColumn& column, uint8_t* buff, const int encode_level) {
        ASSIGN_OR_RETURN(buff, Serde::serialize(*column.null_column(), buff, false, encode_level));
        ASSIGN_OR_RETURN(buff, Serde::serialize(*column.data_column(), buff, false, encode_level));
        return buff;
    }

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, NullableColumn* column,
                                                const int encode_level) {
        ASSIGN_OR_RETURN(buff, Serde::deserialize(buff, end, column->null_column_raw_ptr(), false, encode_level));
        ASSIGN_OR_RETURN(buff, Serde::deserialize(buff, end, column->data_column_raw_ptr(), false, encode_level));
        column->update_has_null();
        return buff;
    }
};

class ArrayColumnSerde {
public:
    using Serde = serde::ColumnArraySerde;
    static int64_t max_serialized_size(const ArrayColumn& column, const int encode_level) {
        return Serde::max_serialized_size(column.offsets(), encode_level) +
               Serde::max_serialized_size(column.elements(), encode_level);
    }

    static StatusOr<uint8_t*> serialize(const ArrayColumn& column, uint8_t* buff, const int encode_level) {
        ASSIGN_OR_RETURN(buff, Serde::serialize(column.offsets(), buff, true, encode_level));
        ASSIGN_OR_RETURN(buff, Serde::serialize(column.elements(), buff, false, encode_level));
        return buff;
    }

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, ArrayColumn* column,
                                                const int encode_level) {
        ASSIGN_OR_RETURN(buff, Serde::deserialize(buff, end, column->offsets_column_raw_ptr(), true, encode_level));
        ASSIGN_OR_RETURN(buff, Serde::deserialize(buff, end, column->elements_column_raw_ptr(), false, encode_level));
        return buff;
    }
};

class MapColumnSerde {
public:
    using Serde = serde::ColumnArraySerde;
    static int64_t max_serialized_size(const MapColumn& column, const int encode_level) {
        return Serde::max_serialized_size(column.offsets(), encode_level) +
               Serde::max_serialized_size(column.keys(), encode_level) +
               Serde::max_serialized_size(column.values(), encode_level);
    }

    static StatusOr<uint8_t*> serialize(const MapColumn& column, uint8_t* buff, const int encode_level) {
        ASSIGN_OR_RETURN(buff, Serde::serialize(column.offsets(), buff, true, encode_level));
        ASSIGN_OR_RETURN(buff, Serde::serialize(column.keys(), buff, false, encode_level));
        ASSIGN_OR_RETURN(buff, Serde::serialize(column.values(), buff, false, encode_level));
        return buff;
    }

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, MapColumn* column,
                                                const int encode_level) {
        ASSIGN_OR_RETURN(buff, Serde::deserialize(buff, end, column->offsets_column_raw_ptr(), true, encode_level));
        ASSIGN_OR_RETURN(buff, Serde::deserialize(buff, end, column->keys_column_raw_ptr(), false, encode_level));
        ASSIGN_OR_RETURN(buff, Serde::deserialize(buff, end, column->values_column_raw_ptr(), false, encode_level));
        return buff;
    }
};

class StructColumnSerde {
public:
    using Serde = serde::ColumnArraySerde;
    static int64_t max_serialized_size(const StructColumn& column, const int encode_level) {
        int64_t size = 0;
        for (const auto& field : column.fields()) {
            size += Serde::max_serialized_size(*field, encode_level);
        }
        return size;
    }

    static StatusOr<uint8_t*> serialize(const StructColumn& column, uint8_t* buff, const int encode_level) {
        for (const auto& field : column.fields()) {
            ASSIGN_OR_RETURN(buff, Serde::serialize(*field, buff, false, encode_level));
        }
        return buff;
    }

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, StructColumn* column,
                                                const int encode_level) {
        for (auto& field : column->fields()) {
            ASSIGN_OR_RETURN(buff, Serde::deserialize(buff, end, field->as_mutable_raw_ptr(), false, encode_level));
        }
        return buff;
    }
};

class ConstColumnSerde {
public:
    using Serde = serde::ColumnArraySerde;
    static int64_t max_serialized_size(const ConstColumn& column, const int encode_level) {
        return /*sizeof(uint64_t)=*/8 + Serde::max_serialized_size(*column.data_column(), encode_level);
    }

    static StatusOr<uint8_t*> serialize(const ConstColumn& column, uint8_t* buff, const int encode_level) {
        buff = write_little_endian_64(column.size(), buff);
        ASSIGN_OR_RETURN(buff, Serde::serialize(*column.data_column(), buff, false, encode_level));
        return buff;
    }

    static StatusOr<const uint8_t*> deserialize(const uint8_t* buff, const uint8_t* end, ConstColumn* column,
                                                const int encode_level) {
        uint64_t size = 0;
        ASSIGN_OR_RETURN(buff, read_little_endian_64(buff, end, &size));
        ASSIGN_OR_RETURN(buff, Serde::deserialize(buff, end, column->data_column_raw_ptr(), false, encode_level));
        column->resize(size);
        return buff;
    }
};

class ColumnSerializedSizeVisitor final : public ColumnVisitorAdapter<ColumnSerializedSizeVisitor> {
public:
    explicit ColumnSerializedSizeVisitor(int64_t init_size, const int encode_level)
            : ColumnVisitorAdapter(this), _size(init_size), _encode_level(encode_level) {}

    Status do_visit(const NullableColumn& column) {
        _size += NullableColumnSerde::max_serialized_size(column, _encode_level);
        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) {
        _size += ConstColumnSerde::max_serialized_size(column, _encode_level);
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        _size += ArrayColumnSerde::max_serialized_size(column, _encode_level);
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        _size += MapColumnSerde::max_serialized_size(column, _encode_level);
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        _size += StructColumnSerde::max_serialized_size(column, _encode_level);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        _size += BinaryColumnSerde::max_serialized_size(column, _encode_level);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        _size += FixedLengthColumnSerde<T, false>::max_serialized_size(column, _encode_level);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        _size += ObjectColumnSerde<T>::max_serialized_size(column);
        return Status::OK();
    }

    Status do_visit(const JsonColumn& column) {
        _size += JsonColumnSerde::max_serialized_size(column);
        return Status::OK();
    }

    Status do_visit(const VariantColumn& column) {
        _size += VariantColumnSerde::max_serialized_size(column);
        return Status::OK();
    }

    int64_t size() const { return _size; }

private:
    int64_t _size;
    int _encode_level;
};

class ColumnSerializingVisitor final : public ColumnVisitorAdapter<ColumnSerializingVisitor> {
public:
    explicit ColumnSerializingVisitor(uint8_t* buff, bool sorted, const int encode_level)
            : ColumnVisitorAdapter(this), _buff(buff), _cur(buff), _sorted(sorted), _encode_level(encode_level) {}

    Status do_visit(const NullableColumn& column) {
        ASSIGN_OR_RETURN(_cur, NullableColumnSerde::serialize(column, _cur, _encode_level));
        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) {
        ASSIGN_OR_RETURN(_cur, ConstColumnSerde::serialize(column, _cur, _encode_level));
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        ASSIGN_OR_RETURN(_cur, ArrayColumnSerde::serialize(column, _cur, _encode_level));
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        ASSIGN_OR_RETURN(_cur, MapColumnSerde::serialize(column, _cur, _encode_level));
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        ASSIGN_OR_RETURN(_cur, StructColumnSerde::serialize(column, _cur, _encode_level));
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        _cur = BinaryColumnSerde::serialize(column, _cur, _encode_level);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        if (_sorted) {
            _cur = FixedLengthColumnSerde<T, true>::serialize(column, _cur, _encode_level);
        } else {
            _cur = FixedLengthColumnSerde<T, false>::serialize(column, _cur, _encode_level);
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        _cur = ObjectColumnSerde<T>::serialize(column, _cur);
        return Status::OK();
    }

    Status do_visit(const JsonColumn& column) {
        _cur = JsonColumnSerde::serialize(column, _cur);
        return Status::OK();
    }

    Status do_visit(const VariantColumn& column) {
        _cur = VariantColumnSerde::serialize(column, _cur);
        return Status::OK();
    }

    uint8_t* cur() const { return _cur; }

    int64_t bytes() const { return _cur - _buff; }

private:
    uint8_t* _buff;
    uint8_t* _cur;
    bool _sorted;
    int _encode_level;
};

class ColumnDeserializingVisitor final : public ColumnVisitorMutableAdapter<ColumnDeserializingVisitor> {
public:
    explicit ColumnDeserializingVisitor(const uint8_t* buff, const uint8_t* end, bool sorted, const int encode_level)
            : ColumnVisitorMutableAdapter(this),
              _buff(buff),
              _end(end),
              _cur(buff),
              _sorted(sorted),
              _encode_level(encode_level) {}

    Status do_visit(NullableColumn* column) {
        ASSIGN_OR_RETURN(_cur, NullableColumnSerde::deserialize(_cur, _end, column, _encode_level));
        return Status::OK();
    }

    Status do_visit(ConstColumn* column) {
        ASSIGN_OR_RETURN(_cur, ConstColumnSerde::deserialize(_cur, _end, column, _encode_level));
        return Status::OK();
    }

    Status do_visit(ArrayColumn* column) {
        ASSIGN_OR_RETURN(_cur, ArrayColumnSerde::deserialize(_cur, _end, column, _encode_level));
        return Status::OK();
    }

    Status do_visit(MapColumn* column) {
        ASSIGN_OR_RETURN(_cur, MapColumnSerde::deserialize(_cur, _end, column, _encode_level));
        return Status::OK();
    }

    Status do_visit(StructColumn* column) {
        ASSIGN_OR_RETURN(_cur, StructColumnSerde::deserialize(_cur, _end, column, _encode_level));
        return Status::OK();
    }

    template <typename T>
    Status do_visit(BinaryColumnBase<T>* column) {
        ASSIGN_OR_RETURN(_cur, BinaryColumnSerde::deserialize(_cur, _end, column, _encode_level));
        return Status::OK();
    }

    template <typename T>
    Status do_visit(FixedLengthColumnBase<T>* column) {
        if (_sorted) {
            using Serd = FixedLengthColumnSerde<T, true>;
            ASSIGN_OR_RETURN(_cur, Serd::deserialize(_cur, _end, column, _encode_level));
        } else {
            using Serd = FixedLengthColumnSerde<T, false>;
            ASSIGN_OR_RETURN(_cur, Serd::deserialize(_cur, _end, column, _encode_level));
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(ObjectColumn<T>* column) {
        ASSIGN_OR_RETURN(_cur, ObjectColumnSerde<T>::deserialize(_cur, _end, column));
        return Status::OK();
    }

    Status do_visit(JsonColumn* column) {
        ASSIGN_OR_RETURN(_cur, JsonColumnSerde::deserialize(_cur, _end, column));
        return Status::OK();
    }

    Status do_visit(VariantColumn* column) {
        ASSIGN_OR_RETURN(_cur, VariantColumnSerde::deserialize(_cur, _end, column));
        return Status::OK();
    }

    const uint8_t* cur() const { return _cur; }

    int64_t bytes() const { return _cur - _buff; }

private:
    const uint8_t* _buff;
    const uint8_t* _end;
    const uint8_t* _cur;
    bool _sorted;
    int _encode_level;
};

int64_t ColumnArraySerde::max_serialized_size(const Column& column, const int encode_level) {
    ColumnSerializedSizeVisitor visitor(0, encode_level);
    auto st = column.accept(&visitor);
    LOG_IF(WARNING, !st.ok()) << st;
    return st.ok() ? visitor.size() : 0;
}

StatusOr<uint8_t*> ColumnArraySerde::serialize(const Column& column, uint8_t* buff, bool sorted,
                                               const int encode_level) {
    ColumnSerializingVisitor visitor(buff, sorted, encode_level);
    RETURN_IF_ERROR(column.accept(&visitor));
    return visitor.cur();
}

StatusOr<const uint8_t*> ColumnArraySerde::deserialize(const uint8_t* buff, const uint8_t* end, Column* column,
                                                       bool sorted, const int encode_level) {
    ColumnDeserializingVisitor visitor(buff, end, sorted, encode_level);
    RETURN_IF_ERROR(column->accept_mutable(&visitor));
    if (visitor.cur() > end) {
        return Status::InvalidArgument("Buffer overflow");
    }
    return visitor.cur();
}

} // namespace starrocks::serde
