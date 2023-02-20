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
#include <lz4/lz4.h>
#include <lz4/lz4frame.h>
#include <streamvbyte.h>
#include <streamvbytedelta.h>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "serde/protobuf_serde.h"
#include "types/hll.h"
#include "util/coding.h"
#include "util/json.h"
#include "util/percentile_value.h"

namespace starrocks::serde {
namespace {
constexpr int ENCODE_SIZE_LIMIT = 256;
uint8_t* write_little_endian_32(uint32_t value, uint8_t* buff) {
    encode_fixed32_le(buff, value);
    return buff + sizeof(value);
}

const uint8_t* read_little_endian_32(const uint8_t* buff, uint32_t* value) {
    *value = decode_fixed32_le(buff);
    return buff + sizeof(*value);
}

uint8_t* write_little_endian_64(uint64_t value, uint8_t* buff) {
    encode_fixed64_le(buff, value);
    return buff + sizeof(value);
}

const uint8_t* read_little_endian_64(const uint8_t* buff, uint64_t* value) {
    *value = decode_fixed64_le(buff);
    return buff + sizeof(*value);
}

uint8_t* write_raw(const void* data, size_t size, uint8_t* buff) {
    strings::memcpy_inlined(buff, data, size);
    return buff + size;
}

const uint8_t* read_raw(const uint8_t* buff, void* target, size_t size) {
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
const uint8_t* decode_integers(const uint8_t* buff, void* target, size_t size) {
    uint64_t encode_size = 0;
    buff = read_little_endian_64(buff, &encode_size);
    uint64_t decode_size = 0;
    if (sorted_32ints) {
        decode_size = streamvbyte_delta_decode(buff, (uint32_t*)target, upper_int32(size), 0);
    } else {
        decode_size = streamvbyte_decode(buff, (uint32_t*)target, upper_int32(size));
    }
    if (encode_size != decode_size) {
        throw std::runtime_error(fmt::format(
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
    uint64_t encode_size =
            LZ4_compress_fast(reinterpret_cast<const char*>(data), reinterpret_cast<char*>(buff + sizeof(uint64_t)),
                              size, LZ4_compressBound(size), std::max(1, std::abs(encode_level / 10000) % 100));
    if (encode_size <= 0) {
        throw std::runtime_error("lz4 compress error.");
    }
    buff = write_little_endian_64(encode_size, buff);

    VLOG_ROW << fmt::format("raw size = {}, encoded size = {}, lz4 compression ratio = {}\n", size, encode_size,
                            encode_size * 1.0 / size);

    return buff + encode_size;
}

const uint8_t* decode_string_lz4(const uint8_t* buff, void* target, size_t size) {
    uint64_t encode_size = 0;
    buff = read_little_endian_64(buff, &encode_size);
    uint64_t decode_size = LZ4_decompress_safe(reinterpret_cast<const char*>(buff), reinterpret_cast<char*>(target),
                                               encode_size, size);
    if (decode_size <= 0) {
        throw std::runtime_error("lz4 decompress error.");
    }
    if (size != decode_size) {
        throw std::runtime_error(
                fmt::format("lz4 encode size does not equal when decoding, encode size = {}, but decode get size = "
                            "{}, raw size = {}.",
                            encode_size, decode_size, size));
    }
    return buff + encode_size;
}

template <typename T, bool sorted>
class FixedLengthColumnSerde {
public:
    static int64_t max_serialized_size(const FixedLengthColumnBase<T>& column, const int encode_level) {
        uint32_t size = sizeof(T) * column.size();
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
            if (sizeof(T) == 4 && sorted) { // only support sorted 32-bit integers
                buff = encode_integers<true>(column.raw_data(), size, buff, encode_level);
            } else {
                buff = encode_integers<false>(column.raw_data(), size, buff, encode_level);
            }
        } else {
            buff = write_raw(column.raw_data(), size, buff);
        }
        return buff;
    }

    static const uint8_t* deserialize(const uint8_t* buff, FixedLengthColumnBase<T>* column, const int encode_level) {
        uint32_t size = 0;
        buff = read_little_endian_32(buff, &size);
        std::vector<T>& data = column->get_data();
        raw::make_room(&data, size / sizeof(T));
        if (EncodeContext::enable_encode_integer(encode_level) && size >= ENCODE_SIZE_LIMIT) {
            if (sizeof(T) == 4 && sorted) { // only support sorted 32-bit integers
                buff = decode_integers<true>(buff, data.data(), size);
            } else {
                buff = decode_integers<false>(buff, data.data(), size);
            }
        } else {
            buff = read_raw(buff, data.data(), size);
        }
        return buff;
    }
};

class BinaryColumnSerde {
public:
    template <typename T>
    static int64_t max_serialized_size(const BinaryColumnBase<T>& column, const int encode_level) {
        const auto& bytes = column.get_bytes();
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
        const auto& bytes = column.get_bytes();
        const auto& offsets = column.get_offset();

        T bytes_size = bytes.size() * sizeof(uint8_t);
        if constexpr (std::is_same_v<T, uint32_t>) {
            buff = write_little_endian_32(bytes_size, buff);
        } else {
            buff = write_little_endian_64(bytes_size, buff);
        }
        if (EncodeContext::enable_encode_string(encode_level) && bytes_size >= ENCODE_SIZE_LIMIT) {
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
    static const uint8_t* deserialize(const uint8_t* buff, BinaryColumnBase<T>* column, const int encode_level) {
        T bytes_size = 0;
        if constexpr (std::is_same_v<T, uint32_t>) {
            buff = read_little_endian_32(buff, &bytes_size);
        } else {
            buff = read_little_endian_64(buff, &bytes_size);
        }
        column->get_bytes().resize(bytes_size);
        if (EncodeContext::enable_encode_string(encode_level) && bytes_size >= ENCODE_SIZE_LIMIT) {
            buff = decode_string_lz4(buff, column->get_bytes().data(), bytes_size);
        } else {
            buff = read_raw(buff, column->get_bytes().data(), bytes_size);
        }

        T offsets_size = 0;
        if constexpr (std::is_same_v<T, uint32_t>) {
            buff = read_little_endian_32(buff, &offsets_size);
        } else {
            buff = read_little_endian_64(buff, &offsets_size);
        }
        raw::make_room(&column->get_offset(), offsets_size / sizeof(typename BinaryColumnBase<T>::Offset));
        if (EncodeContext::enable_encode_integer(encode_level) && offsets_size >= ENCODE_SIZE_LIMIT) {
            if (sizeof(T) == 4) { // only support sorted 32-bit integers
                buff = decode_integers<true>(buff, column->get_offset().data(), offsets_size);
            } else {
                buff = decode_integers<false>(buff, column->get_offset().data(), offsets_size);
            }
        } else {
            buff = read_raw(buff, column->get_offset().data(), offsets_size);
        }
        return buff;
    }
};

template <typename T>
class ObjectColumnSerde {
public:
    static int64_t max_serialized_size(const ObjectColumn<T>& column) {
        const std::vector<T>& pool = column.get_pool();
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

    static const uint8_t* deserialize(const uint8_t* buff, ObjectColumn<T>* column) {
        uint32_t num_objects = 0;
        buff = read_little_endian_32(buff, &num_objects);
        column->reset_column();
        std::vector<T>& pool = column->get_pool();
        pool.reserve(num_objects);
        for (int i = 0; i < num_objects; i++) {
            uint64_t serialized_size = 0;
            buff = read_little_endian_64(buff, &serialized_size);
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
        const std::vector<JsonValue>& pool = column.get_pool();
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

    static const uint8_t* deserialize(const uint8_t* buff, JsonColumn* column) {
        uint32_t actual_version = 0;
        uint32_t num_objects = 0;
        buff = read_little_endian_32(buff, &actual_version);
        buff = read_little_endian_32(buff, &num_objects);
        CHECK_EQ(actual_version, kJsonMetaDefaultFormatVersion) << "Only format_version=1 is supported";

        column->reset_column();
        std::vector<JsonValue>& pool = column->get_pool();
        pool.reserve(num_objects);
        for (int i = 0; i < num_objects; i++) {
            uint64_t serialized_size = 0;
            buff = read_little_endian_64(buff, &serialized_size);
            pool.emplace_back(Slice(buff, serialized_size));
            buff += serialized_size;
        }
        return buff;
    }
};

class NullableColumnSerde {
public:
    static int64_t max_serialized_size(const NullableColumn& column, const int encode_level) {
        return serde::ColumnArraySerde::max_serialized_size(*column.null_column(), encode_level) +
               serde::ColumnArraySerde::max_serialized_size(*column.data_column(), encode_level);
    }

    static uint8_t* serialize(const NullableColumn& column, uint8_t* buff, const int encode_level) {
        buff = serde::ColumnArraySerde::serialize(*column.null_column(), buff, false, encode_level);
        buff = serde::ColumnArraySerde::serialize(*column.data_column(), buff, false, encode_level);
        return buff;
    }

    static const uint8_t* deserialize(const uint8_t* buff, NullableColumn* column, const int encode_level) {
        buff = serde::ColumnArraySerde::deserialize(buff, column->null_column().get(), false, encode_level);
        buff = serde::ColumnArraySerde::deserialize(buff, column->data_column().get(), false, encode_level);
        column->update_has_null();
        return buff;
    }
};

class ArrayColumnSerde {
public:
    static int64_t max_serialized_size(const ArrayColumn& column, const int encode_level) {
        return serde::ColumnArraySerde::max_serialized_size(column.offsets(), encode_level) +
               serde::ColumnArraySerde::max_serialized_size(column.elements(), encode_level);
    }

    static uint8_t* serialize(const ArrayColumn& column, uint8_t* buff, const int encode_level) {
        buff = serde::ColumnArraySerde::serialize(column.offsets(), buff, true, encode_level);
        buff = serde::ColumnArraySerde::serialize(column.elements(), buff, false, encode_level);
        return buff;
    }

    static const uint8_t* deserialize(const uint8_t* buff, ArrayColumn* column, const int encode_level) {
        buff = serde::ColumnArraySerde::deserialize(buff, column->offsets_column().get(), true, encode_level);
        buff = serde::ColumnArraySerde::deserialize(buff, column->elements_column().get(), false, encode_level);
        return buff;
    }
};

class MapColumnSerde {
public:
    static int64_t max_serialized_size(const MapColumn& column, const int encode_level) {
        return serde::ColumnArraySerde::max_serialized_size(column.offsets(), encode_level) +
               serde::ColumnArraySerde::max_serialized_size(column.keys(), encode_level) +
               serde::ColumnArraySerde::max_serialized_size(column.values(), encode_level);
    }

    static uint8_t* serialize(const MapColumn& column, uint8_t* buff, const int encode_level) {
        buff = serde::ColumnArraySerde::serialize(column.offsets(), buff, true, encode_level);
        buff = serde::ColumnArraySerde::serialize(column.keys(), buff, false, encode_level);
        buff = serde::ColumnArraySerde::serialize(column.values(), buff, false, encode_level);
        return buff;
    }

    static const uint8_t* deserialize(const uint8_t* buff, MapColumn* column, const int encode_level) {
        buff = serde::ColumnArraySerde::deserialize(buff, column->offsets_column().get(), true, encode_level);
        buff = serde::ColumnArraySerde::deserialize(buff, column->keys_column().get(), false, encode_level);
        buff = serde::ColumnArraySerde::deserialize(buff, column->values_column().get(), false, encode_level);
        return buff;
    }
};

class StructColumnSerde {
public:
    static int64_t max_serialized_size(const StructColumn& column, const int encode_level) {
        int64_t size = 0;
        for (const auto& field : column.fields()) {
            size += serde::ColumnArraySerde::max_serialized_size(*field, encode_level);
        }
        return size;
    }

    static uint8_t* serialize(const StructColumn& column, uint8_t* buff, const int encode_level) {
        for (const auto& field : column.fields()) {
            buff = serde::ColumnArraySerde::serialize(*field, buff, false, encode_level);
        }
        return buff;
    }

    static const uint8_t* deserialize(const uint8_t* buff, StructColumn* column, const int encode_level) {
        for (const auto& field : column->fields_column()) {
            buff = serde::ColumnArraySerde::deserialize(buff, field.get(), false, encode_level);
        }
        return buff;
    }
};

class ConstColumnSerde {
public:
    static int64_t max_serialized_size(const ConstColumn& column, const int encode_level) {
        return /*sizeof(uint64_t)=*/8 +
               serde::ColumnArraySerde::max_serialized_size(*column.data_column(), encode_level);
    }

    static uint8_t* serialize(const ConstColumn& column, uint8_t* buff, const int encode_level) {
        buff = write_little_endian_64(column.size(), buff);
        buff = serde::ColumnArraySerde::serialize(*column.data_column(), buff, false, encode_level);
        return buff;
    }

    static const uint8_t* deserialize(const uint8_t* buff, ConstColumn* column, const int encode_level) {
        uint64_t size = 0;
        buff = read_little_endian_64(buff, &size);
        buff = serde::ColumnArraySerde::deserialize(buff, column->data_column().get(), false, encode_level);
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
        _cur = NullableColumnSerde::serialize(column, _cur, _encode_level);
        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) {
        _cur = ConstColumnSerde::serialize(column, _cur, _encode_level);
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        _cur = ArrayColumnSerde::serialize(column, _cur, _encode_level);
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        _cur = MapColumnSerde::serialize(column, _cur, _encode_level);
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        _cur = StructColumnSerde::serialize(column, _cur, _encode_level);
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
    explicit ColumnDeserializingVisitor(const uint8_t* buff, bool sorted, const int encode_level)
            : ColumnVisitorMutableAdapter(this),
              _buff(buff),
              _cur(buff),
              _sorted(sorted),
              _encode_level(encode_level) {}

    Status do_visit(NullableColumn* column) {
        _cur = NullableColumnSerde::deserialize(_cur, column, _encode_level);
        return Status::OK();
    }

    Status do_visit(ConstColumn* column) {
        _cur = ConstColumnSerde::deserialize(_cur, column, _encode_level);
        return Status::OK();
    }

    Status do_visit(ArrayColumn* column) {
        _cur = ArrayColumnSerde::deserialize(_cur, column, _encode_level);
        return Status::OK();
    }

    Status do_visit(MapColumn* column) {
        _cur = MapColumnSerde::deserialize(_cur, column, _encode_level);
        return Status::OK();
    }

    Status do_visit(StructColumn* column) {
        _cur = StructColumnSerde::deserialize(_cur, column, _encode_level);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(BinaryColumnBase<T>* column) {
        _cur = BinaryColumnSerde::deserialize(_cur, column, _encode_level);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(FixedLengthColumnBase<T>* column) {
        if (_sorted) {
            _cur = FixedLengthColumnSerde<T, true>::deserialize(_cur, column, _encode_level);
        } else {
            _cur = FixedLengthColumnSerde<T, false>::deserialize(_cur, column, _encode_level);
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(ObjectColumn<T>* column) {
        _cur = ObjectColumnSerde<T>::deserialize(_cur, column);
        return Status::OK();
    }

    Status do_visit(JsonColumn* column) {
        _cur = JsonColumnSerde::deserialize(_cur, column);
        return Status::OK();
    }

    const uint8_t* cur() const { return _cur; }

    int64_t bytes() const { return _cur - _buff; }

private:
    const uint8_t* _buff;
    const uint8_t* _cur;
    bool _sorted;
    int _encode_level;
};

} // namespace

int64_t ColumnArraySerde::max_serialized_size(const Column& column, const int encode_level) {
    ColumnSerializedSizeVisitor visitor(0, encode_level);
    auto st = column.accept(&visitor);
    LOG_IF(WARNING, !st.ok()) << st;
    return st.ok() ? visitor.size() : 0;
}

uint8_t* ColumnArraySerde::serialize(const Column& column, uint8_t* buff, bool sorted, const int encode_level) {
    ColumnSerializingVisitor visitor(buff, sorted, encode_level);
    auto st = column.accept(&visitor);
    LOG_IF(WARNING, !st.ok()) << st;
    return st.ok() ? visitor.cur() : nullptr;
}

const uint8_t* ColumnArraySerde::deserialize(const uint8_t* data, Column* column, bool sorted, const int encode_level) {
    ColumnDeserializingVisitor visitor(data, sorted, encode_level);
    auto st = column->accept_mutable(&visitor);
    LOG_IF(WARNING, !st.ok()) << st;
    return st.ok() ? visitor.cur() : nullptr;
}

} // namespace starrocks::serde
