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

#include "column/column_hash/column_hash.h"

#include <cstring>
#include <memory>
#include <string>

#include "column/array_column.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column_base.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "common/status.h"
#include "runtime/time_types.h"
#include "util/decimal_types.h"
#include "util/hash_util.hpp"

namespace starrocks {

// Hash function type tags
struct FNVHash {
    static constexpr const char* name() { return "FNV"; }
    static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::fnv_hash(data, bytes, seed);
    }
};

struct CRC32Hash {
    static constexpr const char* name() { return "CRC32"; }
    static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::zlib_crc_hash(data, bytes, seed);
    }
};

struct MurmurHash3Hash {
    static constexpr const char* name() { return "MurmurHash3"; }
    static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::murmur_hash3_32(data, bytes, seed);
    }
};

// HashKernel - generic implementation for different column types
template <typename ColumnType>
class HashKernel {
    // General template - can be specialized for different column types
};

// Specialization for FixedLengthColumnBase
template <typename T>
class HashKernel<FixedLengthColumnBase<T>> {
public:
    // Core method: hash a single element
    template <typename HashFunc>
    static void hash_element(const T& value, uint32_t* hash, HashFunc&& hash_func) {
        hash_func(value, hash);
    }

    // Hash a range of elements - reuse hash_element
    template <typename HashFunc>
    static void hash_range(const FixedLengthColumnBase<T>& column, uint32_t* hashes, uint32_t from, uint32_t to,
                           HashFunc&& hash_func) {
        const auto datas = column.immutable_data();
        for (uint32_t i = from; i < to; ++i) {
            hash_element(datas[i], &hashes[i], hash_func);
        }
    }

    // Hash with selection mask - reuse hash_element
    template <typename HashFunc>
    static void hash_with_selection(const FixedLengthColumnBase<T>& column, uint32_t* hashes, uint8_t* selection,
                                    uint16_t from, uint16_t to, HashFunc&& hash_func) {
        const auto datas = column.immutable_data();
        for (uint16_t i = from; i < to; i++) {
            if (selection[i]) {
                hash_element(datas[i], &hashes[i], hash_func);
            }
        }
    }

    // Hash selective indices - reuse hash_element
    template <typename HashFunc>
    static void hash_selective(const FixedLengthColumnBase<T>& column, uint32_t* hashes, uint16_t* sel,
                               uint16_t sel_size, HashFunc&& hash_func) {
        const auto datas = column.immutable_data();
        for (uint16_t i = 0; i < sel_size; i++) {
            hash_element(datas[sel[i]], &hashes[sel[i]], hash_func);
        }
    }

    // Hash single element at index - reuse hash_element
    template <typename HashFunc>
    static void hash_at(const FixedLengthColumnBase<T>& column, uint32_t* hash, uint32_t idx, HashFunc&& hash_func) {
        const auto datas = column.immutable_data();
        hash_element(datas[idx], hash, hash_func);
    }
};

// ColumnHashVisitorImpl - internal implementation
template <typename HashFunction>
class ColumnHashVisitorImpl {
public:
    ColumnHashVisitorImpl(uint32_t* hashes, uint32_t from, uint32_t to)
            : _hashes(hashes), _from(from), _to(to), _selection(nullptr), _sel(nullptr), _sel_size(0) {}

    ColumnHashVisitorImpl(uint32_t* hashes, uint8_t* selection, uint16_t from, uint16_t to)
            : _hashes(hashes), _from(from), _to(to), _selection(selection), _sel(nullptr), _sel_size(0) {}

    ColumnHashVisitorImpl(uint32_t* hashes, uint16_t* sel, uint16_t sel_size)
            : _hashes(hashes), _from(0), _to(0), _selection(nullptr), _sel(sel), _sel_size(sel_size) {}

    ColumnHashVisitorImpl(uint32_t* hash, uint32_t idx)
            : _hashes(hash), _from(idx), _to(idx + 1), _selection(nullptr), _sel(nullptr), _sel_size(0) {}

    // Template method for FixedLengthColumnBase<T>
    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        auto hash_func = [](const T& value, uint32_t* hash) {
            if constexpr (std::is_same_v<HashFunction, CRC32Hash> && (IsDate<T> || IsTimestamp<T>)) {
                std::string str = value.to_string();
                *hash = HashFunction::hash(str.data(), static_cast<int32_t>(str.size()), *hash);
            } else if constexpr (std::is_same_v<HashFunction, CRC32Hash> && IsDecimal<T>) {
                int64_t int_val = value.int_value();
                int32_t frac_val = value.frac_value();
                uint32_t seed = HashFunction::hash(&int_val, sizeof(int_val), *hash);
                *hash = HashFunction::hash(&frac_val, sizeof(frac_val), seed);
            } else if constexpr (std::is_same_v<HashFunction, MurmurHash3Hash>) {
                uint32_t hash_value = 0;
                if constexpr (IsDate<T>) {
                    int64_t long_value = value.julian() - date::UNIX_EPOCH_JULIAN;
                    hash_value = HashFunction::hash(&long_value, sizeof(int64_t), 0);
                } else if constexpr (std::is_same_v<T, int32_t>) {
                    int64_t long_value = value;
                    hash_value = HashFunction::hash(&long_value, sizeof(int64_t), 0);
                } else if constexpr (std::is_same_v<T, int64_t>) {
                    hash_value = HashFunction::hash(&value, sizeof(T), 0);
                } else {
                    DCHECK(false) << "Unsupported type for MurmurHash3";
                    return;
                }
                *hash = hash_value;
            } else {
                *hash = HashFunction::hash(&value, sizeof(T), *hash);
            }
        };
        
        if (_sel != nullptr && _sel_size > 0) {
            HashKernel<FixedLengthColumnBase<T>>::hash_selective(column, _hashes, _sel, _sel_size, hash_func);
        } else if (_selection != nullptr) {
            HashKernel<FixedLengthColumnBase<T>>::hash_with_selection(column, _hashes, _selection, _from, _to, hash_func);
        } else {
            HashKernel<FixedLengthColumnBase<T>>::hash_range(column, _hashes, _from, _to, hash_func);
        }
        return Status::OK();
    }

    // Overloads for specific column types
    Status do_visit(const DecimalV3Column<int32_t>& column) {
        return do_visit(static_cast<const FixedLengthColumnBase<int32_t>&>(column));
    }

    Status do_visit(const DecimalV3Column<int64_t>& column) {
        return do_visit(static_cast<const FixedLengthColumnBase<int64_t>&>(column));
    }

    Status do_visit(const DecimalV3Column<int128_t>& column) {
        return do_visit(static_cast<const FixedLengthColumnBase<int128_t>&>(column));
    }

    Status do_visit(const DecimalV3Column<int256_t>& column) {
        return do_visit(static_cast<const FixedLengthColumnBase<int256_t>&>(column));
    }

    Status do_visit(const BinaryColumn& column) {
        const auto& offsets = column.get_offset();
        const auto& bytes = column.get_bytes();
        if (_sel != nullptr && _sel_size > 0) {
            for (uint16_t i = 0; i < _sel_size; i++) {
                uint16_t idx = _sel[i];
                _hashes[idx] = HashFunction::hash(bytes.data() + offsets[idx],
                                                 static_cast<int32_t>(offsets[idx + 1] - offsets[idx]), _hashes[idx]);
            }
        } else if (_selection != nullptr) {
            for (uint32_t i = _from; i < _to; ++i) {
                if (!_selection[i]) continue;
                _hashes[i] = HashFunction::hash(bytes.data() + offsets[i],
                                               static_cast<int32_t>(offsets[i + 1] - offsets[i]), _hashes[i]);
            }
        } else {
            for (uint32_t i = _from; i < _to; ++i) {
                _hashes[i] = HashFunction::hash(bytes.data() + offsets[i],
                                               static_cast<int32_t>(offsets[i + 1] - offsets[i]), _hashes[i]);
            }
        }
        return Status::OK();
    }

    Status do_visit(const LargeBinaryColumn& column) {
        const auto& offsets = column.get_offset();
        const auto& bytes = column.get_bytes();
        if (_sel != nullptr && _sel_size > 0) {
            for (uint16_t i = 0; i < _sel_size; i++) {
                uint16_t idx = _sel[i];
                _hashes[idx] = HashFunction::hash(bytes.data() + offsets[idx],
                                                 static_cast<int32_t>(offsets[idx + 1] - offsets[idx]), _hashes[idx]);
            }
        } else if (_selection != nullptr) {
            for (uint32_t i = _from; i < _to; ++i) {
                if (!_selection[i]) continue;
                _hashes[i] = HashFunction::hash(bytes.data() + offsets[i],
                                               static_cast<int32_t>(offsets[i + 1] - offsets[i]), _hashes[i]);
            }
        } else {
            for (uint32_t i = _from; i < _to; ++i) {
                _hashes[i] = HashFunction::hash(bytes.data() + offsets[i],
                                               static_cast<int32_t>(offsets[i + 1] - offsets[i]), _hashes[i]);
            }
        }
        return Status::OK();
    }

    Status do_visit(const NullableColumn& column) {
        if (!column.has_null()) {
            ColumnHashVisitor<HashFunction> visitor(_hashes, _from, _to);
            return column.data_column()->accept(&visitor);
        }
        const auto null_data = column.null_column()->immutable_data();
        uint32_t value = 0x9e3779b9;
        uint32_t from = _from;
        while (from < _to) {
            uint32_t new_from = from + 1;
            while (new_from < _to && null_data[from] == null_data[new_from]) {
                ++new_from;
            }
            if (null_data[from]) {
                for (uint32_t i = from; i < new_from; ++i) {
                    _hashes[i] = _hashes[i] ^ (value + (_hashes[i] << 6) + (_hashes[i] >> 2));
                }
            } else {
                ColumnHashVisitor<HashFunction> visitor(_hashes, from, new_from);
                (void)column.data_column()->accept(&visitor);
            }
            from = new_from;
        }
        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) {
        DCHECK(column.size() > 0);
        for (uint32_t i = _from; i < _to; ++i) {
            ColumnHashVisitor<HashFunction> visitor(&_hashes[i], static_cast<uint32_t>(i), static_cast<uint32_t>(i + 1));
            (void)column.data_column()->accept(&visitor);
        }
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        const auto& offsets = column.offsets_column()->immutable_data();
        for (uint32_t i = _from; i < _to; ++i) {
            uint32_t array_size = offsets[i + 1] - offsets[i];
            _hashes[i] = HashFunction::hash(&array_size, sizeof(array_size), _hashes[i]);
            if (array_size > 0) {
                ColumnHashVisitor<HashFunction> visitor(_hashes + i, offsets[i], offsets[i + 1]);
                (void)column.elements_column()->accept(&visitor);
            }
        }
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        const auto& offsets = column.offsets_column()->immutable_data();
        for (uint32_t i = _from; i < _to; ++i) {
            uint32_t map_size = offsets[i + 1] - offsets[i];
            _hashes[i] = HashFunction::hash(&map_size, sizeof(map_size), _hashes[i]);
            if (map_size > 0) {
                ColumnHashVisitor<HashFunction> visitor(_hashes + i, offsets[i], offsets[i + 1]);
                (void)column.keys_column()->accept(&visitor);
                (void)column.values_column()->accept(&visitor);
            }
        }
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        for (const ColumnPtr& field : column.fields()) {
            ColumnHashVisitor<HashFunction> visitor(_hashes, _from, _to);
            (void)field->accept(&visitor);
        }
        return Status::OK();
    }

    Status do_visit(const ObjectColumn<JsonValue>& column) {
        std::string s;
        const auto& pool = column.get_pool();
        for (uint32_t i = _from; i < _to; ++i) {
            s.resize(pool[i].serialize_size());
            size_t size = pool[i].serialize(reinterpret_cast<uint8_t*>(s.data()));
            _hashes[i] = HashFunction::hash(s.data(), static_cast<int32_t>(size), _hashes[i]);
        }
        return Status::OK();
    }

    Status do_visit(const ObjectColumn<VariantValue>& column) {
        std::string s;
        const auto& pool = column.get_pool();
        for (uint32_t i = _from; i < _to; ++i) {
            s.resize(pool[i].serialize_size());
            size_t size = pool[i].serialize(reinterpret_cast<uint8_t*>(s.data()));
            _hashes[i] = HashFunction::hash(s.data(), static_cast<int32_t>(size), _hashes[i]);
        }
        return Status::OK();
    }

private:
    uint32_t* _hashes;
    uint32_t _from;
    uint32_t _to;
    uint8_t* _selection;
    uint16_t* _sel;
    uint16_t _sel_size;
};

// ColumnHashVisitor implementation - forward all visit methods to ColumnHashVisitorImpl
template <typename HashFunction>
ColumnHashVisitor<HashFunction>::ColumnHashVisitor(uint32_t* hashes, uint32_t from, uint32_t to)
        : _impl(std::make_unique<ColumnHashVisitorImpl<HashFunction>>(hashes, from, to)) {}

template <typename HashFunction>
ColumnHashVisitor<HashFunction>::ColumnHashVisitor(uint32_t* hashes, uint8_t* selection, uint16_t from, uint16_t to)
        : _impl(std::make_unique<ColumnHashVisitorImpl<HashFunction>>(hashes, selection, from, to)) {}

template <typename HashFunction>
ColumnHashVisitor<HashFunction>::ColumnHashVisitor(uint32_t* hashes, uint16_t* sel, uint16_t sel_size)
        : _impl(std::make_unique<ColumnHashVisitorImpl<HashFunction>>(hashes, sel, sel_size)) {}

template <typename HashFunction>
ColumnHashVisitor<HashFunction>::ColumnHashVisitor(uint32_t* hash, uint32_t idx)
        : _impl(std::make_unique<ColumnHashVisitorImpl<HashFunction>>(hash, idx)) {}

template <typename HashFunction>
ColumnHashVisitor<HashFunction>::~ColumnHashVisitor() = default;

// Forward all visit methods to ColumnHashVisitorImpl
#define FORWARD_VISIT(ColumnType) \
    template <typename HashFunction> \
    Status ColumnHashVisitor<HashFunction>::visit(const ColumnType& column) { \
        return _impl->do_visit(column); \
    }

FORWARD_VISIT(Int8Column)
FORWARD_VISIT(UInt8Column)
FORWARD_VISIT(Int16Column)
FORWARD_VISIT(UInt16Column)
FORWARD_VISIT(Int32Column)
FORWARD_VISIT(UInt32Column)
FORWARD_VISIT(Int64Column)
FORWARD_VISIT(UInt64Column)
FORWARD_VISIT(Int128Column)
FORWARD_VISIT(FloatColumn)
FORWARD_VISIT(DoubleColumn)
FORWARD_VISIT(DateColumn)
FORWARD_VISIT(TimestampColumn)
FORWARD_VISIT(DecimalColumn)
FORWARD_VISIT(Decimal32Column)
FORWARD_VISIT(Decimal64Column)
FORWARD_VISIT(Decimal128Column)
FORWARD_VISIT(Decimal256Column)
FORWARD_VISIT(FixedLengthColumn<int96_t>)
FORWARD_VISIT(FixedLengthColumn<uint24_t>)
FORWARD_VISIT(FixedLengthColumn<decimal12_t>)
FORWARD_VISIT(BinaryColumn)
FORWARD_VISIT(LargeBinaryColumn)
FORWARD_VISIT(NullableColumn)
FORWARD_VISIT(ConstColumn)
FORWARD_VISIT(ArrayColumn)
FORWARD_VISIT(MapColumn)
FORWARD_VISIT(StructColumn)
FORWARD_VISIT(ObjectColumn<JsonValue>)
FORWARD_VISIT(ObjectColumn<VariantValue>)

#undef FORWARD_VISIT

// Explicit template instantiations
template class ColumnHashVisitorImpl<FNVHash>;
template class ColumnHashVisitorImpl<CRC32Hash>;
template class ColumnHashVisitorImpl<MurmurHash3Hash>;
template class ColumnHashVisitor<FNVHash>;
template class ColumnHashVisitor<CRC32Hash>;
template class ColumnHashVisitor<MurmurHash3Hash>;

} // namespace starrocks

