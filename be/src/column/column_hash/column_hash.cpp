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
#include <string>
#include <type_traits>

#include "column/array_column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column_base.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "common/status.h"
#include "runtime/time_types.h"
#include "util/hash_util.hpp"

namespace starrocks {

// Concept hash function
template <typename HashFunction>
concept HashFunctionConcept = requires(HashFunction h, const void* data, int32_t bytes, uint32_t seed) {
    { h.hash(data, bytes, seed) }
    ->std::same_as<uint32_t>;
};

// Hash function type tags
struct FNVHash {
    [[always_inline]] static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::fnv_hash(data, bytes, seed);
    }
};

struct CRC32Hash {
    [[always_inline]] static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::zlib_crc_hash(data, bytes, seed);
    }
};

struct MurmurHash3Hash {
    [[always_inline]] static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::murmur_hash3_32(data, bytes, seed);
    }
};

struct XXHash3 {
    [[always_inline]] static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::xx_hash3_64(data, bytes, seed);
    }
};

// TODO: support 64 bits hash function

// SelectorXXX: zero-cost range iterator
// Concept definition for Selector
template <typename Selector>
concept SelectorConcept = requires(Selector s, std::function<void(uint32_t)> fn) {
    { s.for_each(fn) }
    ->std::same_as<void>;
};

// [idx]
struct SelectorSingle {
    SelectorSingle(uint32_t idx) : idx(idx) {}

    uint32_t idx;

    template <class Fn>
    void for_each(Fn&& fn) {
        fn(idx);
    }
};

// [from, to]
struct SelectorRange {
    SelectorRange(uint32_t from, uint32_t to) : from(from), to(to) {}

    uint32_t from;
    uint32_t to;

    template <class Fn>
    void for_each(Fn&& fn) {
        for (uint32_t i = from; i < to; i++) {
            fn(i);
        }
    }

    bool select(uint32_t idx) { return from <= idx && idx < to; }
};

// [sel[0], sel[1], ..., sel[sel_size - 1]]
struct SelectorSelective {
    SelectorSelective(uint16_t* sel, uint16_t sel_size) : sel(sel), sel_size(sel_size) {}

    uint16_t* sel;
    uint16_t sel_size;

    template <class Fn>
    void for_each(Fn&& fn) {
        for (uint16_t i = 0; i < sel_size; i++) {
            fn(sel[i]);
        }
    }
};

// [selection[from], selection[from + 1], ..., selection[to - 1]] if selection[n] is 1
struct SelectorSelection {
    SelectorSelection(uint8_t* selection, uint32_t from, uint32_t to) : selection(selection), from(from), to(to) {}

    uint8_t* selection;
    uint32_t from;
    uint32_t to;

    template <class Fn>
    void for_each(Fn&& fn) {
        for (uint32_t i = from; i < to; i++) {
            if (selection[i]) {
                fn(i);
            }
        }
    }

    bool select(uint32_t idx) { return from <= idx && idx < to && selection[idx]; }
};

template <HashFunctionConcept HashFunction, SelectorConcept SelectorType>
class ColumnHashVisitor final : public ColumnVisitorAdapter<ColumnHashVisitor<HashFunction, SelectorType>> {
public:
    ColumnHashVisitor(uint32_t* hashes, SelectorType selector)
            : ColumnVisitorAdapter<ColumnHashVisitor<HashFunction, SelectorType>>(this),
              _hashes(hashes),
              _selector(selector) {}

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        auto hash_value = [](const T& value, uint32_t* hash) {
            // Specialized implementation for CRC32, for compatibility
            if constexpr (std::is_same_v<HashFunction, CRC32Hash> && (IsDate<T> || IsTimestamp<T>)) {
                // TODO: don't create the string
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
                    // Julian Day -> epoch day
                    // TODO, This is not a good place to do a project, this is just for test.
                    // If we need to make it more general, we should do this project in `IcebergMurmurHashProject`
                    // but consider that use date type column as bucket transform is rare, we can do it later.
                    int64_t long_value = value.julian() - date::UNIX_EPOCH_JULIAN;
                    hash_value = HashFunction::hash(&long_value, sizeof(int64_t), 0);
                } else if constexpr (std::is_integral_v<T>) {
                    // Integer and long hash results must be identical for all integer values.
                    // This ensures that schema evolution does not change bucket partition values if integer types are promoted.
                    int64_t long_value = value;
                    hash_value = HashFunction::hash(&long_value, sizeof(int64_t), 0);
                } else {
                    // for decimal/timestamp type, the storage is very different from iceberg,
                    // and consider they are merely used, these types are forbidden by fe
                    return Status::NotSupported("Unsupported type for MurmurHash3");
                }
                *hash = hash_value;
            } else {
                *hash = HashFunction::hash(&value, sizeof(T), *hash);
            }
        };

        const auto datas = column.immutable_data();
        _selector.for_each([&](uint32_t idx) {
            uint32_t* slot_ptr = slot(idx);
            hash_value(datas[idx], slot_ptr);
        });
        return Status::OK();
    }

    // Overloads for specific column types - DecimalV3Column inherits from FixedLengthColumnBase
    // so we can directly call the template version
    template <typename T>
    Status do_visit(const DecimalV3Column<T>& column) {
        if constexpr (std::is_same_v<HashFunction, CRC32Hash>) {
            // Specialized implementation for CRC32, for compatibility
            // When decimal-v2 columns are used as distribution keys and users try to upgrade
            // decimal-v2 column to decimal-v3 by schema change, decimal128(27,9) shall be the
            // only acceptable target type, so keeping result of crc32_hash on type decimal128(27,9)
            // compatible with type decimal-v2 is required in order to keep data layout consistency.
            const auto data = column.immutable_data();
            auto precision = column.precision();
            auto scale = column.scale();
            if constexpr (std::is_same_v<T, int128_t>) {
                if (precision == 27 && scale == 9) {
                    _selector.for_each([&](uint32_t idx) {
                        uint32_t* slot_ptr = slot(idx);
                        auto& decimal_v2_value = (DecimalV2Value&)(data[idx]);
                        int64_t int_val = decimal_v2_value.int_value();
                        int32_t frac_val = decimal_v2_value.frac_value();
                        uint32_t seed = HashFunction::hash(&int_val, sizeof(int_val), *slot_ptr);
                        *slot_ptr = HashFunction::hash(&frac_val, sizeof(frac_val), seed);
                    });
                    return Status::OK();
                }
            }
            _selector.for_each([&](uint32_t idx) {
                uint32_t* slot_ptr = slot(idx);
                *slot_ptr = HashFunction::hash(&data[idx], sizeof(T), *slot_ptr);
            });
            return Status::OK();
        } else {
            return do_visit(static_cast<const FixedLengthColumnBase<T>&>(column));
        }
    }

    template <typename SizeT>
    Status do_visit(const BinaryColumnBase<SizeT>& column) {
        const auto& offsets = column.get_offset();
        const auto& bytes = column.get_bytes();
        _selector.for_each([&](uint32_t idx) {
            uint32_t* slot_ptr = slot(idx);
            auto len = offsets[idx + 1] - offsets[idx];
            // For empty strings, don't modify the hash (hash with 0 bytes preserves the seed)
            if (len > 0) {
                *slot_ptr = HashFunction::hash(bytes.data() + offsets[idx], static_cast<int32_t>(len), *slot_ptr);
            }
        });
        return Status::OK();
    }

    Status do_visit(const NullableColumn& column) {
        if (!column.has_null()) {
            ColumnHashVisitor<HashFunction, SelectorType> visitor(_hashes, _selector);
            return column.data_column()->accept(&visitor);
        }

        const auto null_data = column.null_column()->immutable_data();
        constexpr uint32_t value = 0x9e3779b9;
        auto mix_null = [&](uint32_t idx) {
            uint32_t* slot_ptr = slot(idx);
            if constexpr (std::is_same_v<HashFunction, CRC32Hash>) {
                // Old behavior: treat NULL as 0 for CRC32
                constexpr uint32_t INT_VALUE = 0;
                *slot_ptr = HashUtil::zlib_crc_hash(&INT_VALUE, 4, *slot_ptr);
            } else if constexpr (std::is_same_v<HashFunction, MurmurHash3Hash>) {
                // For MurmurHash3, skip nulls
            } else {
                *slot_ptr = *slot_ptr ^ (value + (*slot_ptr << 6) + (*slot_ptr >> 2));
            }
        };

        // TODO: optimize performance for sparse nulls
        // Fast path: no selection arrays involved, so we can work on continuous ranges.
        if constexpr (std::is_same_v<SelectorType, SelectorRange> || std::is_same_v<SelectorType, SelectorSelection>) {
            uint32_t cursor = _selector.from;
            while (cursor < _selector.to) {
                uint32_t next = cursor + 1;
                while (next < _selector.to && null_data[next] == null_data[cursor]) {
                    ++next;
                }
                if (null_data[cursor]) {
                    for (uint32_t idx = cursor; idx < next; ++idx) {
                        if (!_selector.select(idx)) continue;
                        mix_null(idx);
                    }
                } else {
                    SelectorType new_selector = _selector;
                    new_selector.from = cursor;
                    new_selector.to = next;
                    ColumnHashVisitor<HashFunction, SelectorType> visitor(_hashes, new_selector);
                    (void)column.data_column()->accept(&visitor);
                }
                cursor = next;
            }
            return Status::OK();
        }

        // Fallback: respect selection/sel arrays by handling row by row.
        // TODO: optimize performance
        _selector.for_each([&](uint32_t idx) {
            if (null_data[idx]) {
                mix_null(idx);
            } else {
                ColumnHashVisitor<HashFunction, SelectorSingle> visitor(slot(idx), SelectorSingle(idx));
                (void)column.data_column()->accept(&visitor);
            }
        });
        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) {
        DCHECK(column.size() > 0);
        const ColumnPtr& data = column.data_column();
        _selector.for_each([&](uint32_t idx) {
            uint32_t* slot_ptr = slot(idx);
            ColumnHashVisitor<HashFunction, SelectorSingle> visitor(slot_ptr, SelectorSingle(0));
            (void)data->accept(&visitor);
        });
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        const auto& offsets = column.offsets_column()->immutable_data();
        const ColumnPtr& elements = column.elements_column();
        _selector.for_each([&](uint32_t idx) {
            size_t array_size = offsets[idx + 1] - offsets[idx];
            uint32_t* slot_ptr = slot(idx);
            *slot_ptr = HashFunction::hash(&array_size, sizeof(array_size), *slot_ptr);
            if (array_size > 0) {
                // TODO: optimize performance, don't hash each element one by one, but hash the array as a whole
                // Hash each element with the same seed (the array's hash value)
                // This matches the pattern: elements->crc32_hash(&hash_value - i, i, i + 1)
                for (uint32_t i = offsets[idx]; i < offsets[idx + 1]; ++i) {
                    ColumnHashVisitor<HashFunction, SelectorSingle> visitor(slot_ptr, SelectorSingle(i));
                    (void)elements->accept(&visitor);
                }
            }
        });
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        const auto& offsets = column.offsets_column()->immutable_data();
        const ColumnPtr& keys = column.keys_column();
        const ColumnPtr& values = column.values_column();

        _selector.for_each([&](uint32_t idx) {
            // Should use size_t not uint32_t for compatible
            uint32_t offset = offsets[idx];
            size_t map_size = offsets[idx + 1] - offsets[idx];
            uint32_t* slot_ptr = slot(idx);
            *slot_ptr = HashFunction::hash(&map_size, sizeof(map_size), *slot_ptr);
            uint32_t base_hash = *slot_ptr;
            for (size_t i = 0; i < map_size; i++) {
                uint32_t pair_hash = base_hash;
                ColumnHashVisitor<HashFunction, SelectorSingle> visitor(&pair_hash, SelectorSingle(offset + i));
                (void)keys->accept(&visitor);
                (void)values->accept(&visitor);

                // for get same hash on un-order map, we need to satisfies the commutative law
                *slot_ptr += pair_hash;
            }
        });
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        for (const ColumnPtr& field : column.fields()) {
            (void)field->accept(this);
        }
        return Status::OK();
    }

    Status do_visit(const JsonColumn& column) {
        _selector.for_each([&](uint32_t idx) {
            int64_t h = column.get_object(idx)->hash();
            uint32_t* slot_ptr = slot(idx);
            *slot_ptr = HashFunction::hash(&h, sizeof(h), *slot_ptr);
        });
        return Status::OK();
    }

    Status do_visit(const VariantColumn& column) { return Status::NotSupported("VariantColumn is not supported"); }

    template <typename ObjectType>
    Status do_visit(const ObjectColumn<ObjectType>& column) {
        std::string buffer;
        const auto& pool = column.get_pool();
        _selector.for_each([&](uint32_t idx) {
            buffer.resize(pool[idx].serialize_size());
            size_t size = pool[idx].serialize(reinterpret_cast<uint8_t*>(buffer.data()));
            uint32_t* slot_ptr = slot(idx);
            *slot_ptr = HashFunction::hash(buffer.data(), static_cast<int32_t>(size), *slot_ptr);
        });
        return Status::OK();
    }

private:
    [[always_inline]] uint32_t* slot(uint32_t idx) const {
        if constexpr (std::is_same_v<SelectorType, SelectorSingle>) {
            return _hashes;
        } else {
            return _hashes + idx;
        }
    }

private:
    // output hash values
    uint32_t* _hashes;
    SelectorType _selector;
};

// FNV Hash
void fnv_hash_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to) {
    ColumnHashVisitor<FNVHash, SelectorRange> visitor(hashes, SelectorRange(from, to));
    (void)column.accept(&visitor);
}

void fnv_hash_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection, uint16_t from,
                                    uint16_t to) {
    ColumnHashVisitor<FNVHash, SelectorSelection> visitor(hashes, SelectorSelection(selection, from, to));
    (void)column.accept(&visitor);
}
void fnv_hash_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size) {
    ColumnHashVisitor<FNVHash, SelectorSelective> visitor(hashes, SelectorSelective(sel, sel_size));
    (void)column.accept(&visitor);
}

// CRC32 Hash
void crc32_hash_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to) {
    ColumnHashVisitor<CRC32Hash, SelectorRange> visitor(hashes, SelectorRange(from, to));
    (void)column.accept(&visitor);
}
void crc32_hash_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection, uint16_t from,
                                      uint16_t to) {
    ColumnHashVisitor<CRC32Hash, SelectorSelection> visitor(hashes, SelectorSelection(selection, from, to));
    (void)column.accept(&visitor);
}
void crc32_hash_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size) {
    ColumnHashVisitor<CRC32Hash, SelectorSelective> visitor(hashes, SelectorSelective(sel, sel_size));
    (void)column.accept(&visitor);
}

// Murmur Hash
void murmur_hash3_x86_32_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to) {
    ColumnHashVisitor<MurmurHash3Hash, SelectorRange> visitor(hashes, SelectorRange(from, to));
    (void)column.accept(&visitor);
}
void murmur_hash3_x86_32_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection,
                                               uint16_t from, uint16_t to) {
    ColumnHashVisitor<MurmurHash3Hash, SelectorSelection> visitor(hashes, SelectorSelection(selection, from, to));
    (void)column.accept(&visitor);
}
void murmur_hash3_x86_32_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size) {
    ColumnHashVisitor<MurmurHash3Hash, SelectorSelective> visitor(hashes, SelectorSelective(sel, sel_size));
    (void)column.accept(&visitor);
}

// XXH3
void xxh3_64_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to) {
    ColumnHashVisitor<XXHash3, SelectorRange> visitor(hashes, SelectorRange(from, to));
    (void)column.accept(&visitor);
}
void xxh3_64_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection, uint16_t from,
                                   uint16_t to) {
    ColumnHashVisitor<XXHash3, SelectorSelection> visitor(hashes, SelectorSelection(selection, from, to));
    (void)column.accept(&visitor);
}
void xxh3_64_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size) {
    ColumnHashVisitor<XXHash3, SelectorSelective> visitor(hashes, SelectorSelective(sel, sel_size));
    (void)column.accept(&visitor);
}

} // namespace starrocks
