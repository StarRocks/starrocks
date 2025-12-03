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
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "common/status.h"
#include "runtime/time_types.h"
#include "util/hash_util.hpp"

namespace starrocks {

// Hash function type tags
struct FNVHash {
    static constexpr const char* name() { return "FNV"; }
    [[always_inline]] static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::fnv_hash(data, bytes, seed);
    }
};

struct CRC32Hash {
    static constexpr const char* name() { return "CRC32"; }
    [[always_inline]] static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::zlib_crc_hash(data, bytes, seed);
    }
};

struct MurmurHash3Hash {
    static constexpr const char* name() { return "MurmurHash3"; }
    [[always_inline]] static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::murmur_hash3_32(data, bytes, seed);
    }
};

struct XXHash3 {
    static constexpr const char* name() { return "XXHash"; }
    [[always_inline]] static uint32_t hash(const void* data, int32_t bytes, uint32_t seed) {
        return HashUtil::xx_hash3_64(data, bytes, seed);
    }
};

template <typename HashFunction>
class ColumnHashVisitor final : public ColumnVisitorAdapter<ColumnHashVisitor<HashFunction>> {
public:
    // range
    ColumnHashVisitor(uint32_t* hashes, uint32_t from, uint32_t to)
            : ColumnVisitorAdapter<ColumnHashVisitor<HashFunction>>(this), _hashes(hashes), _from(from), _to(to) {}

    // range with selection
    ColumnHashVisitor(uint32_t* hashes, uint8_t* selection, uint16_t from, uint16_t to)
            : ColumnVisitorAdapter<ColumnHashVisitor<HashFunction>>(this),
              _hashes(hashes),
              _from(from),
              _to(to),
              _selection(selection) {}

    // range with selective
    ColumnHashVisitor(uint32_t* hashes, uint16_t* sel, uint16_t sel_size)
            : ColumnVisitorAdapter<ColumnHashVisitor<HashFunction>>(this),
              _hashes(hashes),
              _sel(sel),
              _sel_size(sel_size) {}

    // single value
    ColumnHashVisitor(uint32_t* hashes, uint32_t idx)
            : ColumnVisitorAdapter<ColumnHashVisitor<HashFunction>>(this), _hashes(hashes), _from(idx), _to(idx + 1) {}

    ~ColumnHashVisitor() override = default;

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        auto hash_value = [](const T& value, uint32_t* hash) {
            if constexpr (std::is_same_v<HashFunction, CRC32Hash> && (IsDate<T> || IsTimestamp<T>)) {
                std::string str = value.to_string();
                *hash = HashFunction::hash(str.data(), static_cast<int32_t>(str.size()), *hash);
            } else if constexpr (IsDecimal<T>) {
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

        const auto datas = column.immutable_data();
        for_each_index([&](uint32_t idx) {
            uint32_t* slot_ptr = slot(idx);
            hash_value(datas[idx], slot_ptr);
        });
        return Status::OK();
    }

    // Overloads for specific column types - DecimalV3Column inherits from FixedLengthColumnBase
    // so we can directly call the template version
    template <typename T>
    Status do_visit(const DecimalV3Column<T>& column) {
        return do_visit(static_cast<const FixedLengthColumnBase<T>&>(static_cast<const Column&>(column)));
    }

    template <typename SizeT>
    Status do_visit(const BinaryColumnBase<SizeT>& column) {
        const auto& offsets = column.get_offset();
        const auto& bytes = column.get_bytes();
        for_each_index([&](uint32_t idx) {
            uint32_t* slot_ptr = slot(idx);
            auto len = offsets[idx + 1] - offsets[idx];
            *slot_ptr = HashFunction::hash(bytes.data() + offsets[idx], static_cast<int32_t>(len), *slot_ptr);
        });
        return Status::OK();
    }

    Status do_visit(const NullableColumn& column) {
        if (!column.has_null()) {
            auto visitor = clone_visitor();
            return column.data_column()->accept(&visitor);
        }

        const auto null_data = column.null_column()->immutable_data();
        constexpr uint32_t value = 0x9e3779b9;
        auto mix_null = [&](uint32_t idx) {
            uint32_t* slot_ptr = slot(idx);
            *slot_ptr = *slot_ptr ^ (value + (*slot_ptr << 6) + (*slot_ptr >> 2));
        };

        // Fast path: no selection arrays involved, so we can work on continuous ranges.
        if (_selection == nullptr && _sel == nullptr) {
            uint32_t cursor = _from;
            while (cursor < _to) {
                uint32_t next = cursor + 1;
                while (next < _to && null_data[next] == null_data[cursor]) {
                    ++next;
                }
                if (null_data[cursor]) {
                    for (uint32_t idx = cursor; idx < next; ++idx) {
                        mix_null(idx);
                    }
                } else {
                    ColumnHashVisitor<HashFunction> visitor(_hashes, cursor, next);
                    (void)column.data_column()->accept(&visitor);
                }
                cursor = next;
            }
            return Status::OK();
        }

        // Fallback: respect selection/sel arrays by handling row by row.
        for_each_index([&](uint32_t idx) {
            if (null_data[idx]) {
                mix_null(idx);
            } else {
                ColumnHashVisitor<HashFunction> visitor(slot(idx), idx);
                (void)column.data_column()->accept(&visitor);
            }
        });
        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) {
        DCHECK(column.size() > 0);
        const ColumnPtr& data = column.data_column();
        for_each_index([&](uint32_t idx) {
            ColumnHashVisitor<HashFunction> visitor(slot(idx), idx);
            (void)data->accept(&visitor);
        });
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        const auto& offsets = column.offsets_column()->immutable_data();
        const ColumnPtr& elements = column.elements_column();
        for_each_index([&](uint32_t idx) {
            uint32_t array_size = offsets[idx + 1] - offsets[idx];
            uint32_t* slot_ptr = slot(idx);
            *slot_ptr = HashFunction::hash(&array_size, sizeof(array_size), *slot_ptr);
            if (array_size > 0) {
                ColumnHashVisitor<HashFunction> visitor(slot_ptr, offsets[idx], offsets[idx + 1]);
                (void)elements->accept(&visitor);
            }
        });
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        const auto& offsets = column.offsets_column()->immutable_data();
        const ColumnPtr& keys = column.keys_column();
        const ColumnPtr& values = column.values_column();
        for_each_index([&](uint32_t idx) {
            uint32_t map_size = offsets[idx + 1] - offsets[idx];
            uint32_t* slot_ptr = slot(idx);
            *slot_ptr = HashFunction::hash(&map_size, sizeof(map_size), *slot_ptr);
            if (map_size > 0) {
                ColumnHashVisitor<HashFunction> visitor(slot_ptr, offsets[idx], offsets[idx + 1]);
                (void)keys->accept(&visitor);
                (void)values->accept(&visitor);
            }
        });
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        for (const ColumnPtr& field : column.fields()) {
            auto visitor = clone_visitor();
            (void)field->accept(&visitor);
        }
        return Status::OK();
    }

    Status do_visit(const JsonColumn& column) { return Status::NotSupported("JsonColumn is not supported"); }

    Status do_visit(const VariantColumn& column) { return Status::NotSupported("VariantColumn is not supported"); }

    template <typename ObjectType>
    Status do_visit(const ObjectColumn<ObjectType>& column) {
        std::string buffer;
        const auto& pool = column.get_pool();
        for_each_index([&](uint32_t idx) {
            buffer.resize(pool[idx].serialize_size());
            size_t size = pool[idx].serialize(reinterpret_cast<uint8_t*>(buffer.data()));
            uint32_t* slot_ptr = slot(idx);
            *slot_ptr = HashFunction::hash(buffer.data(), static_cast<int32_t>(size), *slot_ptr);
        });
        return Status::OK();
    }

private:
    template <typename Fn>
    void for_each_index(Fn&& fn) const {
        if (_sel != nullptr && _sel_size > 0) {
            for (uint16_t i = 0; i < _sel_size; ++i) {
                fn(_sel[i]);
            }
        } else if (_selection != nullptr) {
            for (uint32_t i = _from; i < _to; ++i) {
                if (_selection[i]) {
                    fn(i);
                }
            }
        } else {
            for (uint32_t i = _from; i < _to; ++i) {
                fn(i);
            }
        }
    }

    ColumnHashVisitor<HashFunction> clone_visitor() const {
        if (_sel != nullptr && _sel_size > 0) {
            return ColumnHashVisitor<HashFunction>(_hashes, _sel, _sel_size);
        } else if (_selection != nullptr) {
            return ColumnHashVisitor<HashFunction>(_hashes, _selection, static_cast<uint16_t>(_from),
                                                   static_cast<uint16_t>(_to));
        } else {
            return ColumnHashVisitor<HashFunction>(_hashes, _from, _to);
        }
    }

    [[always_inline]] uint32_t* slot(uint32_t idx) const { return _hashes + idx; }

private:
    // outoput hash values
    uint32_t* _hashes;
    // range
    uint32_t _from;
    uint32_t _to;
    // selection
    uint8_t* _selection;
    // selective
    uint16_t* _sel;
    uint16_t _sel_size;
};

// Explicit template instantiations
template class ColumnHashVisitor<FNVHash>;
template class ColumnHashVisitor<CRC32Hash>;
template class ColumnHashVisitor<MurmurHash3Hash>;
template class ColumnHashVisitor<XXHash3>;

// FNV Hash
void fnv_hash_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to) {
    ColumnHashVisitor<FNVHash> visitor(hashes, from, to);
    (void)column.accept(&visitor);
}

void fnv_hash_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection, uint16_t from,
                                    uint16_t to) {
    ColumnHashVisitor<FNVHash> visitor(hashes, selection, from, to);
    (void)column.accept(&visitor);
}
void fnv_hash_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size) {
    ColumnHashVisitor<FNVHash> visitor(hashes, sel, sel_size);
    (void)column.accept(&visitor);
}

// CRC32 Hash
void crc32_hash_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to) {
    ColumnHashVisitor<CRC32Hash> visitor(hashes, from, to);
    (void)column.accept(&visitor);
}
void crc32_hash_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection, uint16_t from,
                                      uint16_t to) {
    ColumnHashVisitor<CRC32Hash> visitor(hashes, selection, from, to);
    (void)column.accept(&visitor);
}
void crc32_hash_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size) {
    ColumnHashVisitor<CRC32Hash> visitor(hashes, sel, sel_size);
    (void)column.accept(&visitor);
}

// Murmur Hash
void murmur_hash3_x86_32_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to) {
    ColumnHashVisitor<MurmurHash3Hash> visitor(hashes, from, to);
    (void)column.accept(&visitor);
}
void murmur_hash3_x86_32_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection,
                                               uint16_t from, uint16_t to) {
    ColumnHashVisitor<MurmurHash3Hash> visitor(hashes, selection, from, to);
    (void)column.accept(&visitor);
}
void murmur_hash3_x86_32_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size) {
    ColumnHashVisitor<MurmurHash3Hash> visitor(hashes, sel, sel_size);
    (void)column.accept(&visitor);
}

} // namespace starrocks
