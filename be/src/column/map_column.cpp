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

#include "column/map_column.h"

#include <cstdint>
#include <set>

#include "column/column_helper.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/sorting.h"
#include "gutil/bits.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

void MapColumn::check_or_die() const {
    CHECK_EQ(_offsets->get_data().back(), _keys->size());
    CHECK_EQ(_offsets->get_data().back(), _values->size());
    DCHECK(_keys->is_nullable());
    DCHECK(_values->is_nullable());
    _offsets->check_or_die();
    _keys->check_or_die();
    _values->check_or_die();
}

MapColumn::MapColumn(ColumnPtr keys, ColumnPtr values, UInt32Column::Ptr offsets)
        : _keys(std::move(keys)), _values(std::move(values)), _offsets(std::move(offsets)) {
    DCHECK(_keys->is_nullable());
    DCHECK(_values->is_nullable());
    if (_offsets->empty()) {
        _offsets->append(0);
    }
}

size_t MapColumn::size() const {
    return _offsets->size() - 1;
}

size_t MapColumn::capacity() const {
    return _offsets->capacity() - 1;
}

const uint8_t* MapColumn::raw_data() const {
    DCHECK(false) << "Don't support map column raw_data";
    return nullptr;
}

uint8_t* MapColumn::mutable_raw_data() {
    DCHECK(false) << "Don't support map column mutable_raw_data";
    return nullptr;
}

size_t MapColumn::byte_size(size_t from, size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    return _keys->byte_size(_offsets->get_data()[from],
                            _offsets->get_data()[from + size] - _offsets->get_data()[from]) +
           _values->byte_size(_offsets->get_data()[from],
                              _offsets->get_data()[from + size] - _offsets->get_data()[from]) +
           _offsets->byte_size(from, size);
}

size_t MapColumn::byte_size(size_t idx) const {
    return _keys->byte_size(_offsets->get_data()[idx], _offsets->get_data()[idx + 1]) +
           _values->byte_size(_offsets->get_data()[idx], _offsets->get_data()[idx + 1]) +
           sizeof(_offsets->get_data()[idx]);
}

void MapColumn::reserve(size_t n) {
    _offsets->reserve(n + 1);
}

void MapColumn::resize(size_t n) {
    _offsets->get_data().resize(n + 1, _offsets->get_data().back());
    size_t array_size = _offsets->get_data().back();
    _keys->resize(array_size);
    _values->resize(array_size);
}

void MapColumn::assign(size_t n, size_t idx) {
    DCHECK_LE(idx, this->size()) << "Range error when assign MapColumn.";
    auto desc = this->clone_empty();
    auto datum = get(idx); // just reference
    desc->append_value_multiple_times(&datum, n);
    swap_column(*desc);
    desc->reset_column();
}

void MapColumn::append_datum(const Datum& datum) {
    const auto& map = datum.get<DatumMap>();
    size_t map_size = map.size();
    for (const auto& it : map) {
        _keys->append_datum(convert2Datum(it.first));
        _values->append_datum(it.second);
    }
    _offsets->append(_offsets->get_data().back() + static_cast<uint32_t>(map_size));
}

void MapColumn::append(const Column& src, size_t offset, size_t count) {
    const auto& map_column = down_cast<const MapColumn&>(src);

    const UInt32Column& src_offsets = map_column.offsets();
    size_t src_offset = src_offsets.get_data()[offset];
    size_t src_count = src_offsets.get_data()[offset + count] - src_offset;

    _keys->append(map_column.keys(), src_offset, src_count);
    _values->append(map_column.values(), src_offset, src_count);

    for (size_t i = offset; i < offset + count; i++) {
        uint32_t l = src_offsets.get_data()[i + 1] - src_offsets.get_data()[i];
        _offsets->append(_offsets->get_data().back() + l);
    }
}

void MapColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        append(src, indexes[from + i], 1);
    }
}

void MapColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        append(src, index, 1);
    }
}

void MapColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* datum = reinterpret_cast<const Datum*>(value);
    const auto& map = datum->get<DatumMap>();

    for (size_t c = 0; c < count; ++c) {
        append_datum(map);
    }
}

bool MapColumn::append_nulls(size_t count) {
    for (int i = 0; i < count; i++) {
        _offsets->append(_offsets->get_data().back());
    }
    return true;
}

void MapColumn::append_default() {
    _offsets->append(_offsets->get_data().back());
}

void MapColumn::append_default(size_t count) {
    size_t offset = _offsets->get_data().back();
    _offsets->append_value_multiple_times(&offset, count);
}

void MapColumn::fill_default(const Filter& filter) {
    std::vector<uint32_t> indexes;
    for (size_t i = 0; i < filter.size(); i++) {
        if (filter[i] == 1 && get_map_size(i) > 0) {
            indexes.push_back(static_cast<uint32_t>(i));
        }
    }
    auto default_column = clone_empty();
    default_column->append_default(indexes.size());
    update_rows(*default_column, indexes.data());
}

void MapColumn::update_rows(const Column& src, const uint32_t* indexes) {
    const auto& map_column = down_cast<const MapColumn&>(src);

    const UInt32Column& src_offsets = map_column.offsets();
    size_t replace_num = src.size();
    bool need_resize = false;
    for (size_t i = 0; i < replace_num; ++i) {
        if (_offsets->get_data()[indexes[i] + 1] - _offsets->get_data()[indexes[i]] !=
            src_offsets.get_data()[i + 1] - src_offsets.get_data()[i]) {
            need_resize = true;
            break;
        }
    }

    if (!need_resize) {
        Buffer<uint32_t> element_idxes;
        for (size_t i = 0; i < replace_num; ++i) {
            size_t element_count = src_offsets.get_data()[i + 1] - src_offsets.get_data()[i];
            size_t element_offset = _offsets->get_data()[indexes[i]];
            for (size_t j = 0; j < element_count; j++) {
                element_idxes.emplace_back(element_offset + j);
            }
        }
        _keys->update_rows(map_column.keys(), element_idxes.data());
        _values->update_rows(map_column.values(), element_idxes.data());
    } else {
        MutableColumnPtr new_map_column = clone_empty();
        size_t idx_begin = 0;
        for (size_t i = 0; i < replace_num; ++i) {
            size_t count = indexes[i] - idx_begin;
            new_map_column->append(*this, idx_begin, count);
            new_map_column->append(src, i, 1);
            idx_begin = indexes[i] + 1;
        }
        int64_t remain_count = _offsets->size() - idx_begin - 1;
        if (remain_count > 0) {
            new_map_column->append(*this, idx_begin, remain_count);
        }
        swap_column(*new_map_column.get());
    }
}

void MapColumn::remove_first_n_values(size_t count) {
    if (count >= _offsets->size()) {
        count = _offsets->size() - 1;
    }

    size_t offset = _offsets->get_data()[count];
    _keys->remove_first_n_values(offset);
    _values->remove_first_n_values(offset);
    _offsets->remove_first_n_values(count);

    for (size_t i = 0; i < _offsets->size(); i++) {
        _offsets->get_data()[i] -= offset;
    }
}

uint32_t MapColumn::serialize(size_t idx, uint8_t* pos) {
    DCHECK(!_keys->is_map());
    uint32_t offset = _offsets->get_data()[idx];
    uint32_t map_size = _offsets->get_data()[idx + 1] - offset;

    strings::memcpy_inlined(pos, &map_size, sizeof(map_size));
    size_t ser_size = sizeof(map_size);

    // unstable sort keys, map keys must be unique
    SmallPermutation perm(map_size);
    {
        for (uint32_t i = 0; i < map_size; i++) {
            perm[i].index_in_chunk = offset + i;
        }
        Tie tie(map_size, 1);
        std::pair<int, int> range{0, map_size};
        auto st = sort_and_tie_column(false, _keys, SortDesc(true, true), perm, tie, range, false);
        DCHECK(st.ok());
    }

    for (size_t i = 0; i < map_size; ++i) {
        uint32_t index = perm[i].index_in_chunk;
        ser_size += _keys->serialize(index, pos + ser_size);
        ser_size += _values->serialize(index, pos + ser_size);
    }
    return static_cast<uint32_t>(ser_size);
}

uint32_t MapColumn::serialize_default(uint8_t* pos) {
    uint32_t map_size = 0;
    strings::memcpy_inlined(pos, &map_size, sizeof(map_size));
    return sizeof(map_size);
}

const uint8_t* MapColumn::deserialize_and_append(const uint8_t* pos) {
    uint32_t map_size = 0;
    memcpy(&map_size, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    _offsets->append(_offsets->get_data().back() + map_size);
    for (size_t i = 0; i < map_size; ++i) {
        pos = _keys->deserialize_and_append(pos);
        pos = _values->deserialize_and_append(pos);
    }
    return pos;
}

uint32_t MapColumn::max_one_element_serialize_size() const {
    // TODO: performance optimization.
    size_t n = size();
    uint32_t max_size = 0;
    for (size_t i = 0; i < n; i++) {
        max_size = std::max(max_size, serialize_size(i));
    }
    return max_size;
}

uint32_t MapColumn::serialize_size(size_t idx) const {
    uint32_t offset = _offsets->get_data()[idx];
    uint32_t map_size = _offsets->get_data()[idx + 1] - offset;

    uint32_t ser_size = sizeof(map_size);
    for (size_t i = 0; i < map_size; ++i) {
        ser_size += _keys->serialize_size(offset + i);
        ser_size += _values->serialize_size(offset + i);
    }
    return ser_size;
}

void MapColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                uint32_t max_one_row_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

void MapColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    reserve(chunk_size);
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

MutableColumnPtr MapColumn::clone_empty() const {
    return create_mutable(_keys->clone_empty(), _values->clone_empty(), UInt32Column::create());
}

size_t MapColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    DCHECK_EQ(size(), to);
    auto* offsets = reinterpret_cast<uint32_t*>(_offsets->mutable_raw_data());
    uint32_t elements_start = offsets[from];
    uint32_t elements_end = offsets[to];
    Filter element_filter(elements_end, 0);

    auto check_offset = from;
    auto result_offset = from;

#if defined(__AVX2__) || defined(USE_AVX2KI)
    const uint8_t* f_data = filter.data();

    constexpr size_t kBatchSize = /*width of AVX registers*/ 256 / 8;
    const __m256i all0 = _mm256_setzero_si256();

    while (check_offset + kBatchSize < to) {
        __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + check_offset));
        uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));

        if (mask == 0) {
            // all no hit, pass
        } else if (mask == 0xffffffff) {
            // all hit, copy all
            auto element_size = offsets[check_offset + kBatchSize] - offsets[check_offset];
            memset(element_filter.data() + offsets[check_offset], 1, element_size);
            if (result_offset != check_offset) {
                DCHECK_LE(offsets[result_offset], offsets[check_offset]);
                // Equivalent to the following code:
                // ```
                //   uint32_t array_sizes[kBatchSize];
                //   for (int i = 0; i < kBatchSize; i++) {
                //     array_sizes[i] = offsets[check_offset + i + 1] - offsets[check_offset + i];
                //   }
                //   for (int i = 0; i < kBatchSize; i++) {
                //     offsets[result_offset + i + 1] = offsets[result_offset + i] + array_sizes[i];
                //   }
                // ```
                auto delta = offsets[check_offset] - offsets[result_offset];
                memmove(offsets + result_offset + 1, offsets + check_offset + 1, kBatchSize * sizeof(offsets[0]));
                for (int i = 0; i < kBatchSize; i++) {
                    offsets[result_offset + i + 1] -= delta;
                }
            }
            result_offset += kBatchSize;
        } else {
            // skip not hit row, it's will reduce compare when filter layout is sparse,
            // like "00010001...", but is ineffective when the filter layout is dense.

            auto zero_count = Bits::CountTrailingZerosNonZero32(mask);
            auto i = zero_count;
            while (i < kBatchSize) {
                mask = zero_count < 31 ? mask >> (zero_count + 1) : 0;

                auto array_size = offsets[check_offset + i + 1] - offsets[check_offset + i];
                memset(element_filter.data() + offsets[check_offset + i], 1, array_size);
                offsets[result_offset + 1] = offsets[result_offset] + array_size;
                zero_count = Bits::CountTrailingZeros32(mask);
                result_offset += 1;
                i += (zero_count + 1);
            }
        }
        check_offset += kBatchSize;
    }
#endif

    for (auto i = check_offset; i < to; ++i) {
        if (filter[i]) {
            DCHECK_GE(offsets[i + 1], offsets[i]);
            uint32_t array_size = offsets[i + 1] - offsets[i];
            memset(element_filter.data() + offsets[i], 1, array_size);
            offsets[result_offset + 1] = offsets[result_offset] + array_size;

            result_offset++;
        }
    }

    auto ret = _keys->filter_range(element_filter, elements_start, elements_end);
    DCHECK_EQ(offsets[result_offset], ret);
    ret = _values->filter_range(element_filter, elements_start, elements_end);
    DCHECK_EQ(offsets[result_offset], ret);
    resize(result_offset);
    return result_offset;
}

int MapColumn::compare_at(size_t left, size_t right, const Column& right_column, int nan_direction_hint) const {
    return -1;
}

int MapColumn::equals(size_t left, const Column& rhs, size_t right, bool safe_eq) const {
    const auto& rhs_map = down_cast<const MapColumn&>(rhs);

    size_t lhs_offset = _offsets->get_data()[left];
    size_t lhs_end = _offsets->get_data()[left + 1];
    size_t rhs_offset = rhs_map._offsets->get_data()[right];
    size_t rhs_end = rhs_map._offsets->get_data()[right + 1];
    // If size is not equal return false
    if (lhs_end - lhs_offset != rhs_end - rhs_offset) {
        return false;
    }

    // process the null key at last if exists, so non-nullable keys can exactly identify equal one or not.
    // if any non-nullable key does not match, return false;
    // else if all non-nullable key are matched (true or null), check the last nullable keys.
    // if the last nullable key is not matched, return false; else if there is null result from all keys matching,
    // return null, else return true.

    bool has_null_eq = false;
    uint32_t null_id = 0;
    std::vector<uint32_t> index;
    for (uint32_t i = lhs_offset; i < lhs_end; ++i) {
        if (_keys->is_null(i)) {
            null_id = i;
            continue;
        }
        index.push_back(i);
    }
    if (index.size() < (lhs_end - lhs_offset)) {
        index.push_back(null_id);
    }
    std::set<uint32_t> right_index;
    for (uint32_t j = rhs_offset; j < rhs_end; ++j) {
        right_index.insert(j);
    }

    for (auto i : index) {
        bool real_eq = false;
        bool null_eq = false;
        uint32_t eq_id = 0;
        for (unsigned int j : right_index) {
            int key_res = _keys->equals(i, *(rhs_map._keys.get()), j, safe_eq);
            if (key_res == EQUALS_FALSE) {
                continue;
            }
            // So two keys are the same or right key is null
            int val_res = _values->equals(i, *(rhs_map._values.get()), j, safe_eq);

            // case 1: key_res == EQUALS_TRUE
            if (key_res == EQUALS_TRUE) {
                if (val_res == EQUALS_FALSE) {
                    return EQUALS_FALSE;
                } else if (val_res == EQUALS_NULL) {
                    null_eq = true;
                } else if (val_res == EQUALS_TRUE) {
                    null_eq = false;
                    real_eq = true;
                }
                eq_id = j;
                break;
            }
            // case 2: key_res == EQUALS_NULL, continue
            if (val_res != EQUALS_FALSE) {
                eq_id = j;
                null_eq = true;
            }
        }
        if (null_eq || real_eq) {
            right_index.erase(eq_id);
            has_null_eq |= (!real_eq && null_eq);
        } else {
            return EQUALS_FALSE;
        }
    }

    DCHECK(right_index.empty()); // all matched return null or true

    // unsafe eq && has null eq, should return NULL
    // unsafe eq && none null eq, should return TRUE
    // safe eq, should return TRUE
    return !safe_eq && has_null_eq ? EQUALS_NULL : EQUALS_TRUE;
}

void MapColumn::fnv_hash_at(uint32_t* hash, uint32_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size()) << "idx + 1 should be less than offsets size";
    uint32_t offset = _offsets->get_data()[idx];
    // Should use size_t not uint32_t for compatible
    size_t map_size = _offsets->get_data()[idx + 1] - offset;

    *hash = HashUtil::fnv_hash(&map_size, static_cast<uint32_t>(sizeof(map_size)), *hash);
    uint32_t base_hash = *hash;
    for (size_t i = 0; i < map_size; ++i) {
        uint32_t pair_hash = base_hash;
        uint32_t ele_offset = offset + static_cast<uint32_t>(i);
        _keys->fnv_hash_at(&pair_hash, ele_offset);
        _values->fnv_hash_at(&pair_hash, ele_offset);

        // for get same hash on un-order map, we need to satisfies the commutative law
        *hash += pair_hash;
    }
}

void MapColumn::crc32_hash_at(uint32_t* hash, uint32_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size()) << "idx + 1 should be less than offsets size";
    uint32_t offset = _offsets->get_data()[idx];
    // Should use size_t not uint32_t for compatible
    size_t map_size = _offsets->get_data()[idx + 1] - offset;

    *hash = HashUtil::zlib_crc_hash(&map_size, static_cast<uint32_t>(sizeof(map_size)), *hash);
    uint32_t base_hash = *hash;
    for (size_t i = 0; i < map_size; ++i) {
        uint32_t pair_hash = base_hash;
        uint32_t ele_offset = offset + i;
        _keys->crc32_hash_at(&pair_hash, ele_offset);
        _values->crc32_hash_at(&pair_hash, ele_offset);

        // for get same hash on un-order map, we need to satisfies the commutative law
        *hash += pair_hash;
    }
}

// TODO: fnv_hash and crc32_hash in map column may has performance problem
// We need to make it possible in the future to provide vistor interface to iterator data
// as much as possible

void MapColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        fnv_hash_at(hash + i, i);
    }
}

void MapColumn::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        crc32_hash_at(hash + i, i);
    }
}

int64_t MapColumn::xor_checksum(uint32_t from, uint32_t to) const {
    // The XOR of MapColumn
    // XOR the offsets column and elements column
    int64_t xor_checksum = 0;
    for (size_t idx = from; idx < to; ++idx) {
        int64_t array_size = _offsets->get_data()[idx + 1] - _offsets->get_data()[idx];
        xor_checksum ^= array_size;
    }
    uint32_t element_from = _offsets->get_data()[from];
    uint32_t element_to = _offsets->get_data()[to];
    xor_checksum ^= _keys->xor_checksum(element_from, element_to);
    return (xor_checksum ^ _values->xor_checksum(element_from, element_to));
}

void MapColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    DCHECK_LT(idx, size());
    const size_t offset = _offsets->get_data()[idx];
    const size_t map_size = _offsets->get_data()[idx + 1] - offset;

    buf->begin_push_bracket();
    Column* keys = _keys.get();
    Column* values = _values.get();
    if (map_size > 0) {
        keys->put_mysql_row_buffer(buf, offset);
        buf->separator(':');
        values->put_mysql_row_buffer(buf, offset);
    }
    for (size_t i = 1; i < map_size; i++) {
        buf->separator(',');
        keys->put_mysql_row_buffer(buf, offset + i);
        buf->separator(':');
        values->put_mysql_row_buffer(buf, offset + i);
    }
    buf->finish_push_bracket();
}

Datum MapColumn::get(size_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size()) << "idx + 1 should be less than offsets size";
    size_t offset = _offsets->get_data()[idx];
    size_t map_size = _offsets->get_data()[idx + 1] - offset;

    DatumMap res;
    for (size_t i = 0; i < map_size; ++i) {
        res[_keys->get(offset + i).convert2DatumKey()] = _values->get(offset + i);
    }
    return {res};
}

size_t MapColumn::get_map_size(size_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size());
    return _offsets->get_data()[idx + 1] - _offsets->get_data()[idx];
}

std::pair<size_t, size_t> MapColumn::get_map_offset_size(size_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size());
    return {_offsets->get_data()[idx], _offsets->get_data()[idx + 1] - _offsets->get_data()[idx]};
}

bool MapColumn::set_null(size_t idx) {
    return false;
}

size_t MapColumn::reference_memory_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    size_t start_offset = _offsets->get_data()[from];
    size_t elements_num = _offsets->get_data()[from + size] - start_offset;
    return _keys->reference_memory_usage(start_offset, elements_num) +
           _values->reference_memory_usage(start_offset, elements_num) + _offsets->reference_memory_usage(from, size);
}

void MapColumn::swap_column(Column& rhs) {
    auto& map_column = down_cast<MapColumn&>(rhs);
    _offsets->swap_column(*map_column.offsets_column());
    _keys->swap_column(*map_column.keys_column());
    _values->swap_column(*map_column.values_column());
}

void MapColumn::reset_column() {
    Column::reset_column();
    _offsets->resize(1);
    _keys->reset_column();
    _values->reset_column();
}

std::string MapColumn::debug_item(size_t idx) const {
    DCHECK_LT(idx, size());
    uint32_t offset = _offsets->get_data()[idx];
    uint32_t map_size = _offsets->get_data()[idx + 1] - offset;

    std::stringstream ss;
    ss << "{";
    for (size_t i = 0; i < map_size; ++i) {
        if (i > 0) {
            ss << ",";
        }
        ss << _keys->debug_item(offset + i);
        ss << ":";
        ss << _values->debug_item(offset + i);
    }
    ss << "}";
    return ss.str();
}

std::string MapColumn::debug_string() const {
    std::stringstream ss;
    for (size_t i = 0; i < size(); ++i) {
        if (i > 0) {
            ss << ", ";
        }
        ss << debug_item(i);
    }
    return ss.str();
}

StatusOr<ColumnPtr> MapColumn::upgrade_if_overflow() {
    if (_offsets->size() > Column::MAX_CAPACITY_LIMIT) {
        return Status::InternalError("Size of MapColumn exceed the limit");
    }

    auto ret = upgrade_helper_func(&_keys);
    if (!ret.ok()) {
        return ret;
    }

    return upgrade_helper_func(&_values);
}

StatusOr<ColumnPtr> MapColumn::downgrade() {
    auto ret = downgrade_helper_func(&_keys);
    if (!ret.ok()) {
        return ret;
    }

    return downgrade_helper_func(&_values);
}

Status MapColumn::unfold_const_children(const starrocks::TypeDescriptor& type) {
    DCHECK(type.children.size() == 2) << "Map schema does not match data's";
    _keys = ColumnHelper::unfold_const_column(type.children[0], _keys->size(), _keys);
    _values = ColumnHelper::unfold_const_column(type.children[1], _values->size(), _values);
    return Status::OK();
}

// keep the last identical key
void MapColumn::remove_duplicated_keys(bool need_recursive) {
    // recursively distinct keys
    if (need_recursive && _values->is_map()) {
        down_cast<MapColumn*>(ColumnHelper::get_data_column(_values.get()))->remove_duplicated_keys(true);
    }
    Filter filter(_keys->size(), 1);
    // compute hash for all keys
    auto hash = std::make_unique<uint32_t[]>(_keys->size());
    memset(hash.get(), 0, _keys->size() * sizeof(uint32_t));
    _keys->fnv_hash(hash.get(), 0, _keys->size());

    bool has_duplicated_keys = false;
    size_t size = this->size();
    UInt32Column::Ptr new_offsets = UInt32Column::create();
    new_offsets->reserve(size + 1);
    auto& offsets_vec = new_offsets->get_data();
    offsets_vec.push_back(0);

    uint32_t new_offset = 0;
    for (auto i = 0; i < size; ++i) {
        std::unordered_multimap<uint32_t, uint32_t> key_hash_to_offsets;
        key_hash_to_offsets.reserve(_offsets->get_data()[i + 1] - _offsets->get_data()[i]);
        for (int32_t j = _offsets->get_data()[i + 1] - 1; j >= 0 && j >= _offsets->get_data()[i]; --j) {
            auto same_hash_offsets = key_hash_to_offsets.equal_range(hash[j]);
            for (auto it = same_hash_offsets.first; it != same_hash_offsets.second; ++it) {
                if (_keys->equals(j, *_keys, it->second)) {
                    filter[j] = 0;
                    has_duplicated_keys = true;
                    break;
                }
            }
            new_offset += filter[j];
            if (filter[j] != 0) {
                key_hash_to_offsets.emplace(hash[j], (uint32_t)j);
            }
        }
        offsets_vec.push_back(new_offset);
    }
    if (has_duplicated_keys) {
        auto new_keys_size = _keys->filter(filter);
        auto new_values_size = _values->filter(filter);
        DCHECK(new_keys_size == new_values_size);
        _offsets.swap(new_offsets);
    }
}

} // namespace starrocks
