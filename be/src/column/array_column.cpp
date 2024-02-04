// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/array_column.h"

#include <cstdint>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "gutil/bits.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "util/mysql_row_buffer.h"

namespace starrocks::vectorized {

void ArrayColumn::check_or_die() const {
    CHECK_EQ(_offsets->get_data().back(), _elements->size());
    DCHECK(_elements->is_nullable());
    _offsets->check_or_die();
    _elements->check_or_die();
}

ArrayColumn::ArrayColumn(ColumnPtr elements, UInt32Column::Ptr offsets)
        : _elements(std::move(elements)), _offsets(std::move(offsets)) {
    if (_offsets->empty()) {
        _offsets->append(0);
    }
}

size_t ArrayColumn::size() const {
    return _offsets->size() - 1;
}

size_t ArrayColumn::capacity() const {
    return _offsets->capacity() - 1;
}

const uint8_t* ArrayColumn::raw_data() const {
    return _elements->raw_data();
}

uint8_t* ArrayColumn::mutable_raw_data() {
    return _elements->mutable_raw_data();
}

size_t ArrayColumn::byte_size(size_t from, size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    return _elements->byte_size(_offsets->get_data()[from],
                                _offsets->get_data()[from + size] - _offsets->get_data()[from]) +
           _offsets->Column::byte_size(from, size);
}

size_t ArrayColumn::byte_size(size_t idx) const {
    return _elements->byte_size(_offsets->get_data()[idx], _offsets->get_data()[idx + 1]) +
           sizeof(_offsets->get_data()[idx]);
}

void ArrayColumn::reserve(size_t n) {
    _offsets->reserve(n + 1);
}

void ArrayColumn::resize(size_t n) {
    _offsets->get_data().resize(n + 1, _offsets->get_data().back());
    size_t array_size = _offsets->get_data().back();
    _elements->resize(array_size);
}

void ArrayColumn::assign(size_t n, size_t idx) {
    DCHECK_LE(idx, this->size()) << "Range error when assign arrayColumn.";
    auto desc = this->clone_empty();
    auto datum = get(idx); // just reference
    desc->append_value_multiple_times(&datum, n);
    swap_column(*desc);
    desc->reset_column();
}

void ArrayColumn::append_datum(const Datum& datum) {
    const auto& array = datum.get<DatumArray>();
    size_t array_size = array.size();
    for (size_t i = 0; i < array_size; ++i) {
        _elements->append_datum(array[i]);
    }
    _offsets->append(_offsets->get_data().back() + array_size);
}

void ArrayColumn::append_array_element(const Column& elem, size_t null_elem) {
    _elements->append(elem);
    _elements->append_nulls(null_elem);
    _offsets->append(_offsets->get_data().back() + elem.size() + null_elem);
}

void ArrayColumn::append(const Column& src, size_t offset, size_t count) {
    const auto& array_column = down_cast<const ArrayColumn&>(src);

    const UInt32Column& src_offsets = array_column.offsets();
    size_t src_offset = src_offsets.get_data()[offset];
    size_t src_count = src_offsets.get_data()[offset + count] - src_offset;

    _elements->append(array_column.elements(), src_offset, src_count);

    for (size_t i = offset; i < offset + count; i++) {
        size_t l = src_offsets.get_data()[i + 1] - src_offsets.get_data()[i];
        _offsets->append(_offsets->get_data().back() + l);
    }
}

void ArrayColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        append(src, indexes[from + i], 1);
    }
}

// TODO(fzh): directly copy elements for un-nested arrays
void ArrayColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        append(src, index, 1);
    }
}

void ArrayColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* datum = reinterpret_cast<const Datum*>(value);
    const auto& array = datum->get<DatumArray>();
    size_t array_size = array.size();

    for (size_t c = 0; c < count; ++c) {
        for (size_t i = 0; i < array_size; ++i) {
            _elements->append_datum(array[i]);
        }
        _offsets->append(_offsets->get_data().back() + array_size);
    }
}

bool ArrayColumn::append_nulls(size_t count) {
    return false;
}

void ArrayColumn::append_default() {
    _offsets->append(_offsets->get_data().back());
}

void ArrayColumn::append_default(size_t count) {
    size_t offset = _offsets->get_data().back();
    _offsets->append_value_multiple_times(&offset, count);
}

void ArrayColumn::fill_default(const Filter& filter) {
    std::vector<uint32_t> indexes;
    for (size_t i = 0; i < filter.size(); i++) {
        if (filter[i] == 1 && get_element_size(i) > 0) {
            indexes.push_back(i);
        }
    }
    auto default_column = clone_empty();
    default_column->append_default(indexes.size());
    update_rows(*default_column, indexes.data());
}

Status ArrayColumn::update_rows(const Column& src, const uint32_t* indexes) {
    const auto& array_column = down_cast<const ArrayColumn&>(src);

    const UInt32Column& src_offsets = array_column.offsets();
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
        RETURN_IF_ERROR(_elements->update_rows(array_column.elements(), element_idxes.data()));
    } else {
        MutableColumnPtr new_array_column = clone_empty();
        size_t idx_begin = 0;
        for (size_t i = 0; i < replace_num; ++i) {
            size_t count = indexes[i] - idx_begin;
            new_array_column->append(*this, idx_begin, count);
            new_array_column->append(src, i, 1);
            idx_begin = indexes[i] + 1;
        }
        int32_t remain_count = _offsets->size() - idx_begin - 1;
        if (remain_count > 0) {
            new_array_column->append(*this, idx_begin, remain_count);
        }
        swap_column(*new_array_column.get());
    }

    return Status::OK();
}

uint32_t ArrayColumn::serialize(size_t idx, uint8_t* pos) {
    uint32_t offset = _offsets->get_data()[idx];
    uint32_t array_size = _offsets->get_data()[idx + 1] - offset;

    strings::memcpy_inlined(pos, &array_size, sizeof(array_size));
    size_t ser_size = sizeof(array_size);
    for (size_t i = 0; i < array_size; ++i) {
        ser_size += _elements->serialize(offset + i, pos + ser_size);
    }
    return ser_size;
}

uint32_t ArrayColumn::serialize_default(uint8_t* pos) {
    uint32_t array_size = 0;
    strings::memcpy_inlined(pos, &array_size, sizeof(array_size));
    return sizeof(array_size);
}

const uint8_t* ArrayColumn::deserialize_and_append(const uint8_t* pos) {
    uint32_t array_size = 0;
    memcpy(&array_size, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    _offsets->append(_offsets->get_data().back() + array_size);
    for (size_t i = 0; i < array_size; ++i) {
        pos = _elements->deserialize_and_append(pos);
    }
    return pos;
}

uint32_t ArrayColumn::max_one_element_serialize_size() const {
    // TODO: performance optimization.
    size_t n = size();
    uint32_t max_size = 0;
    for (size_t i = 0; i < n; i++) {
        max_size = std::max(max_size, serialize_size(i));
    }
    return max_size;
}

uint32_t ArrayColumn::serialize_size(size_t idx) const {
    uint32_t offset = _offsets->get_data()[idx];
    uint32_t array_size = _offsets->get_data()[idx + 1] - offset;

    uint32_t ser_size = sizeof(array_size);
    for (size_t i = 0; i < array_size; ++i) {
        ser_size += _elements->serialize_size(offset + i);
    }
    return ser_size;
}

void ArrayColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                  uint32_t max_one_row_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

void ArrayColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    reserve(chunk_size);
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

MutableColumnPtr ArrayColumn::clone_empty() const {
    return create_mutable(_elements->clone_empty(), UInt32Column::create());
}

size_t ArrayColumn::filter_range(const Column::Filter& filter, size_t from, size_t to) {
    DCHECK_EQ(size(), to);
    auto* offsets = reinterpret_cast<uint32_t*>(_offsets->mutable_raw_data());
    uint32_t elements_start = offsets[from];
    uint32_t elements_end = offsets[to];
    Filter element_filter(elements_end, 0);

    auto check_offset = from;
    auto result_offset = from;

#ifdef __AVX2__
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

    auto ret = _elements->filter_range(element_filter, elements_start, elements_end);
    DCHECK_EQ(offsets[result_offset], ret);
    resize(result_offset);
    return result_offset;
}

int ArrayColumn::compare_at(size_t left, size_t right, const Column& right_column, int nan_direction_hint) const {
    const auto& rhs = down_cast<const ArrayColumn&>(right_column);

    size_t lhs_offset = _offsets->get_data()[left];
    size_t lhs_size = _offsets->get_data()[left + 1] - lhs_offset;

    const UInt32Column& rhs_offsets = rhs.offsets();
    size_t rhs_offset = rhs_offsets.get_data()[right];
    size_t rhs_size = rhs_offsets.get_data()[right + 1] - rhs_offset;
    size_t min_size = std::min(lhs_size, rhs_size);
    for (size_t i = 0; i < min_size; ++i) {
        int res = _elements->compare_at(lhs_offset + i, rhs_offset + i, rhs.elements(), nan_direction_hint);
        if (res != 0) {
            return res;
        }
    }

    return lhs_size < rhs_size ? -1 : (lhs_size == rhs_size ? 0 : 1);
}

void ArrayColumn::fnv_hash_at(uint32_t* hash, int32_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size()) << "idx + 1 should be less than offsets size";
    size_t offset = _offsets->get_data()[idx];
    size_t array_size = _offsets->get_data()[idx + 1] - offset;

    *hash = HashUtil::fnv_hash(&array_size, sizeof(array_size), *hash);
    for (size_t i = 0; i < array_size; ++i) {
        uint32_t ele_offset = offset + i;
        _elements->fnv_hash_at(hash, ele_offset);
    }
}

void ArrayColumn::crc32_hash_at(uint32_t* hash, int32_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size()) << "idx + 1 should be less than offsets size";
    size_t offset = _offsets->get_data()[idx];
    size_t array_size = _offsets->get_data()[idx + 1] - offset;

    *hash = HashUtil::zlib_crc_hash(&array_size, sizeof(array_size), *hash);
    for (size_t i = 0; i < array_size; ++i) {
        uint32_t ele_offset = offset + i;
        _elements->crc32_hash_at(hash, ele_offset);
    }
}

// TODO: fnv_hash and crc32_hash in array column may has performance problem
// We need to make it possible in the future to provide vistor interface to iterator data
// as much as possible

void ArrayColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        fnv_hash_at(hash + i, i);
    }
}

void ArrayColumn::crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        crc32_hash_at(hash + i, i);
    }
}

int64_t ArrayColumn::xor_checksum(uint32_t from, uint32_t to) const {
    // The XOR of ArrayColumn
    // XOR the offsets column and elements column
    int64_t xor_checksum = 0;
    for (size_t idx = from; idx < to; ++idx) {
        int64_t array_size = _offsets->get_data()[idx + 1] - _offsets->get_data()[idx];
        xor_checksum ^= array_size;
    }
    uint32_t element_from = _offsets->get_data()[from];
    uint32_t element_to = _offsets->get_data()[to];
    return (xor_checksum ^ _elements->xor_checksum(element_from, element_to));
}

void ArrayColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    DCHECK_LT(idx, size());
    const size_t offset = _offsets->get_data()[idx];
    const size_t array_size = _offsets->get_data()[idx + 1] - offset;

    buf->begin_push_array();
    Column* elements = _elements.get();
    if (array_size > 0) {
        elements->put_mysql_row_buffer(buf, offset);
    }
    for (size_t i = 1; i < array_size; i++) {
        buf->separator(',');
        elements->put_mysql_row_buffer(buf, offset + i);
    }
    buf->finish_push_array();
}

Datum ArrayColumn::get(size_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size()) << "idx + 1 should be less than offsets size";
    size_t offset = _offsets->get_data()[idx];
    size_t array_size = _offsets->get_data()[idx + 1] - offset;

    DatumArray res(array_size);
    for (size_t i = 0; i < array_size; ++i) {
        res[i] = _elements->get(offset + i);
    }
    return {res};
}

std::pair<size_t, size_t> ArrayColumn::get_element_offset_size(size_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size());
    size_t offset = _offsets->get_data()[idx];
    size_t size = _offsets->get_data()[idx + 1] - _offsets->get_data()[idx];
    return {offset, size};
}

size_t ArrayColumn::get_element_null_count(size_t idx) const {
    auto offset_size = get_element_offset_size(idx);
    auto nullable = down_cast<NullableColumn*>(_elements.get());
    return nullable->null_count(offset_size.first, offset_size.second);
}

size_t ArrayColumn::get_element_size(size_t idx) const {
    DCHECK_LT(idx + 1, _offsets->size());
    return _offsets->get_data()[idx + 1] - _offsets->get_data()[idx];
}

bool ArrayColumn::set_null(size_t idx) {
    return false;
}

size_t ArrayColumn::reference_memory_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    size_t start_offset = _offsets->get_data()[from];
    size_t elements_num = _offsets->get_data()[from + size] - start_offset;
    return _elements->reference_memory_usage(start_offset, elements_num) + _offsets->reference_memory_usage(from, size);
}

void ArrayColumn::swap_column(Column& rhs) {
    auto& array_column = down_cast<ArrayColumn&>(rhs);
    _offsets->swap_column(*array_column.offsets_column());
    _elements->swap_column(*array_column.elements_column());
}

void ArrayColumn::reset_column() {
    Column::reset_column();
    _offsets->resize(1);
    _elements->reset_column();
}

std::string ArrayColumn::debug_item(uint32_t idx) const {
    DCHECK_LT(idx, size());
    size_t offset = _offsets->get_data()[idx];
    size_t array_size = _offsets->get_data()[idx + 1] - offset;

    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < array_size; ++i) {
        if (i > 0) {
            ss << ",";
        }
        ss << _elements->debug_item(offset + i);
    }
    ss << "]";
    return ss.str();
}

std::string ArrayColumn::debug_string() const {
    std::stringstream ss;
    for (size_t i = 0; i < size(); ++i) {
        if (i > 0) {
            ss << ", ";
        }
        ss << debug_item(i);
    }
    return ss.str();
}

StatusOr<ColumnPtr> ArrayColumn::upgrade_if_overflow() {
    if (_offsets->size() > Column::MAX_CAPACITY_LIMIT) {
        return Status::InternalError("Size of ArrayColumn exceed the limit");
    }

    return upgrade_helper_func(&_elements);
}

StatusOr<ColumnPtr> ArrayColumn::downgrade() {
    return downgrade_helper_func(&_elements);
}

bool ArrayColumn::empty_null_array(const NullColumnPtr& null_map) {
    DCHECK(null_map->size() == this->size());
    bool need_empty = false;
    auto size = this->size();
    // TODO: optimize it using SIMD
    for (auto i = 0; i < size && !need_empty; ++i) {
        if (null_map->get_data()[i] && _offsets->get_data()[i + 1] != _offsets->get_data()[i]) {
            need_empty = true;
        }
    }
    // TODO: copy too much may result in worse performance.
    if (need_empty) {
        auto new_array_column = clone_empty();
        int count = 0;
        int null_count = 0;
        for (size_t i = 0; i < size; ++i) {
            if (null_map->get_data()[i]) {
                ++null_count;
                if (count > 0) {
                    new_array_column->append(*this, i - count, count);
                    count = 0;
                }
            } else {
                ++count;
                if (null_count > 0) {
                    new_array_column->append_default(null_count);
                    null_count = 0;
                }
            }
        }
        if (count > 0) {
            new_array_column->append(*this, size - count, count);
            count = 0;
        }
        if (null_count > 0) {
            new_array_column->append_default(null_count);
            null_count = 0;
        }
        swap_column(*new_array_column.get());
    }
    return need_empty;
}

} // namespace starrocks::vectorized
