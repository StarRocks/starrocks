// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/binary_column.h"

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include "column/bytes.h"
#include "common/logging.h"
#include "gutil/bits.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "util/coding.h"
#include "util/hash_util.hpp"
#include "util/mysql_row_buffer.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

void BinaryColumn::check_or_die() const {
    CHECK_EQ(_bytes.size(), _offsets.back());
    for (uint32_t i = 1; i < _offsets.size(); i++) {
        CHECK_GE(_offsets[i], _offsets[i - 1]);
    }
    if (_slices_cache) {
        for (int32_t i = 0; i < size(); i++) {
            CHECK_EQ(_slices[i].data, get_slice(i).data);
            CHECK_EQ(_slices[i].size, get_slice(i).size);
        }
    }
}

void BinaryColumn::append(const Column& src, size_t offset, size_t count) {
    const auto& b = down_cast<const BinaryColumn&>(src);
    const unsigned char* p = &b._bytes[b._offsets[offset]];
    const unsigned char* e = &b._bytes[b._offsets[offset + count]];

    _bytes.insert(_bytes.end(), p, e);

    for (size_t i = offset; i < offset + count; i++) {
        size_t l = b._offsets[i + 1] - b._offsets[i];
        _offsets.emplace_back(_offsets.back() + l);
    }
    _slices_cache = false;
}

void BinaryColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    const auto& src_column = down_cast<const BinaryColumn&>(src);
    const auto& src_offsets = src_column.get_offset();
    const auto& src_bytes = src_column.get_bytes();

    size_t cur_row_count = _offsets.size() - 1;
    size_t cur_byte_size = _bytes.size();

    _offsets.resize(cur_row_count + size + 1);
    for (size_t i = 0; i < size; i++) {
        uint32_t row_idx = indexes[from + i];
        uint32_t str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        _offsets[cur_row_count + i + 1] = _offsets[cur_row_count + i] + str_size;
        cur_byte_size += str_size;
    }
    _bytes.resize(cur_byte_size);

    auto* dest_bytes = _bytes.data();
    for (uint32_t i = 0; i < size; i++) {
        uint32_t row_idx = indexes[from + i];
        uint32_t str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        strings::memcpy_inlined(dest_bytes + _offsets[cur_row_count + i], src_bytes.data() + src_offsets[row_idx],
                                str_size);
    }

    _slices_cache = false;
}

void BinaryColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    auto& src_column = down_cast<const BinaryColumn&>(src);
    auto& src_offsets = src_column.get_offset();
    auto& src_bytes = src_column.get_bytes();

    size_t cur_row_count = _offsets.size() - 1;
    size_t cur_byte_size = _bytes.size();

    _offsets.resize(cur_row_count + size + 1);
    for (size_t i = 0; i < size; i++) {
        uint32_t row_idx = index;
        uint32_t str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        _offsets[cur_row_count + i + 1] = _offsets[cur_row_count + i] + str_size;
        cur_byte_size += str_size;
    }
    _bytes.resize(cur_byte_size);

    auto* dest_bytes = _bytes.data();
    for (uint32_t i = 0; i < size; i++) {
        uint32_t row_idx = index;
        uint32_t str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        strings::memcpy_inlined(dest_bytes + _offsets[cur_row_count + i], src_bytes.data() + src_offsets[row_idx],
                                str_size);
    }

    _slices_cache = false;
}

bool BinaryColumn::append_strings(const Buffer<Slice>& strs) {
    for (const auto& s : strs) {
        const uint8_t* const p = reinterpret_cast<const Bytes::value_type*>(s.data);
        _bytes.insert(_bytes.end(), p, p + s.size);
        _offsets.emplace_back(_bytes.size());
    }
    _slices_cache = false;
    return true;
}

// NOTE: this function should not be inlined. If this function is inlined,
// the append_strings_overflow will be slower by 30%
template <size_t copy_length>
void append_fixed_length(const Buffer<Slice>& strs, Bytes* bytes, BinaryColumn::Offsets* offsets)
        __attribute__((noinline));

template <size_t copy_length>
void append_fixed_length(const Buffer<Slice>& strs, Bytes* bytes, BinaryColumn::Offsets* offsets) {
    size_t size = bytes->size();
    for (const auto& s : strs) {
        size += s.size;
    }

    size_t offset = bytes->size();
    bytes->resize(size + copy_length);
    for (const auto& s : strs) {
        strings::memcpy_inlined(&(*bytes)[offset], s.data, copy_length);
        offset += s.size;
        offsets->emplace_back(offset);
    }
    bytes->resize(offset);
}

bool BinaryColumn::append_strings_overflow(const Buffer<Slice>& strs, size_t max_length) {
    if (max_length <= 16) {
        append_fixed_length<16>(strs, &_bytes, &_offsets);
    } else if (max_length <= 32) {
        append_fixed_length<32>(strs, &_bytes, &_offsets);
    } else if (max_length <= 64) {
        append_fixed_length<64>(strs, &_bytes, &_offsets);
    } else if (max_length <= 128) {
        append_fixed_length<128>(strs, &_bytes, &_offsets);
    } else {
        for (const auto& s : strs) {
            const uint8_t* const p = reinterpret_cast<const Bytes::value_type*>(s.data);
            _bytes.insert(_bytes.end(), p, p + s.size);
            _offsets.emplace_back(_bytes.size());
        }
    }
    _slices_cache = false;
    return true;
}

bool BinaryColumn::append_continuous_strings(const Buffer<Slice>& strs) {
    if (strs.empty()) {
        return true;
    }
    size_t new_size = _bytes.size();
    const uint8_t* p = reinterpret_cast<const uint8_t*>(strs.front().data);
    const uint8_t* q = reinterpret_cast<const uint8_t*>(strs.back().data + strs.back().size);
    _bytes.insert(_bytes.end(), p, q);
    for (const Slice& s : strs) {
        new_size += s.size;
        _offsets.emplace_back(new_size);
    }
    DCHECK_EQ(_bytes.size(), new_size);
    _slices_cache = false;
    return true;
}

void BinaryColumn::append_value_multiple_times(const void* value, size_t count) {
    const Slice* slice = reinterpret_cast<const Slice*>(value);
    size_t size = slice->size * count;
    _bytes.reserve(size);

    const uint8_t* const p = reinterpret_cast<const uint8_t*>(slice->data);
    const uint8_t* const pend = p + slice->size;
    for (size_t i = 0; i < count; ++i) {
        _bytes.insert(_bytes.end(), p, pend);
        _offsets.emplace_back(_bytes.size());
    }
    _slices_cache = false;
}

void BinaryColumn::_build_slices() const {
    DCHECK(_offsets.size() > 0);
    _slices_cache = false;
    _slices.clear();

    _slices.reserve(_offsets.size() - 1);

    for (int i = 0; i < _offsets.size() - 1; ++i) {
        _slices.emplace_back(_bytes.data() + _offsets[i], _offsets[i + 1] - _offsets[i]);
    }

    _slices_cache = true;
}

Status BinaryColumn::update_rows(const Column& src, const uint32_t* indexes) {
    const auto& src_column = down_cast<const BinaryColumn&>(src);
    size_t replace_num = src.size();
    bool need_resize = false;
    for (size_t i = 0; i < replace_num; ++i) {
        DCHECK_LT(indexes[i], _offsets.size());
        uint32_t cur_len = _offsets[indexes[i] + 1] - _offsets[indexes[i]];
        uint32_t new_len = src_column._offsets[i + 1] - src_column._offsets[i];
        if (cur_len != new_len) {
            need_resize = true;
            break;
        }
    }

    if (!need_resize) {
        auto* dest_bytes = _bytes.data();
        const auto& src_bytes = src_column.get_bytes();
        const auto& src_offsets = src_column.get_offset();
        for (size_t i = 0; i < replace_num; ++i) {
            uint32_t str_size = src_offsets[i + 1] - src_offsets[i];
            strings::memcpy_inlined(dest_bytes + _offsets[indexes[i]], src_bytes.data() + src_offsets[i], str_size);
        }
    } else {
        auto new_binary_column = BinaryColumn::create();
        size_t idx_begin = 0;
        for (size_t i = 0; i < replace_num; i++) {
            DCHECK_GE(_offsets.size() - 1, indexes[i]);
            size_t count = indexes[i] - idx_begin;
            new_binary_column->append(*this, idx_begin, count);
            new_binary_column->append(src, i, 1);
            idx_begin = indexes[i] + 1;
        }
        int32_t remain_count = _offsets.size() - idx_begin - 1;
        if (remain_count > 0) {
            new_binary_column->append(*this, idx_begin, remain_count);
        }
        swap_column(*new_binary_column.get());
    }

    return Status::OK();
}

void BinaryColumn::assign(size_t n, size_t idx) {
    std::string value = std::string((char*)_bytes.data() + _offsets[idx], _offsets[idx + 1] - _offsets[idx]);
    _bytes.clear();
    _offsets.clear();
    _offsets.emplace_back(0);
    const uint8_t* const start = reinterpret_cast<const Bytes::value_type*>(value.data());
    const uint8_t* const end = start + value.size();
    for (int i = 0; i < n; ++i) {
        _bytes.insert(_bytes.end(), start, end);
        _offsets.emplace_back(_bytes.size());
    }
    _slices_cache = false;
}

//TODO(kks): improve this
void BinaryColumn::remove_first_n_values(size_t count) {
    DCHECK_LE(count, _offsets.size() - 1);
    size_t remain_size = _offsets.size() - 1 - count;

    ColumnPtr column = cut(count, remain_size);
    auto* binary_column = down_cast<BinaryColumn*>(column.get());
    _offsets = std::move(binary_column->_offsets);
    _bytes = std::move(binary_column->_bytes);
    _slices_cache = false;
}

ColumnPtr BinaryColumn::cut(size_t start, size_t length) const {
    auto result = create();

    if (start >= size() || length == 0) {
        return result;
    }

    size_t upper = std::min(start + length, _offsets.size());
    size_t start_offset = _offsets[start];

    // offset re-compute
    result->get_offset().resize(upper - start + 1);
    // Always set offsets[0] to 0, in order to easily get element
    result->get_offset()[0] = 0;
    for (size_t i = start + 1, j = 1; i < upper + 1; ++i, ++j) {
        result->get_offset()[j] = _offsets[i] - start_offset;
    }

    // copy value
    result->_bytes.resize(_offsets[upper] - _offsets[start]);
    strings::memcpy_inlined(result->_bytes.data(), _bytes.data() + _offsets[start], _offsets[upper] - _offsets[start]);

    return result;
}

size_t BinaryColumn::filter_range(const Column::Filter& filter, size_t from, size_t to) {
    auto start_offset = from;
    auto result_offset = from;

    uint8_t* data = _bytes.data();

#ifdef __AVX2__
    const uint8_t* f_data = filter.data();

    int simd_bits = 256;
    int batch_nums = simd_bits / (8 * (int)sizeof(uint8_t));
    __m256i all0 = _mm256_setzero_si256();

    while (start_offset + batch_nums < to) {
        __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + start_offset));
        uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));

        if (mask == 0) {
            // all no hit, pass
        } else if (mask == 0xffffffff) {
            // all hit, copy all

            // copy data
            uint32_t size = _offsets[start_offset + batch_nums] - _offsets[start_offset];
            memmove(data + _offsets[result_offset], data + _offsets[start_offset], size);

            // set offsets, try vectorized
            uint32_t* offset_data = _offsets.data();
            for (int i = 0; i < batch_nums; ++i) {
                // TODO: performance, all sub one same offset ?
                offset_data[result_offset + i + 1] = offset_data[result_offset + i] +
                                                     offset_data[start_offset + i + 1] - offset_data[start_offset + i];
            }

            result_offset += batch_nums;
        } else {
            // skip not hit row, it's will reduce compare when filter layout is sparse,
            // like "00010001...", but is ineffective when the filter layout is dense.

            uint32_t zero_count = Bits::CountTrailingZerosNonZero32(mask);
            uint32_t i = zero_count;
            while (i < batch_nums) {
                mask = zero_count < 31 ? mask >> (zero_count + 1) : 0;

                uint32_t size = _offsets[start_offset + i + 1] - _offsets[start_offset + i];
                // copy date
                memmove(data + _offsets[result_offset], data + _offsets[start_offset + i], size);

                // set offsets
                _offsets[result_offset + 1] = _offsets[result_offset] + size;
                zero_count = Bits::CountTrailingZeros32(mask);
                result_offset += 1;
                i += (zero_count + 1);
            }
        }
        start_offset += batch_nums;
    }
#endif

    for (auto i = start_offset; i < to; ++i) {
        if (filter[i]) {
            DCHECK_GE(_offsets[i + 1], _offsets[i]);
            uint32_t size = _offsets[i + 1] - _offsets[i];
            // copy data
            memmove(data + _offsets[result_offset], data + _offsets[i], size);

            // set offsets
            _offsets[result_offset + 1] = _offsets[result_offset] + size;

            result_offset++;
        }
    }

    this->resize(result_offset);
    return result_offset;
}

int BinaryColumn::compare_row(std::vector<int8_t>& cmp_result, Datum rhs_value, int sort_order, int null_first) const {
    DCHECK(sort_order == 1 || sort_order == -1);

    const Container& lhs_datas = get_data();
    Slice rhs_data = rhs_value.get<Slice>();
    auto cmp = [&](int lhs_row) {
        if (sort_order == 1) {
            return SorterComparator<Slice>::compare(lhs_datas[lhs_row], rhs_data);
        } else {
            return -1 * SorterComparator<Slice>::compare(lhs_datas[lhs_row], rhs_data);
        }
    };

    return compare_row_helper(cmp_result, cmp);
}

void BinaryColumn::sort_and_tie(const bool& cancel, bool is_asc_order, bool is_null_first,
                                SmallPermutation& permutation, Tie& tie, std::pair<int, int> range, bool build_tie) {
    DCHECK_GE(size(), permutation.size());
    using ItemType = InlinePermuteItem<Slice>;
    auto cmp = [&](const ItemType& lhs, const ItemType& rhs) -> int {
        return lhs.inline_value.compare(rhs.inline_value);
    };

    auto inlined = create_inline_permutation<Slice>(permutation, get_data());
    sort_and_tie_helper(cancel, this, is_asc_order, inlined, tie, cmp, range, build_tie);
    restore_inline_permutation(inlined, permutation);
}

int BinaryColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    const BinaryColumn& right_column = down_cast<const BinaryColumn&>(rhs);
    return get_slice(left).compare(right_column.get_slice(right));
}

uint32_t BinaryColumn::max_one_element_serialize_size() const {
    uint32_t max_size = 0;
    auto prev_offset = _offsets[0];
    for (size_t i = 0; i < _offsets.size() - 1; ++i) {
        auto curr_offset = _offsets[i + 1];
        max_size = std::max(max_size, curr_offset - prev_offset);
        prev_offset = curr_offset;
    }
    return max_size + sizeof(uint32_t);
}

uint32_t BinaryColumn::serialize(size_t idx, uint8_t* pos) {
    uint32_t binary_size = _offsets[idx + 1] - _offsets[idx];
    uint32_t offset = _offsets[idx];

    strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
    strings::memcpy_inlined(pos + sizeof(uint32_t), &_bytes[offset], binary_size);

    return sizeof(uint32_t) + binary_size;
}

uint32_t BinaryColumn::serialize_default(uint8_t* pos) {
    uint32_t binary_size = 0;
    strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
    return sizeof(uint32_t);
}

void BinaryColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                   uint32_t max_one_row_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

const uint8_t* BinaryColumn::deserialize_and_append(const uint8_t* pos) {
    uint32_t string_size{};
    strings::memcpy_inlined(&string_size, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    size_t old_size = _bytes.size();
    _bytes.insert(_bytes.end(), pos, pos + string_size);

    _offsets.emplace_back(old_size + string_size);
    return pos + string_size;
}

void BinaryColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    uint32_t string_size = *((uint32_t*)srcs[0].data);
    _bytes.reserve(chunk_size * string_size * 2);
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

void BinaryColumn::fnv_hash(uint32_t* hashes, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        hashes[i] = HashUtil::fnv_hash(_bytes.data() + _offsets[i], _offsets[i + 1] - _offsets[i], hashes[i]);
    }
}

void BinaryColumn::crc32_hash(uint32_t* hashes, uint32_t from, uint32_t to) const {
    // keep hash if _bytes is empty
    for (uint32_t i = from; i < to && !_bytes.empty(); ++i) {
        hashes[i] = HashUtil::zlib_crc_hash(_bytes.data() + _offsets[i], _offsets[i + 1] - _offsets[i], hashes[i]);
    }
}

int64_t BinaryColumn::xor_checksum(uint32_t from, uint32_t to) const {
    // The XOR of BinaryColumn
    // For one string, treat it as a number of 64-bit integers and 8-bit integers.
    // XOR all of the integers to get a checksum for one string.
    // XOR all of the checksums to get xor_checksum.
    int64_t xor_checksum = 0;

    for (size_t i = from; i < to; ++i) {
        size_t num = _offsets[i + 1] - _offsets[i];
        const uint8_t* src = reinterpret_cast<const uint8_t*>(_bytes.data() + _offsets[i]);

#ifdef __AVX2__
        // AVX2 intructions can improve the speed of XOR procedure of one string.
        __m256i avx2_checksum = _mm256_setzero_si256();
        size_t step = sizeof(__m256i) / sizeof(uint8_t);

        while (num >= step) {
            const __m256i left = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
            avx2_checksum = _mm256_xor_si256(left, avx2_checksum);
            src += step;
            num -= step;
        }
        int64_t* checksum_vec = reinterpret_cast<int64_t*>(&avx2_checksum);
        size_t eight_byte_step = sizeof(__m256i) / sizeof(int64_t);
        for (size_t j = 0; j < eight_byte_step; ++j) {
            xor_checksum ^= checksum_vec[j];
        }
#endif

        while (num >= 8) {
            xor_checksum ^= *reinterpret_cast<const int64_t*>(src);
            src += 8;
            num -= 8;
        }
        for (size_t j = 0; j < num; ++j) {
            xor_checksum ^= src[j];
        }
    }

    return xor_checksum;
}

void BinaryColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    uint32_t start = _offsets[idx];
    uint32_t len = _offsets[idx + 1] - start;
    buf->push_string((const char*)_bytes.data() + start, len);
}

std::string BinaryColumn::debug_item(uint32_t idx) const {
    std::string s;
    auto slice = get_slice(idx);
    s.reserve(slice.size + 2);
    s.push_back('\'');
    s.append(slice.data, slice.size);
    s.push_back('\'');
    return s;
}

} // namespace starrocks::vectorized
