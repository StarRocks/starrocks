// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/binary_column.h"

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include "column/bytes.h"
#include "common/logging.h"
#include "gutil/bits.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "util/hash_util.hpp"
#include "util/mysql_row_buffer.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

template <typename T>
void BinaryColumnBase<T>::check_or_die() const {
    CHECK_EQ(_bytes.size(), _offsets.back());
    size_t size = this->size();
    for (size_t i = 0; i < size; i++) {
        CHECK_GE(_offsets[i + 1], _offsets[i]);
    }
    if (_slices_cache) {
        for (size_t i = 0; i < size; i++) {
            CHECK_EQ(_slices[i].data, get_slice(i).data);
            CHECK_EQ(_slices[i].size, get_slice(i).size);
        }
    }
}

template <typename T>
void BinaryColumnBase<T>::append(const Column& src, size_t offset, size_t count) {
    const auto& b = down_cast<const BinaryColumnBase<T>&>(src);
    const unsigned char* p = &b._bytes[b._offsets[offset]];
    const unsigned char* e = &b._bytes[b._offsets[offset + count]];

    _bytes.insert(_bytes.end(), p, e);

    for (size_t i = offset; i < offset + count; i++) {
        size_t l = b._offsets[i + 1] - b._offsets[i];
        _offsets.emplace_back(_offsets.back() + l);
    }
    _slices_cache = false;
}

template <typename T>
void BinaryColumnBase<T>::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    const auto& src_column = down_cast<const BinaryColumnBase<T>&>(src);
    const auto& src_offsets = src_column.get_offset();
    const auto& src_bytes = src_column.get_bytes();

    size_t cur_row_count = _offsets.size() - 1;
    size_t cur_byte_size = _bytes.size();

    _offsets.resize(cur_row_count + size + 1);
    for (size_t i = 0; i < size; i++) {
        uint32_t row_idx = indexes[from + i];
        T str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        _offsets[cur_row_count + i + 1] = _offsets[cur_row_count + i] + str_size;
        cur_byte_size += str_size;
    }
    _bytes.resize(cur_byte_size);

    auto* dest_bytes = _bytes.data();
    for (size_t i = 0; i < size; i++) {
        uint32_t row_idx = indexes[from + i];
        T str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        strings::memcpy_inlined(dest_bytes + _offsets[cur_row_count + i], src_bytes.data() + src_offsets[row_idx],
                                str_size);
    }

    _slices_cache = false;
}

template <typename T>
void BinaryColumnBase<T>::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size,
                                                      bool deep_copy) {
    auto& src_column = down_cast<const BinaryColumnBase<T>&>(src);
    auto& src_offsets = src_column.get_offset();
    auto& src_bytes = src_column.get_bytes();

    size_t cur_row_count = _offsets.size() - 1;
    size_t cur_byte_size = _bytes.size();

    _offsets.resize(cur_row_count + size + 1);
    for (size_t i = 0; i < size; i++) {
        uint32_t row_idx = index;
        T str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        _offsets[cur_row_count + i + 1] = _offsets[cur_row_count + i] + str_size;
        cur_byte_size += str_size;
    }
    _bytes.resize(cur_byte_size);

    auto* dest_bytes = _bytes.data();
    for (size_t i = 0; i < size; i++) {
        uint32_t row_idx = index;
        T str_size = src_offsets[row_idx + 1] - src_offsets[row_idx];
        strings::memcpy_inlined(dest_bytes + _offsets[cur_row_count + i], src_bytes.data() + src_offsets[row_idx],
                                str_size);
    }

    _slices_cache = false;
}

//TODO(fzh): optimize copy using SIMD
template <typename T>
ColumnPtr BinaryColumnBase<T>::replicate(const std::vector<uint32_t>& offsets) {
    auto dest = std::dynamic_pointer_cast<BinaryColumnBase<T>>(BinaryColumnBase<T>::create());
    auto& dest_offsets = dest->get_offset();
    auto& dest_bytes = dest->get_bytes();
    auto src_size = offsets.size() - 1; // this->size() may be large than offsets->size() -1
    size_t total_size = 0;              // total size to copy
    for (auto i = 0; i < src_size; ++i) {
        auto bytes_size = _offsets[i + 1] - _offsets[i];
        total_size += bytes_size * (offsets[i + 1] - offsets[i]);
    }
    dest_bytes.resize(total_size);
    dest_offsets.resize(dest_offsets.size() + offsets.back());

    auto pos = 0;
    for (auto i = 0; i < src_size; ++i) {
        auto bytes_size = _offsets[i + 1] - _offsets[i];
        for (auto j = offsets[i]; j < offsets[i + 1]; ++j) {
            strings::memcpy_inlined(dest_bytes.data() + pos, _bytes.data() + _offsets[i], bytes_size);
            pos += bytes_size;
            dest_offsets[j + 1] = pos;
        }
    }
    return dest;
}

template <typename T>
bool BinaryColumnBase<T>::append_strings(const Buffer<Slice>& strs) {
    for (const auto& s : strs) {
        const auto* const p = reinterpret_cast<const Bytes::value_type*>(s.data);
        _bytes.insert(_bytes.end(), p, p + s.size);
        _offsets.emplace_back(_bytes.size());
    }
    _slices_cache = false;
    return true;
}

// NOTE: this function should not be inlined. If this function is inlined,
// the append_strings_overflow will be slower by 30%
template <typename T, size_t copy_length>
void append_fixed_length(const Buffer<Slice>& strs, Bytes* bytes, typename BinaryColumnBase<T>::Offsets* offsets)
        __attribute__((noinline));

template <typename T, size_t copy_length>
void append_fixed_length(const Buffer<Slice>& strs, Bytes* bytes, typename BinaryColumnBase<T>::Offsets* offsets) {
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

template <typename T>
bool BinaryColumnBase<T>::append_strings_overflow(const Buffer<Slice>& strs, size_t max_length) {
    if (max_length <= 8) {
        append_fixed_length<T, 8>(strs, &_bytes, &_offsets);
    } else if (max_length <= 16) {
        append_fixed_length<T, 16>(strs, &_bytes, &_offsets);
    } else if (max_length <= 32) {
        append_fixed_length<T, 32>(strs, &_bytes, &_offsets);
    } else if (max_length <= 64) {
        append_fixed_length<T, 64>(strs, &_bytes, &_offsets);
    } else if (max_length <= 128) {
        append_fixed_length<T, 128>(strs, &_bytes, &_offsets);
    } else {
        for (const auto& s : strs) {
            const auto* const p = reinterpret_cast<const Bytes::value_type*>(s.data);
            _bytes.insert(_bytes.end(), p, p + s.size);
            _offsets.emplace_back(_bytes.size());
        }
    }
    _slices_cache = false;
    return true;
}

template <typename T>
bool BinaryColumnBase<T>::append_continuous_strings(const Buffer<Slice>& strs) {
    if (strs.empty()) {
        return true;
    }
    size_t new_size = _bytes.size();
    const auto* p = reinterpret_cast<const uint8_t*>(strs.front().data);
    const auto* q = reinterpret_cast<const uint8_t*>(strs.back().data + strs.back().size);
    _bytes.insert(_bytes.end(), p, q);

    _offsets.reserve(_offsets.size() + strs.size());
    for (const Slice& s : strs) {
        new_size += s.size;
        _offsets.emplace_back(new_size);
    }
    DCHECK_EQ(_bytes.size(), new_size);
    _slices_cache = false;
    return true;
}

template <typename T>
bool BinaryColumnBase<T>::append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) {
    if (size == 0) return true;
    size_t bytes_size = _bytes.size();

    // copy blob
    size_t data_size = size * fixed_length;
    const auto* p = reinterpret_cast<const uint8_t*>(data);
    const auto* q = reinterpret_cast<const uint8_t*>(data + data_size);
    _bytes.insert(_bytes.end(), p, q);

    // copy offsets
    starrocks::raw::stl_vector_resize_uninitialized(&_offsets, _offsets.size() + size);
    // _offsets.resize(_offsets.size() + size);
    T* off_data = _offsets.data() + _offsets.size() - size;

    int i = 0;

#ifdef __AVX2__
    if constexpr (std::is_same_v<T, uint32_t>) {
        if ((bytes_size + fixed_length * size) < std::numeric_limits<uint32_t>::max()) {
            const int times = size / 8;

#define FX(m) (m * fixed_length)
#define BFX(m) (bytes_size + m * fixed_length)
            __m256i base = _mm256_set_epi32(BFX(8), BFX(7), BFX(6), BFX(5), BFX(4), BFX(3), BFX(2), BFX(1));
            __m256i delta = _mm256_set_epi32(FX(8), FX(8), FX(8), FX(8), FX(8), FX(8), FX(8), FX(8));
            for (int t = 0; t < times; t++) {
                _mm256_storeu_si256((__m256i*)off_data, base);
                base = _mm256_add_epi32(base, delta);
                off_data += 8;
            }

            i = times * 8;
            bytes_size += fixed_length * i;
        }
    }
#endif
    for (; i < size; i++) {
        bytes_size += fixed_length;
        *(off_data++) = bytes_size;
    }
    return true;
}

template <typename T>
void BinaryColumnBase<T>::append_value_multiple_times(const void* value, size_t count) {
    const auto* slice = reinterpret_cast<const Slice*>(value);
    size_t size = slice->size * count;
    _bytes.reserve(size);

    const auto* const p = reinterpret_cast<const uint8_t*>(slice->data);
    const uint8_t* const pend = p + slice->size;
    for (size_t i = 0; i < count; ++i) {
        _bytes.insert(_bytes.end(), p, pend);
        _offsets.emplace_back(_bytes.size());
    }
    _slices_cache = false;
}

template <typename T>
void BinaryColumnBase<T>::_build_slices() const {
    if constexpr (std::is_same_v<T, uint32_t>) {
        CHECK_LT(_bytes.size(), (size_t)UINT32_MAX) << "BinaryColumn size overflow";
    }

    DCHECK(_offsets.size() > 0);
    _slices_cache = false;
    _slices.clear();

    _slices.reserve(_offsets.size() - 1);

    for (size_t i = 0; i < _offsets.size() - 1; ++i) {
        _slices.emplace_back(_bytes.data() + _offsets[i], _offsets[i + 1] - _offsets[i]);
    }

    _slices_cache = true;
}

template <typename T>
void BinaryColumnBase<T>::fill_default(const Filter& filter) {
    std::vector<uint32_t> indexes;
    for (size_t i = 0; i < filter.size(); i++) {
        size_t len = _offsets[i + 1] - _offsets[i];
        if (filter[i] == 1 && len > 0) {
            indexes.push_back(i);
        }
    }
    if (indexes.empty()) {
        return;
    }
    auto default_column = clone_empty();
    default_column->append_default(indexes.size());
    update_rows(*default_column, indexes.data());
}

template <typename T>
Status BinaryColumnBase<T>::update_rows(const Column& src, const uint32_t* indexes) {
    const auto& src_column = down_cast<const BinaryColumnBase<T>&>(src);
    size_t replace_num = src.size();
    bool need_resize = false;
    for (size_t i = 0; i < replace_num; ++i) {
        DCHECK_LT(indexes[i], _offsets.size());
        T cur_len = _offsets[indexes[i] + 1] - _offsets[indexes[i]];
        T new_len = src_column._offsets[i + 1] - src_column._offsets[i];
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
            T str_size = src_offsets[i + 1] - src_offsets[i];
            strings::memcpy_inlined(dest_bytes + _offsets[indexes[i]], src_bytes.data() + src_offsets[i], str_size);
        }
    } else {
        auto new_binary_column = BinaryColumnBase<T>::create();
        size_t idx_begin = 0;
        for (size_t i = 0; i < replace_num; i++) {
            DCHECK_GE(_offsets.size() - 1, indexes[i]);
            size_t count = indexes[i] - idx_begin;
            new_binary_column->append(*this, idx_begin, count);
            new_binary_column->append(src, i, 1);
            idx_begin = indexes[i] + 1;
        }
        if (size() > indexes[replace_num - 1] + 1) {
            size_t remain_count = size() - indexes[replace_num - 1] - 1;
            new_binary_column->append(*this, indexes[replace_num - 1] + 1, remain_count);
        }
        swap_column(*new_binary_column);
    }

    return Status::OK();
}

template <typename T>
void BinaryColumnBase<T>::assign(size_t n, size_t idx) {
    std::string value = std::string((char*)_bytes.data() + _offsets[idx], _offsets[idx + 1] - _offsets[idx]);
    _bytes.clear();
    _offsets.clear();
    _offsets.emplace_back(0);
    const auto* const start = reinterpret_cast<const Bytes::value_type*>(value.data());
    const uint8_t* const end = start + value.size();
    for (int i = 0; i < n; ++i) {
        _bytes.insert(_bytes.end(), start, end);
        _offsets.emplace_back(_bytes.size());
    }
    _slices_cache = false;
}

//TODO(kks): improve this
template <typename T>
void BinaryColumnBase<T>::remove_first_n_values(size_t count) {
    DCHECK_LE(count, _offsets.size() - 1);
    size_t remain_size = _offsets.size() - 1 - count;

    ColumnPtr column = cut(count, remain_size);
    auto* binary_column = down_cast<BinaryColumnBase<T>*>(column.get());
    _offsets = std::move(binary_column->_offsets);
    _bytes = std::move(binary_column->_bytes);
    _slices_cache = false;
}

template <typename T>
ColumnPtr BinaryColumnBase<T>::cut(size_t start, size_t length) const {
    auto result = this->create();

    if (start >= size() || length == 0) {
        return result;
    }

    size_t upper = std::min(start + length, _offsets.size());
    T start_offset = _offsets[start];

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

template <typename T>
size_t BinaryColumnBase<T>::filter_range(const Column::Filter& filter, size_t from, size_t to) {
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
            T size = _offsets[start_offset + batch_nums] - _offsets[start_offset];
            memmove(data + _offsets[result_offset], data + _offsets[start_offset], size);

            // set offsets, try vectorized
            T* offset_data = _offsets.data();
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

                T size = _offsets[start_offset + i + 1] - _offsets[start_offset + i];
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
            T size = _offsets[i + 1] - _offsets[i];
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

template <typename T>
int BinaryColumnBase<T>::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    const auto& right_column = down_cast<const BinaryColumnBase<T>&>(rhs);
    return get_slice(left).compare(right_column.get_slice(right));
}

template <typename T>
uint32_t BinaryColumnBase<T>::max_one_element_serialize_size() const {
    uint32_t max_size = 0;
    T prev_offset = _offsets[0];
    for (size_t i = 0; i < _offsets.size() - 1; ++i) {
        T curr_offset = _offsets[i + 1];
        // it's safe to cast, because max size of one string is 2^32
        max_size = std::max(max_size, static_cast<uint32_t>(curr_offset - prev_offset));
        prev_offset = curr_offset;
    }
    // TODO: may be overflow here, i will solve it later
    return max_size + sizeof(uint32_t);
}

template <typename T>
uint32_t BinaryColumnBase<T>::serialize(size_t idx, uint8_t* pos) {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t binary_size = _offsets[idx + 1] - _offsets[idx];
    T offset = _offsets[idx];

    strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
    strings::memcpy_inlined(pos + sizeof(uint32_t), &_bytes[offset], binary_size);

    return sizeof(uint32_t) + binary_size;
}

template <typename T>
uint32_t BinaryColumnBase<T>::serialize_default(uint8_t* pos) {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t binary_size = 0;
    strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
    return sizeof(uint32_t);
}

template <typename T>
void BinaryColumnBase<T>::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                          uint32_t max_one_row_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

template <typename T>
const uint8_t* BinaryColumnBase<T>::deserialize_and_append(const uint8_t* pos) {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t string_size{};
    strings::memcpy_inlined(&string_size, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    size_t old_size = _bytes.size();
    _bytes.insert(_bytes.end(), pos, pos + string_size);

    _offsets.emplace_back(old_size + string_size);
    return pos + string_size;
}

template <typename T>
void BinaryColumnBase<T>::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t string_size = *((uint32_t*)srcs[0].data);
    _bytes.reserve(chunk_size * string_size * 2);
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

template <typename T>
void BinaryColumnBase<T>::fnv_hash(uint32_t* hashes, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; ++i) {
        hashes[i] = HashUtil::fnv_hash(_bytes.data() + _offsets[i], _offsets[i + 1] - _offsets[i], hashes[i]);
    }
}

template <typename T>
void BinaryColumnBase<T>::crc32_hash(uint32_t* hashes, uint32_t from, uint32_t to) const {
    // keep hash if _bytes is empty
    for (uint32_t i = from; i < to && !_bytes.empty(); ++i) {
        hashes[i] = HashUtil::zlib_crc_hash(_bytes.data() + _offsets[i], _offsets[i + 1] - _offsets[i], hashes[i]);
    }
}

template <typename T>
int64_t BinaryColumnBase<T>::xor_checksum(uint32_t from, uint32_t to) const {
    // The XOR of BinaryColumn
    // For one string, treat it as a number of 64-bit integers and 8-bit integers.
    // XOR all of the integers to get a checksum for one string.
    // XOR all of the checksums to get xor_checksum.
    int64_t xor_checksum = 0;

    for (size_t i = from; i < to; ++i) {
        size_t num = _offsets[i + 1] - _offsets[i];
        const auto* src = reinterpret_cast<const uint8_t*>(_bytes.data() + _offsets[i]);

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
        auto* checksum_vec = reinterpret_cast<int64_t*>(&avx2_checksum);
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

template <typename T>
void BinaryColumnBase<T>::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    T start = _offsets[idx];
    T len = _offsets[idx + 1] - start;
    buf->push_string((const char*)_bytes.data() + start, len);
}

template <typename T>
std::string BinaryColumnBase<T>::debug_item(uint32_t idx) const {
    std::string s;
    auto slice = get_slice(idx);
    s.reserve(slice.size + 2);
    s.push_back('\'');
    s.append(slice.data, slice.size);
    s.push_back('\'');
    return s;
}

size_t find_first_overflow_point(const BinaryColumnBase<uint32_t>::Offsets& offsets, size_t start, size_t end) {
    for (size_t i = start; i < end; i++) {
        if (offsets[i] > offsets[i + 1]) {
            return i + 1;
        }
    }
    return end;
}

template <typename T>
StatusOr<ColumnPtr> BinaryColumnBase<T>::upgrade_if_overflow() {
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);

    if constexpr (std::is_same_v<T, uint32_t>) {
        if (_offsets.size() > Column::MAX_CAPACITY_LIMIT) {
            return Status::InternalError("column size exceed the limit");
        } else if (_bytes.size() >= Column::MAX_CAPACITY_LIMIT) {
            auto new_column = BinaryColumnBase<uint64_t>::create();
            new_column->get_offset().resize(_offsets.size());
            new_column->get_bytes().swap(_bytes);

            size_t base = 0;
            size_t start = 0;
            size_t end = _offsets.size();

            // TODO: There may be better implementations which improve performance
            while (start < end) {
                size_t mid = find_first_overflow_point(_offsets, start, end);
                for (size_t i = start; i < mid; i++) {
                    new_column->get_offset()[i] = static_cast<uint64_t>(_offsets[i]) + base;
                }
                base += Column::MAX_CAPACITY_LIMIT;
                start = mid;
            }

            // NOTE(yanz): in BinaryColumnBase, we have an invariant that `_offsets.back == _bytes.size()`;  
            // and since _bytes has been moved to new_column, we have to clear _offset to keep the invariant.
            _offsets.clear();
            return new_column;
        } else {
            return nullptr;
        }
    } else {
        return nullptr;
    }
}

template <typename T>
StatusOr<ColumnPtr> BinaryColumnBase<T>::downgrade() {
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);

    if constexpr (std::is_same_v<T, uint32_t>) {
        return nullptr;
    } else {
        if (_bytes.size() >= Column::MAX_CAPACITY_LIMIT) {
            return Status::InternalError("column size exceed the limit, can't downgrade");
        } else {
            auto new_column = BinaryColumn::create();
            new_column->get_offset().resize(_offsets.size());
            new_column->get_bytes().swap(_bytes);

            for (size_t i = 0; i < _offsets.size(); i++) {
                new_column->get_offset()[i] = _offsets[i];
            }
            _offsets.resize(0);
            return new_column;
        }
    }
}

template <typename T>
bool BinaryColumnBase<T>::has_large_column() const {
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);

    if constexpr (std::is_same_v<T, uint64_t>) {
        return true;
    } else {
        return false;
    }
}

template <typename T>
bool BinaryColumnBase<T>::capacity_limit_reached(std::string* msg) const {
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);
    if constexpr (std::is_same_v<T, uint32_t>) {
        // The size limit of a single element is 2^32 - 1.
        // The size limit of all elements is 2^32 - 1.
        // The number limit of elements is 2^32 - 1.
        if (_bytes.size() >= Column::MAX_CAPACITY_LIMIT) {
            if (msg != nullptr) {
                msg->append("Total byte size of binary column exceed the limit: " +
                            std::to_string(Column::MAX_CAPACITY_LIMIT));
            }
            return true;
        } else if (_offsets.size() >= Column::MAX_CAPACITY_LIMIT) {
            if (msg != nullptr) {
                msg->append("Total row count of binary column exceed the limit: " +
                            std::to_string(Column::MAX_CAPACITY_LIMIT));
            }
            return true;
        } else {
            return false;
        }
    } else {
        // The size limit of a single element is 2^32 - 1.
        // The size limit of all elements is 2^64 - 1.
        // The number limit of elements is 2^32 - 1.
        if (_bytes.size() >= Column::MAX_LARGE_CAPACITY_LIMIT) {
            if (msg != nullptr) {
                msg->append("Total byte size of large binary column exceed the limit: " +
                            std::to_string(Column::MAX_LARGE_CAPACITY_LIMIT));
            }
            return true;
        } else if (_offsets.size() >= Column::MAX_CAPACITY_LIMIT) {
            if (msg != nullptr) {
                msg->append("Total row count of large binary column exceed the limit: " +
                            std::to_string(Column::MAX_CAPACITY_LIMIT));
            }
            return true;
        } else {
            return false;
        }
    }
}

template class BinaryColumnBase<uint32_t>;
template class BinaryColumnBase<uint64_t>;

} // namespace starrocks::vectorized
