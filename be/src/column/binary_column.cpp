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

#include "column/binary_column.h"

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include "base/container/raw_container.h"
#include "base/hash/hash_util.hpp"
#include "column/bytes.h"
#include "column/mysql_row_buffer.h"
#include "column/vectorized_fwd.h"
#include "common/config_local_io_fwd.h"
#include "gutil/bits.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"

namespace starrocks {
template <typename T>
BinaryColumnBase<T>::BinaryColumnBase(ContainerResource resource, Offsets offsets)
        : _bytes(), _offsets(std::move(offsets)), _resource(std::move(resource)) {
    if (_offsets.empty()) {
        _offsets.emplace_back(0);
    }
    if (!config::enable_zero_copy_from_page_cache) {
        _ensure_materialized();
    }
}

template <typename T>
void BinaryColumnBase<T>::check_or_die() const {
    CHECK_EQ(get_immutable_bytes().size(), _offsets.back());
    size_t size = this->size();
    for (size_t i = 0; i < size; i++) {
        DCHECK_GE(_offsets[i + 1], _offsets[i]);
    }
}

template <typename T>
void BinaryColumnBase<T>::append(const Slice& str) {
    auto& bytes = get_bytes();
    bytes.insert(bytes.end(), str.data, str.data + str.size);
    _offsets.emplace_back(bytes.size());
    invalidate_slice_cache();
}

template <typename T>
void BinaryColumnBase<T>::_append_binary_impl(const Offsets& src_offsets, const uint8_t* src_base, size_t offset,
                                              size_t count) {
    if (UNLIKELY(count == 0)) {
        return;
    }

    const uint64_t src_begin = src_offsets[offset];
    const uint64_t src_end = src_offsets[offset + count];
    const size_t copy_bytes = src_end - src_begin;

    auto& dst_bytes = get_bytes();
    const uint64_t dst_begin = _offsets.back();
    const uint64_t dst_end = dst_begin + copy_bytes;

    const size_t old_bytes_size = dst_bytes.size();
    dst_bytes.resize(old_bytes_size + copy_bytes);
    if (copy_bytes != 0) {
        strings::memcpy_inlined(dst_bytes.data() + old_bytes_size, src_base + src_begin, copy_bytes);
    }

    const size_t old_offsets_size = _offsets.size();
    _offsets.resize_uninitialized(old_offsets_size + count, dst_end);

    Offsets::visit_storage_pair(_offsets, src_offsets, [&](auto& dst_buf, const auto& src_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        using SrcValue = typename std::decay_t<decltype(src_buf)>::value_type;
        DstValue* __restrict dst_ptr = dst_buf.data() + old_offsets_size;
        const SrcValue* __restrict src_ptr = src_buf.data() + offset + 1;

        if constexpr (std::is_same_v<SrcValue, DstValue>) {
            if (LIKELY(src_begin == dst_begin)) {
                strings::memcpy_inlined(dst_ptr, src_ptr, count * sizeof(DstValue));
            } else {
                const DstValue delta = static_cast<DstValue>(dst_begin - src_begin);
                for (size_t i = 0; i < count; ++i) {
                    dst_ptr[i] = src_ptr[i] + delta;
                }
            }
        } else {
            for (size_t i = 0; i < count; ++i) {
                dst_ptr[i] = static_cast<DstValue>(static_cast<uint64_t>(src_ptr[i]) - src_begin + dst_begin);
            }
        }
    });

    invalidate_slice_cache();
}

template <typename T>
void BinaryColumnBase<T>::append(const Column& src, size_t offset, size_t count) {
    DCHECK(offset + count <= src.size());

    if (UNLIKELY(count == 0)) {
        return;
    }

    if (src.is_binary()) {
        const auto& binary = down_cast<const BinaryColumn&>(src);
        _append_binary_impl(binary.get_offset(), binary.raw_bytes(), offset, count);
        return;
    }
    if (src.is_large_binary()) {
        const auto& binary = down_cast<const LargeBinaryColumn&>(src);
        _append_binary_impl(binary.get_offset(), binary.raw_bytes(), offset, count);
        return;
    }

    CHECK(false) << "BinaryColumnBase::append: incompatible column type"
                 << " src.is_binary=" << src.is_binary() << " src.is_large_binary=" << src.is_large_binary();
}

template <typename T>
void BinaryColumnBase<T>::append_selective(const Column& src, const uint32_t* indexes, uint32_t from,
                                           const uint32_t size) {
    if (UNLIKELY(size == 0)) {
        return;
    }

    if (src.is_binary_view()) {
        src.append_selective_to(*this, indexes, from, size);
        return;
    }

    indexes += from;

    const auto& src_column = down_cast<const BinaryColumnBase<T>&>(src);
    auto& bytes = get_bytes();
    const auto& src_offsets = src_column.get_offset();

    const size_t prev_num_offsets = _offsets.size();
    const uint64_t dst_begin = _offsets.back();
    const uint64_t src_bytes_size = src_column._total_bytes();

    auto copy_ranges_and_write_offsets = [&](auto* dst_offsets, uint8_t* __restrict dest_bytes,
                                             const uint8_t* __restrict src_bytes, const auto* range_data) {
        using DstValue = std::remove_pointer_t<decltype(dst_offsets)>;
        uint64_t cur_offset = dst_begin;

        if (src_bytes_size > 32 * 1024 * 1024ull) {
            for (size_t i = 0; i < size; ++i) {
                if (i + 16 < size) {
                    __builtin_prefetch(src_bytes + range_data[i * 2 + 32]);
                }
                const size_t str_size = range_data[i * 2 + 1] - range_data[i * 2];
                strings::memcpy_inlined(dest_bytes + cur_offset, src_bytes + range_data[i * 2], str_size);
                cur_offset += str_size;
                dst_offsets[i] = static_cast<DstValue>(cur_offset);
            }
        } else {
            for (size_t i = 0; i < size; ++i) {
                const size_t str_size = range_data[i * 2 + 1] - range_data[i * 2];
                // Only copy 16 bytes extra when src is small enough; on large sources the extra reads hurt cache.
                strings::memcpy_inlined_overflow16(dest_bytes + cur_offset, src_bytes + range_data[i * 2],
                                                   static_cast<ssize_t>(str_size));
                cur_offset += str_size;
                dst_offsets[i] = static_cast<DstValue>(cur_offset);
            }
        }
    };

    bool used_dst_offsets_as_ranges = false;
    src_offsets.visit_storage([&](const auto& src_buf) {
        using SrcValue = typename std::decay_t<decltype(src_buf)>::value_type;
        if constexpr (std::is_same_v<SrcValue, uint32_t>) {
            used_dst_offsets_as_ranges = true;
        }
    });

    // Reuse destination offsets as the temporary [src_begin, src_end] range buffer only when source offsets
    // are physically uint32_t. Large source offsets may exceed the destination's final offset range; storing
    // them in _offsets as scratch data could force an unnecessary promotion to uint64_t.
    if (used_dst_offsets_as_ranges) {
        // resize for scratch data
        _offsets.resize_uninitialized(prev_num_offsets + static_cast<size_t>(size) * 2);

        uint64_t small_append_bytes = 0;
        Offsets::visit_storage_pair(_offsets, src_offsets, [&](auto& dst_buf, const auto& src_buf) {
            using SrcValue = typename std::decay_t<decltype(src_buf)>::value_type;
            if constexpr (std::is_same_v<SrcValue, uint32_t>) {
                auto* range_data = dst_buf.data() + prev_num_offsets;
                const auto* src_offset_data = src_buf.data();

                for (size_t i = 0; i < size; ++i) {
                    const uint32_t src_idx = indexes[i];
                    // assgin for scratch data
                    range_data[i * 2] = src_offset_data[src_idx];
                    range_data[i * 2 + 1] = src_offset_data[src_idx + 1];
                    small_append_bytes += range_data[i * 2 + 1] - range_data[i * 2];
                }
            }
        });

        const uint64_t dst_end = dst_begin + small_append_bytes;
        _offsets.ensure_width_for_value(dst_end);

        bytes.resize(dst_end);
        const auto* __restrict src_bytes = src_column._data_base();
        auto* __restrict dest_bytes = bytes.data();

        _offsets.visit_storage([&](auto& dst_buf) {
            auto* range_data = dst_buf.data() + prev_num_offsets;
            copy_ranges_and_write_offsets(range_data, dest_bytes, src_bytes, range_data);
        });

        _offsets.resize(prev_num_offsets + size);
    } else {
        // use a separate temporary buffer for source ranges when source offsets are large.
        Buffer<uint64_t> ranges;
        raw::stl_vector_resize_uninitialized(&ranges, static_cast<size_t>(size) * 2);
        uint64_t append_bytes = 0;
        src_offsets.visit_storage([&](const auto& src_buf) {
            const auto* __restrict src_offset_data = src_buf.data();
            auto* __restrict range_data = ranges.data();

            for (size_t i = 0; i < size; ++i) {
                const uint32_t src_idx = indexes[i];
                range_data[i * 2] = src_offset_data[src_idx];
                range_data[i * 2 + 1] = src_offset_data[src_idx + 1];
                append_bytes += range_data[i * 2 + 1] - range_data[i * 2];
            }
        });

        const uint64_t dst_end = dst_begin + append_bytes;
        _offsets.resize_uninitialized(prev_num_offsets + size, dst_end);

        bytes.resize(dst_end);
        const auto* __restrict src_bytes = src_column._data_base();
        auto* __restrict dest_bytes = bytes.data();
        const auto* __restrict range_data = ranges.data();

        _offsets.visit_storage([&](auto& dst_buf) {
            auto* dst_offsets = dst_buf.data() + prev_num_offsets;
            copy_ranges_and_write_offsets(dst_offsets, dest_bytes, src_bytes, range_data);
        });
    }

    invalidate_slice_cache();
}

template <typename T>
void BinaryColumnBase<T>::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    if (UNLIKELY(size == 0)) {
        return;
    }

    const auto& src_column = down_cast<const BinaryColumnBase<T>&>(src);
    const auto& src_offsets = src_column.get_offset();
    const uint64_t src_begin = src_offsets[index];
    const uint64_t src_end = src_offsets[index + 1];
    const size_t str_size = src_end - src_begin;
    auto& bytes = get_bytes();

    const size_t prev_num_offsets = _offsets.size();
    const uint64_t dst_begin = _offsets.back();
    const uint64_t dst_end = dst_begin + static_cast<uint64_t>(str_size) * size;

    if (str_size == 0) {
        _offsets.append_empty_values(size);
        invalidate_slice_cache();
        return;
    }

    _offsets.resize_uninitialized(prev_num_offsets + size, dst_end);
    bytes.resize(dst_end);

    const uint8_t* src_bytes = src_column._data_base();
    auto* dest_bytes = bytes.data();
    _offsets.visit_storage([&](auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* dst_offsets = dst_buf.data() + prev_num_offsets;
        uint64_t cur_offset = dst_begin;
        for (size_t i = 0; i < size; ++i) {
            strings::memcpy_inlined(dest_bytes + cur_offset, src_bytes + src_begin, str_size);
            cur_offset += str_size;
            dst_offsets[i] = static_cast<DstValue>(cur_offset);
        }
    });

    invalidate_slice_cache();
}

//TODO(fzh): optimize copy using SIMD
template <typename T>
StatusOr<MutableColumnPtr> BinaryColumnBase<T>::replicate(const Buffer<uint32_t>& offsets) {
    auto dest = BinaryColumnBase<T>::create();
    auto& dest_offsets = dest->get_offset();
    auto& dest_bytes = dest->get_bytes();
    const size_t src_rows = offsets.size() - 1; // this->size() may be larger than offsets->size() - 1
    uint64_t total_size = 0;                    // total size to copy

    _offsets.visit_storage([&](const auto& src_buf) {
        const auto* __restrict src_offset_data = src_buf.data();
        for (size_t i = 0; i < src_rows; ++i) {
            const uint64_t bytes_size = src_offset_data[i + 1] - src_offset_data[i];
            total_size += bytes_size * (offsets[i + 1] - offsets[i]);
        }
    });

    dest_bytes.resize(total_size);
    dest_offsets.resize_uninitialized(static_cast<size_t>(offsets.back()) + 1, total_size);

    const uint8_t* src_bytes = _data_base();
    auto* dest_bytes_data = dest_bytes.data();
    Offsets::visit_storage_pair(_offsets, dest_offsets, [&](const auto& src_buf, auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        const auto* __restrict src_offset_data = src_buf.data();
        auto* __restrict dst_offset_data = dst_buf.data();
        uint64_t pos = 0;

        dst_offset_data[0] = 0;
        for (size_t i = 0; i < src_rows; ++i) {
            const uint64_t src_begin = src_offset_data[i];
            const size_t bytes_size = src_offset_data[i + 1] - src_begin;
            for (uint32_t j = offsets[i]; j < offsets[i + 1]; ++j) {
                strings::memcpy_inlined(dest_bytes_data + pos, src_bytes + src_begin, bytes_size);
                pos += bytes_size;
                dst_offset_data[j + 1] = static_cast<DstValue>(pos);
            }
        }
    });

    return dest;
}

template <typename T>
bool BinaryColumnBase<T>::append_strings(const Slice* data, size_t size) {
    if (UNLIKELY(size == 0)) {
        return true;
    }

    auto& bytes = get_bytes();
    uint64_t append_bytes = 0;
    for (size_t i = 0; i < size; ++i) {
        append_bytes += data[i].size;
    }

    if (append_bytes == 0) {
        _offsets.append_empty_values(size);
        invalidate_slice_cache();
        return true;
    }

    const size_t prev_num_offsets = _offsets.size();
    const uint64_t dst_begin = _offsets.back();
    const uint64_t dst_end = dst_begin + append_bytes;
    _offsets.resize_uninitialized(prev_num_offsets + size, dst_end);
    bytes.resize(dst_end);

    auto* bytes_ptr = bytes.data();
    const bool small_payload = append_bytes / size <= 16;

    _offsets.visit_storage([&](auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* __restrict dst_offsets = dst_buf.data() + prev_num_offsets;
        uint64_t cur_offset = dst_begin;

        if (small_payload) {
            for (size_t i = 0; i < size; ++i) {
                const size_t str_size = data[i].size;
                const auto* const p = reinterpret_cast<const Bytes::value_type*>(data[i].data);
                strings::memcpy_inlined(bytes_ptr + cur_offset, p, str_size);
                cur_offset += str_size;
                dst_offsets[i] = static_cast<DstValue>(cur_offset);
            }
        } else {
            for (size_t i = 0; i < size; ++i) {
                cur_offset += data[i].size;
                dst_offsets[i] = static_cast<DstValue>(cur_offset);
            }
        }
    });

    if (!small_payload) {
        uint64_t cur_offset = dst_begin;
        for (size_t i = 0; i < size; ++i) {
            const size_t str_size = data[i].size;
            const auto* const p = reinterpret_cast<const Bytes::value_type*>(data[i].data);
            strings::memcpy_inlined(bytes_ptr + cur_offset, p, str_size);
            cur_offset += str_size;
        }
    }

    invalidate_slice_cache();
    return true;
}

// NOTE: this function should not be inlined. If this function is inlined,
// the append_strings_overflow will be slower by 30%
template <typename T, size_t copy_length>
void append_fixed_length(const Slice* data, size_t data_size, Bytes* bytes,
                         typename BinaryColumnBase<T>::Offsets* offsets) __attribute__((noinline));

template <typename T, size_t copy_length>
void append_fixed_length(const Slice* data, size_t data_size, Bytes* bytes,
                         typename BinaryColumnBase<T>::Offsets* offsets) {
    uint64_t new_bytes_size = bytes->size();
    for (size_t i = 0; i < data_size; i++) {
        const auto& s = data[i];
        new_bytes_size += s.size;
    }

    uint64_t offset = bytes->size();
    bytes->resize(new_bytes_size + copy_length);

    const size_t rows = data_size;
    const size_t prev_num_offsets = offsets->size();
    offsets->resize_uninitialized(prev_num_offsets + rows, new_bytes_size);

    offsets->visit_storage([&](auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* __restrict dst_offsets = dst_buf.data() + prev_num_offsets;

        for (size_t i = 0; i < rows; ++i) {
            memcpy(&(*bytes)[offset], data[i].get_data(), copy_length);
            offset += data[i].get_size();
            dst_offsets[i] = static_cast<DstValue>(offset);
        }
    });

    bytes->resize(offset);
}

template <typename T>
bool BinaryColumnBase<T>::append_strings_overflow(const Slice* data, size_t size, size_t max_length) {
    auto& bytes = get_bytes();
    if (max_length <= 8) {
        append_fixed_length<T, 8>(data, size, &bytes, &_offsets);
    } else if (max_length <= 16) {
        append_fixed_length<T, 16>(data, size, &bytes, &_offsets);
    } else if (max_length <= 32) {
        append_fixed_length<T, 32>(data, size, &bytes, &_offsets);
    } else if (max_length <= 64) {
        append_fixed_length<T, 64>(data, size, &bytes, &_offsets);
    } else if (max_length <= 128) {
        append_fixed_length<T, 128>(data, size, &bytes, &_offsets);
    } else {
        return append_strings(data, size);
    }
    invalidate_slice_cache();
    return true;
}

template <typename T>
bool BinaryColumnBase<T>::append_continuous_strings(const Slice* data, size_t size) {
    if (UNLIKELY(size == 0)) {
        return true;
    }

    auto& bytes = get_bytes();
    const uint64_t old_bytes_size = bytes.size();
    const auto* p = reinterpret_cast<const uint8_t*>(data[0].data);
    const auto* q = reinterpret_cast<const uint8_t*>(data[size - 1].data + data[size - 1].size);
    bytes.insert(bytes.end(), p, q);

    const size_t prev_num_offsets = _offsets.size();
    const uint64_t new_size = bytes.size();
    _offsets.resize_uninitialized(prev_num_offsets + size, new_size);

    _offsets.visit_storage([&](auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* __restrict dst_offsets = dst_buf.data() + prev_num_offsets;
        uint64_t offset = old_bytes_size;

        for (size_t i = 0; i < size; ++i) {
            offset += data[i].size;
            dst_offsets[i] = static_cast<DstValue>(offset);
        }
        DCHECK_EQ(offset, new_size);
    });

    invalidate_slice_cache();
    return true;
}

template <typename T>
bool BinaryColumnBase<T>::append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) {
    if (UNLIKELY(size == 0)) return true;
    auto& bytes = get_bytes();
    const uint64_t old_bytes_size = bytes.size();
    const uint64_t new_bytes_size = old_bytes_size + static_cast<uint64_t>(fixed_length) * size;

    // copy blob
    const size_t data_size = size * fixed_length;
    const auto* p = reinterpret_cast<const uint8_t*>(data);
    const auto* q = reinterpret_cast<const uint8_t*>(data + data_size);
    bytes.insert(bytes.end(), p, q);
    DCHECK_EQ(bytes.size(), new_bytes_size);

    // copy offsets
    const size_t prev_num_offsets = _offsets.size();
    _offsets.resize_uninitialized(prev_num_offsets + size, new_bytes_size);

    _offsets.visit_storage([&](auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* off_data = dst_buf.data() + prev_num_offsets;
        size_t i = 0;
        uint64_t offset = old_bytes_size;
#ifdef __AVX2__
        if constexpr (std::is_same_v<DstValue, uint32_t>) {
            if (new_bytes_size < std::numeric_limits<uint32_t>::max()) {
                const size_t times = size / 8;

#define FX(m) (m * fixed_length)
#define BFX(m) (static_cast<int>(old_bytes_size + m * fixed_length))
                __m256i base = _mm256_set_epi32(BFX(8), BFX(7), BFX(6), BFX(5), BFX(4), BFX(3), BFX(2), BFX(1));
                __m256i delta = _mm256_set_epi32(FX(8), FX(8), FX(8), FX(8), FX(8), FX(8), FX(8), FX(8));
                for (size_t t = 0; t < times; t++) {
                    _mm256_storeu_si256((__m256i*)off_data, base);
                    base = _mm256_add_epi32(base, delta);
                    off_data += 8;
                }

                i = times * 8;
                offset += static_cast<uint64_t>(fixed_length) * i;
            }
        }
#endif
        for (; i < size; i++) {
            offset += fixed_length;
            *(off_data++) = static_cast<DstValue>(offset);
        }
        DCHECK_EQ(offset, new_bytes_size);
    });

    return true;
}

template <typename T>
void BinaryColumnBase<T>::append_bytes(char* const* data, uint32_t* length, size_t size) {
    auto& bytes = get_bytes();
    for (size_t i = 0; i < size; i++) {
        bytes.insert(bytes.end(), data[i], data[i] + length[i]);
    }
    invalidate_slice_cache();
}

template <typename T, size_t copy_length>
void append_bytes_fixed_length(char* const* data, const uint32_t* lengths, size_t data_size, Bytes* bytes,
                               const typename BinaryColumnBase<T>::Offsets& offsets) __attribute__((noinline));

template <typename T, size_t copy_length>
__attribute__((noinline)) void append_bytes_fixed_length(char* const* data, const uint32_t* lengths, size_t data_size,
                                                         Bytes* bytes,
                                                         const typename BinaryColumnBase<T>::Offsets& offsets) {
    size_t length = offsets.size();
    size_t byte_sizes = offsets[length - 1];

    size_t offset = bytes->size();
    bytes->resize(byte_sizes + copy_length);

    for (size_t i = 0; i < data_size; ++i) {
        memcpy(&(*bytes)[offset], data[i], copy_length);
        offset += lengths[i];
    }

    bytes->resize(offset);
}

template <typename T>
void BinaryColumnBase<T>::append_bytes_overflow(char* const* data, uint32_t* lengths, size_t size, size_t max_length) {
    auto& bytes = get_bytes();
    if (max_length <= 8) {
        append_bytes_fixed_length<T, 8>(data, lengths, size, &bytes, _offsets);
    } else if (max_length <= 16) {
        append_bytes_fixed_length<T, 16>(data, lengths, size, &bytes, _offsets);
    } else if (max_length <= 32) {
        append_bytes_fixed_length<T, 32>(data, lengths, size, &bytes, _offsets);
    } else if (max_length <= 64) {
        append_bytes_fixed_length<T, 64>(data, lengths, size, &bytes, _offsets);
    } else if (max_length <= 128) {
        append_bytes_fixed_length<T, 128>(data, lengths, size, &bytes, _offsets);
    } else {
        append_bytes(data, lengths, size);
    }
    invalidate_slice_cache();
}

template <typename T>
void BinaryColumnBase<T>::append_value_multiple_times(const void* value, size_t count) {
    if (UNLIKELY(count == 0)) {
        return;
    }

    const auto* slice = reinterpret_cast<const Slice*>(value);
    auto& bytes = get_bytes();
    const size_t prev_num_offsets = _offsets.size();
    const uint64_t old_bytes_size = bytes.size();
    const uint64_t dst_end = old_bytes_size + static_cast<uint64_t>(slice->size) * count;

    if (slice->size == 0) {
        _offsets.append_empty_values(count);
        invalidate_slice_cache();
        return;
    }

    _offsets.resize_uninitialized(prev_num_offsets + count, dst_end);
    bytes.resize(dst_end);

    const auto* const p = reinterpret_cast<const uint8_t*>(slice->data);
    auto* bytes_ptr = bytes.data();

    _offsets.visit_storage([&](auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* __restrict dst_offsets = dst_buf.data() + prev_num_offsets;
        uint64_t offset = old_bytes_size;

        for (size_t i = 0; i < count; ++i) {
            strings::memcpy_inlined(bytes_ptr + offset, p, slice->size);
            offset += slice->size;
            dst_offsets[i] = static_cast<DstValue>(offset);
        }
    });

    invalidate_slice_cache();
}

template <typename T>
void BinaryColumnBase<T>::build_slices(Container& slices) const {
    DCHECK(_offsets.size() > 0);

    slices.resize(_offsets.size() - 1);
    const uint8_t* data_ptr = _data_base();
    _offsets.visit_storage([&](const auto& offsets_buf) {
        const auto* __restrict offsets = offsets_buf.data();
        for (size_t i = 0; i + 1 < offsets_buf.size(); ++i) {
            slices[i] = {data_ptr + offsets[i], offsets[i + 1] - offsets[i]};
        }
    });
}

template <typename T>
void BinaryColumnBase<T>::_build_german_strings() const {
    DCHECK(_offsets.size() > 0);
    _german_strings_cache = false;
    _german_strings.clear();

    const auto num_rows = _offsets.size() - 1;
    _german_strings.resize(num_rows);

    const auto* base = _data_base();
    _offsets.visit_storage([&](const auto& offsets_buf) {
        const auto* __restrict offsets = offsets_buf.data();
        for (size_t i = 0; i < num_rows; ++i) {
            _german_strings[i] = GermanString(base + offsets[i], offsets[i + 1] - offsets[i]);
        }
    });
    _german_strings_cache = true;
}

template <typename T>
void BinaryColumnBase<T>::_ensure_materialized() {
    if (_resource.empty()) {
        return;
    }
    size_t bytes = _total_bytes();
    _bytes.resize(bytes);
    if (bytes > 0) {
        strings::memcpy_inlined(_bytes.data(), reinterpret_cast<const uint8_t*>(_resource.data()), bytes);
    }
    _resource.reset();
    invalidate_slice_cache();
}

template <typename T>
void BinaryColumnBase<T>::fill_default(const Filter& filter) {
    std::vector<uint32_t> indexes;
    for (size_t i = 0; i < filter.size(); i++) {
        size_t len = _offsets[i + 1] - _offsets[i];
        if (filter[i] == 1 && len > 0) {
            indexes.push_back(static_cast<uint32_t>(i));
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
void BinaryColumnBase<T>::update_rows(const Column& src, const uint32_t* indexes) {
    const auto& src_column = down_cast<const BinaryColumnBase<T>&>(src);
    size_t replace_num = src.size();
    bool need_resize = false;
    for (size_t i = 0; i < replace_num; ++i) {
        DCHECK_LT(indexes[i], _offsets.size());
        size_t cur_len = _offsets[indexes[i] + 1] - _offsets[indexes[i]];
        size_t new_len = src_column._offsets[i + 1] - src_column._offsets[i];
        if (cur_len != new_len) {
            need_resize = true;
            break;
        }
    }

    if (!need_resize) {
        auto& bytes = get_bytes();
        auto* dest_bytes = bytes.data();
        const uint8_t* src_bytes = src_column._data_base();
        const auto& src_offsets = src_column.get_offset();
        for (size_t i = 0; i < replace_num; ++i) {
            size_t str_size = src_offsets[i + 1] - src_offsets[i];
            strings::memcpy_inlined(dest_bytes + _offsets[indexes[i]], src_bytes + src_offsets[i], str_size);
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
}

template <typename T>
void BinaryColumnBase<T>::assign(size_t n, size_t idx) {
    const uint8_t* base = _data_base();
    std::string value =
            std::string(reinterpret_cast<const char*>(base + _offsets[idx]), _offsets[idx + 1] - _offsets[idx]);
    _resource.reset();

    const size_t value_size = value.size();
    const uint64_t total_bytes = static_cast<uint64_t>(value_size) * n;
    _bytes.resize(total_bytes);

    _offsets.clear();
    _offsets.emplace_back(0);
    if (total_bytes == 0) {
        _offsets.append_empty_values(n);
        invalidate_slice_cache();
        return;
    }

    _offsets.resize_uninitialized(n + 1, total_bytes);
    auto* bytes = _bytes.data();
    const auto* const start = reinterpret_cast<const Bytes::value_type*>(value.data());
    _offsets.visit_storage([&](auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* __restrict dst_offsets = dst_buf.data();
        uint64_t offset = 0;

        dst_offsets[0] = 0;
        for (size_t i = 0; i < n; ++i) {
            strings::memcpy_inlined(bytes + offset, start, value_size);
            offset += value_size;
            dst_offsets[i + 1] = static_cast<DstValue>(offset);
        }
    });

    invalidate_slice_cache();
}

//TODO(kks): improve this
template <typename T>
void BinaryColumnBase<T>::remove_first_n_values(size_t count) {
    DCHECK_LE(count, _offsets.size() - 1);
    size_t remain_size = _offsets.size() - 1 - count;

    MutableColumnPtr column = cut(count, remain_size);
    auto* binary_column = down_cast<BinaryColumnBase<T>*>(column.get());
    _offsets = std::move(binary_column->_offsets);
    _bytes = std::move(binary_column->get_bytes());
    _resource.reset();
    invalidate_slice_cache();
}

template <typename T>
MutableColumnPtr BinaryColumnBase<T>::cut(size_t start, size_t length) const {
    auto result = this->create();

    if (start >= size() || length == 0) {
        return result;
    }

    size_t upper = start + std::min(length, _offsets.size() - start);
    const uint64_t start_offset = _offsets[start];
    const uint64_t copy_bytes = _offsets[upper] - start_offset;

    // offset re-compute
    auto& result_offsets = result->get_offset();
    result_offsets.resize_uninitialized(upper - start + 1, copy_bytes);
    // Always set offsets[0] to 0, in order to easily get element
    Offsets::visit_storage_pair(result_offsets, _offsets, [&](auto& dst_buf, const auto& src_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* __restrict dst_offsets = dst_buf.data();
        const auto* __restrict src_offsets = src_buf.data();

        dst_offsets[0] = 0;
        for (size_t i = start + 1, j = 1; i < upper + 1; ++i, ++j) {
            dst_offsets[j] = static_cast<DstValue>(static_cast<uint64_t>(src_offsets[i]) - start_offset);
        }
    });

    // copy value
    const uint8_t* base = _data_base();
    result->get_bytes().resize(copy_bytes);
    strings::memcpy_inlined(result->get_bytes().data(), base + start_offset, copy_bytes);

    return result;
}

template <typename T>
size_t BinaryColumnBase<T>::filter_range(const Filter& filter, size_t from, size_t to) {
    auto start_offset = from;
    auto result_offset = from;

    auto& bytes = get_bytes();
    uint8_t* data = bytes.data();

    _offsets.visit_storage([&](auto& offsets_buf) {
        auto* offset_data = offsets_buf.data();

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
                const size_t bytes_size = offset_data[start_offset + batch_nums] - offset_data[start_offset];
                memmove(data + offset_data[result_offset], data + offset_data[start_offset], bytes_size);

                // set offsets, try vectorized
                for (int i = 0; i < batch_nums; ++i) {
                    // TODO: performance, all sub one same offset ?
                    offset_data[result_offset + i + 1] =
                            offset_data[result_offset + i] + offset_data[start_offset + i + 1] -
                            offset_data[start_offset + i];
                }

                result_offset += batch_nums;
            } else {
                // skip not hit row, it's will reduce compare when filter layout is sparse,
                // like "00010001...", but is ineffective when the filter layout is dense.

                uint32_t zero_count = Bits::CountTrailingZerosNonZero32(mask);
                uint32_t i = zero_count;
                while (i < batch_nums) {
                    mask = zero_count < 31 ? mask >> (zero_count + 1) : 0;

                    const size_t bytes_size = offset_data[start_offset + i + 1] - offset_data[start_offset + i];
                    // copy date
                    memmove(data + offset_data[result_offset], data + offset_data[start_offset + i], bytes_size);

                    // set offsets
                    offset_data[result_offset + 1] = offset_data[result_offset] + bytes_size;
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
                DCHECK_GE(offset_data[i + 1], offset_data[i]);
                const size_t bytes_size = offset_data[i + 1] - offset_data[i];
                // copy data
                memmove(data + offset_data[result_offset], data + offset_data[i], bytes_size);

                // set offsets
                offset_data[result_offset + 1] = offset_data[result_offset] + bytes_size;

                result_offset++;
            }
        }
    });

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
    _offsets.visit_storage([&](const auto& offsets_buf) {
        const auto* __restrict offsets = offsets_buf.data();
        for (size_t i = 0; i + 1 < offsets_buf.size(); ++i) {
            // it's safe to cast, because max size of one string is 2^32
            const size_t curr_length = offsets[i + 1] - offsets[i];
            DCHECK_LE(curr_length, std::numeric_limits<uint32_t>::max());
            max_size = std::max(max_size, static_cast<uint32_t>(curr_length));
        }
    });
    // TODO: may be overflow here, i will solve it later
    return max_size + sizeof(uint32_t);
}

template <typename T>
size_t BinaryColumnBase<T>::serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval,
                                                        uint32_t max_row_size, size_t start, size_t count) const {
    return serialize_batch_at_interval_with_null_masks(dst, byte_offset, byte_interval, max_row_size, start, count,
                                                       nullptr);
}

template <typename T>
size_t BinaryColumnBase<T>::serialize_batch_at_interval_with_null_masks(uint8_t* dst, size_t byte_offset,
                                                                        size_t byte_interval, uint32_t max_row_size,
                                                                        size_t start, size_t count,
                                                                        const uint8_t* null_masks) const {
    auto process = [&]<bool IsNullable>() {
        dst += byte_offset;
        if constexpr (IsNullable) {
            dst++; // reserve one byte for null flag
        }
        if (UNLIKELY(count == 0)) {
            return max_row_size;
        }

        const uint8_t* bytes = _data_base();
        _offsets.visit_storage([&](const auto& offsets_buf) {
            const auto* __restrict offsets = offsets_buf.data();
            auto begin = offsets[start];
            for (size_t i = start; i < start + count; ++i, dst += byte_interval) {
                const auto end = offsets[i + 1];
                if constexpr (IsNullable) {
                    if (null_masks[i]) {
                        *dst = 0xFF; // Invalid UTF-8 string.
                        begin = end;
                        continue;
                    }
                }

                const size_t length = end - begin;
                if (length > max_row_size) {
                    *dst = 0xFF;
                } else if (length > 0 && bytes[end - 1] == 0) {
                    *dst = 0xFF;
                } else {
                    strings::memcpy_inlined(dst, bytes + begin, length);
                }
                begin = end;
            }
        });

        return max_row_size;
    };

    if (null_masks != nullptr) {
        return process.template operator()<true>();
    } else {
        return process.template operator()<false>();
    }
}

template <typename T>
uint32_t BinaryColumnBase<T>::serialize_default(uint8_t* pos) const {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t binary_size = 0;
    strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
    return sizeof(uint32_t);
}

template <typename T>
void BinaryColumnBase<T>::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                          uint32_t max_one_row_size) const {
    const uint8_t* base = _data_base();
    uint32_t* sizes = slice_sizes.data();

    _offsets.visit_storage([&](const auto& offsets_buf) {
        const auto* __restrict offsets_data = offsets_buf.data();
        uint64_t offset = offsets_data[0];
        for (size_t i = 0; i < chunk_size; ++i) {
            const uint64_t next_offset = offsets_data[i + 1];
            const uint32_t binary_size = static_cast<uint32_t>(next_offset - offset);
            uint8_t* pos = dst + i * max_one_row_size + sizes[i];

            strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
            strings::memcpy_inlined(pos + sizeof(uint32_t), base + offset, binary_size);
            sizes[i] += sizeof(uint32_t) + binary_size;
            offset = next_offset;
        }
    });
}

template <typename T>
const uint8_t* BinaryColumnBase<T>::deserialize_and_append(const uint8_t* pos) {
    // max size of one string is 2^32, so use uint32_t not T
    uint32_t string_size{};
    strings::memcpy_inlined(&string_size, pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    auto& bytes = get_bytes();
    size_t old_size = bytes.size();
    bytes.insert(bytes.end(), pos, pos + string_size);

    _offsets.emplace_back(old_size + string_size);
    return pos + string_size;
}

template <typename T>
void BinaryColumnBase<T>::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    if (UNLIKELY(chunk_size == 0)) {
        return;
    }

    // max size of one string is 2^32, so use uint32_t not T
    uint64_t append_bytes = 0;
    for (size_t i = 0; i < chunk_size; ++i) {
        uint32_t string_size{};
        strings::memcpy_inlined(&string_size, srcs[i].data, sizeof(uint32_t));
        append_bytes += string_size;
    }

    auto& bytes = get_bytes();
    const size_t prev_num_offsets = _offsets.size();
    const uint64_t dst_begin = bytes.size();
    const uint64_t dst_end = dst_begin + append_bytes;

    _offsets.resize_uninitialized(prev_num_offsets + chunk_size, dst_end);
    bytes.resize(dst_end);

    auto* bytes_ptr = bytes.data();
    _offsets.visit_storage([&](auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* __restrict dst_offsets = dst_buf.data() + prev_num_offsets;
        uint64_t offset = dst_begin;

        for (size_t i = 0; i < chunk_size; ++i) {
            const uint8_t* pos = reinterpret_cast<const uint8_t*>(srcs[i].data);
            uint32_t string_size{};
            strings::memcpy_inlined(&string_size, pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);
            strings::memcpy_inlined(bytes_ptr + offset, pos, string_size);
            offset += string_size;
            dst_offsets[i] = static_cast<DstValue>(offset);
            srcs[i].data = reinterpret_cast<char*>(const_cast<uint8_t*>(pos + string_size));
        }
    });
}

template <typename T>
void BinaryColumnBase<T>::serialize_batch_with_null_masks(uint8_t* dst, Buffer<uint32_t>& slice_sizes,
                                                          size_t chunk_size, uint32_t max_one_row_size,
                                                          const uint8_t* null_masks, bool has_null) const {
    uint32_t* sizes = slice_sizes.data();
    const uint8_t* base = _data_base();

    if (!has_null) {
        const bool null_flag = false;
        _offsets.visit_storage([&](const auto& offsets_buf) {
            const auto* __restrict offsets_data = offsets_buf.data();
            uint64_t offset = offsets_data[0];
            for (size_t i = 0; i < chunk_size; ++i) {
                const uint64_t next_offset = offsets_data[i + 1];
                const uint32_t binary_size = static_cast<uint32_t>(next_offset - offset);
                uint8_t* pos = dst + i * max_one_row_size + sizes[i];

                strings::memcpy_inlined(pos, &null_flag, sizeof(bool));
                pos += sizeof(bool);
                strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
                strings::memcpy_inlined(pos + sizeof(uint32_t), base + offset, binary_size);
                sizes[i] += sizeof(bool) + sizeof(uint32_t) + binary_size;
                offset = next_offset;
            }
        });
    } else {
        _offsets.visit_storage([&](const auto& offsets_buf) {
            const auto* __restrict offsets_data = offsets_buf.data();
            uint64_t offset = offsets_data[0];
            for (size_t i = 0; i < chunk_size; ++i) {
                const uint64_t next_offset = offsets_data[i + 1];
                uint8_t* pos = dst + i * max_one_row_size + sizes[i];
                strings::memcpy_inlined(pos, null_masks + i, sizeof(bool));
                sizes[i] += sizeof(bool);

                if (!null_masks[i]) {
                    const uint32_t binary_size = static_cast<uint32_t>(next_offset - offset);
                    pos += sizeof(bool);
                    strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
                    strings::memcpy_inlined(pos + sizeof(uint32_t), base + offset, binary_size);
                    sizes[i] += sizeof(uint32_t) + binary_size;
                }
                offset = next_offset;
            }
        });
    }
}

template <typename T>
void BinaryColumnBase<T>::deserialize_and_append_batch_nullable(Buffer<Slice>& srcs, size_t chunk_size,
                                                                Buffer<uint8_t>& is_nulls, bool& has_null) {
    if (UNLIKELY(chunk_size == 0)) {
        return;
    }

    uint64_t append_bytes = 0;
    is_nulls.reserve(is_nulls.size() + chunk_size);
    for (size_t i = 0; i < chunk_size; ++i) {
        bool null{};
        strings::memcpy_inlined(&null, srcs[i].data, sizeof(bool));
        is_nulls.emplace_back(null);
        if (null) {
            has_null = true;
        } else {
            uint32_t string_size{};
            strings::memcpy_inlined(&string_size, srcs[i].data + sizeof(bool), sizeof(uint32_t));
            append_bytes += string_size;
        }
    }

    auto& bytes = get_bytes();
    const size_t prev_num_offsets = _offsets.size();
    const uint64_t dst_begin = bytes.size();
    const uint64_t dst_end = dst_begin + append_bytes;

    _offsets.resize_uninitialized(prev_num_offsets + chunk_size, dst_end);
    bytes.resize(dst_end);

    auto* bytes_ptr = bytes.data();
    _offsets.visit_storage([&](auto& dst_buf) {
        using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
        auto* __restrict dst_offsets = dst_buf.data() + prev_num_offsets;
        uint64_t offset = dst_begin;

        for (size_t i = 0; i < chunk_size; ++i) {
            const uint8_t* pos = reinterpret_cast<const uint8_t*>(srcs[i].data);
            bool null{};
            strings::memcpy_inlined(&null, pos, sizeof(bool));
            pos += sizeof(bool);

            if (!null) {
                uint32_t string_size{};
                strings::memcpy_inlined(&string_size, pos, sizeof(uint32_t));
                pos += sizeof(uint32_t);
                strings::memcpy_inlined(bytes_ptr + offset, pos, string_size);
                offset += string_size;
                pos += string_size;
            }

            dst_offsets[i] = static_cast<DstValue>(offset);
            srcs[i].data = reinterpret_cast<char*>(const_cast<uint8_t*>(pos));
        }
    });
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
        const auto* src = _data_base() + _offsets[i];

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
void BinaryColumnBase<T>::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    size_t start = _offsets[idx];
    size_t len = _offsets[idx + 1] - start;
    const char* base = reinterpret_cast<const char*>(_data_base());
    if (_is_binary_type) {
        buf->push_binary(base + start, len);
    } else {
        buf->push_string(base + start, len);
    }
}

template <typename T>
std::string BinaryColumnBase<T>::debug_item(size_t idx) const {
    std::string s;
    auto slice = get_slice(idx);
    s.reserve(slice.size + 2);
    s.push_back('\'');
    s.append(slice.data, slice.size);
    s.push_back('\'');
    return s;
}

template <typename T>
std::string BinaryColumnBase<T>::raw_item_value(size_t idx) const {
    std::string s;
    auto slice = get_slice(idx);
    s.reserve(slice.size);
    s.append(slice.data, slice.size);
    return s;
}

template <typename T>
StatusOr<MutableColumnPtr> BinaryColumnBase<T>::upgrade_if_overflow() {
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);

    if constexpr (std::is_same_v<T, uint32_t>) {
        if (_offsets.size() > Column::MAX_CAPACITY_LIMIT) {
            return Status::InternalError("column size exceed the limit");
        }
        if (!_offsets.is_large()) {
            return nullptr;
        }

        auto new_column = BinaryColumnBase<uint64_t>::create();
        new_column->get_bytes().swap(get_bytes());
        new_column->get_offset() = std::move(_offsets);

        // Keep the moved-from column internally consistent until the caller replaces it.
        _offsets.reset();
        _offsets.emplace_back(0);
        invalidate_slice_cache();
        return new_column;
    } else {
        return nullptr;
    }
}

template <typename T>
StatusOr<MutableColumnPtr> BinaryColumnBase<T>::downgrade() {
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);

    if constexpr (std::is_same_v<T, uint32_t>) {
        return nullptr;
    } else {
        if (_offsets.back() >= Column::MAX_CAPACITY_LIMIT) {
            return Status::InternalError("column size exceed the limit, can't downgrade");
        }

        auto new_column = BinaryColumn::create();
        auto& dst_offsets = new_column->get_offset();
        dst_offsets.resize_uninitialized(_offsets.size(), _offsets.back());
        Offsets::visit_storage_pair(dst_offsets, _offsets, [&](auto& dst_buf, const auto& src_buf) {
            using DstValue = typename std::decay_t<decltype(dst_buf)>::value_type;
            using SrcValue = typename std::decay_t<decltype(src_buf)>::value_type;
            auto* __restrict dst_offsets_data = dst_buf.data();
            const auto* __restrict src_offsets = src_buf.data();

            if constexpr (std::is_same_v<SrcValue, DstValue>) {
                strings::memcpy_inlined(dst_offsets_data, src_offsets, _offsets.size() * sizeof(DstValue));
            } else {
                for (size_t i = 0; i < _offsets.size(); i++) {
                    dst_offsets_data[i] = static_cast<DstValue>(src_offsets[i]);
                }
            }
        });

        new_column->get_bytes().swap(get_bytes());

        // Keep the moved-from column internally consistent until the caller replaces it.
        _offsets.reset();
        _offsets.emplace_back(0);
        invalidate_slice_cache();
        return new_column;
    }
}

template <typename T>
bool BinaryColumnBase<T>::has_large_column() const {
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);
    return std::is_same_v<T, uint64_t>;
}

template <typename T>
Status BinaryColumnBase<T>::capacity_limit_reached() const {
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);
    // AdaptiveOffsets can promote total byte offsets to uint64_t; row count remains limited by uint32_t APIs.
    if (_offsets.size() >= Column::MAX_CAPACITY_LIMIT) {
        const char* column_name = has_large_column() ? "large binary column" : "binary column";
        return Status::CapacityLimitExceed(strings::Substitute("Total row count of $0 exceed the limit: $1",
                                                               column_name,
                                                               std::to_string(Column::MAX_CAPACITY_LIMIT)));
    }
    return Status::OK();
}

template class BinaryColumnBase<uint32_t>;
template class BinaryColumnBase<uint64_t>;
} // namespace starrocks
