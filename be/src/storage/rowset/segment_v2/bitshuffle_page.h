// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bitshuffle_page.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <ostream>

#include "column/fixed_length_column.h"
#include "gutil/port.h"
#include "runtime/date_value.hpp"
#include "storage/olap_common.h"
#include "storage/rowset/segment_v2/bitshuffle_wrapper.h"
#include "storage/rowset/segment_v2/common.h"
#include "storage/rowset/segment_v2/options.h"
#include "storage/rowset/segment_v2/page_builder.h"
#include "storage/rowset/segment_v2/page_decoder.h"
#include "storage/types.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace starrocks {
namespace segment_v2 {

enum { BITSHUFFLE_PAGE_HEADER_SIZE = 16 };

std::string bitshuffle_error_msg(int64_t err);

// BitshufflePageBuilder bitshuffles and compresses the bits of fixed
// size type blocks with lz4.
//
// The page format is as follows:
//
// 1. Header: (16 bytes total)
//
//    <num_elements> [32-bit]
//      The number of elements encoded in the page.
//
//    <compressed_size> [32-bit]
//      The post-compression size of the page, including this header.
//
//    <padded_num_elements> [32-bit]
//      Padding is needed to meet the requirements of the bitshuffle
//      library such that the input/output is a multiple of 8. Some
//      ignored elements are appended to the end of the page if necessary
//      to meet this requirement.
//
//      This header field is the post-padding element count.
//
//    <elem_size_bytes> [32-bit]
//      The size of the elements, in bytes, as actually encoded. In the
//      case that all of the data in a page can fit into a smaller
//      integer type, then we may choose to encode that smaller type
//      to save CPU costs.
//
//      This is currently only implemented in the UINT32 page type.
//
//   NOTE: all on-disk ints are encoded little-endian
//
// 2. Element data
//
//    The header is followed by the bitshuffle-compressed element data.
//
template <FieldType Type>
class BitshufflePageBuilder final : public PageBuilder {
public:
    explicit BitshufflePageBuilder(const PageBuilderOptions& options)
            : _reserved_head_size(0), _max_count(options.data_page_size / SIZE_OF_TYPE), _count(0), _finished(false) {
        _data.reserve(ALIGN_UP(_max_count, 8u) * SIZE_OF_TYPE);
    }

    void reserve_head(uint8_t head_size) override {
        CHECK(_reserved_head_size == 0);
        _reserved_head_size = head_size;
    }

    bool is_page_full() override { return _count >= _max_count; }

    size_t add(const uint8_t* vals, size_t count) override {
        DCHECK(!_finished);
        size_t to_add = std::min<size_t>(_max_count - _count, count);
        size_t old_sz = _data.size();
        _data.resize(old_sz + to_add * SIZE_OF_TYPE);
        memcpy(&_data[old_sz], vals, to_add * SIZE_OF_TYPE);
        _count += to_add;
        return to_add;
    }

    size_t add_one(const uint8_t* elem) {
        if (_max_count == _count) {
            return 0;
        }
        size_t old_sz = _data.size();
        _data.resize(old_sz + sizeof(SIZE_OF_TYPE));
        _count += 1;
        if constexpr (SIZE_OF_TYPE == 1) {
            *reinterpret_cast<uint8_t*>(&_data[old_sz]) = *elem;
            return 1;
        }
        if constexpr (SIZE_OF_TYPE == 2) {
            *reinterpret_cast<uint16_t*>(&_data[old_sz]) = *reinterpret_cast<const uint16_t*>(elem);
            return 1;
        }
        if constexpr (SIZE_OF_TYPE == 4) {
            *reinterpret_cast<uint32_t*>(&_data[old_sz]) = *reinterpret_cast<const uint32_t*>(elem);
            return 1;
        }
        if constexpr (SIZE_OF_TYPE == 8) {
            *reinterpret_cast<uint64_t*>(&_data[old_sz]) = *reinterpret_cast<const uint64_t*>(elem);
            return 1;
        }
        memcpy(&_data[old_sz], elem, SIZE_OF_TYPE);
        return 1;
    }

    faststring* finish() override {
        if (_count > 0) {
            _first_value = cell(0);
            _last_value = cell(_count - 1);
        }
        return _finish();
    }

    void reset() override {
        _count = 0;
        _data.clear();
        _finished = false;
        DCHECK_EQ(reinterpret_cast<uintptr_t>(_data.data()) & (alignof(CppType) - 1), 0)
                << "buffer must be naturally-aligned";
    }

    size_t count() const override { return _count; }

    uint64_t size() const override { return _data.size(); }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, &_first_value, SIZE_OF_TYPE);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, &_last_value, SIZE_OF_TYPE);
        return Status::OK();
    }

private:
    typedef typename TypeTraits<Type>::CppType CppType;

    faststring* _finish() {
        // Do padding so that the input num of element is multiple of 8.
        int num_elems_after_padding = ALIGN_UP(_count, 8U);
        int padding_elems = num_elems_after_padding - _count;
        int padding_bytes = padding_elems * SIZE_OF_TYPE;
        for (int i = 0; i < padding_bytes; i++) {
            _data.push_back(0);
        }

        _compressed_data.resize(_reserved_head_size + BITSHUFFLE_PAGE_HEADER_SIZE +
                                bitshuffle::compress_lz4_bound(num_elems_after_padding, SIZE_OF_TYPE, 0));

        uint8_t* compressed_data = _compressed_data.data() + _reserved_head_size;
        uint8_t* uncompressed_data = _data.data();

        int64_t bytes = bitshuffle::compress_lz4(uncompressed_data, &compressed_data[BITSHUFFLE_PAGE_HEADER_SIZE],
                                                 num_elems_after_padding, SIZE_OF_TYPE, 0);
        CHECK_GE(bytes, 0) << "Fail to bitshuffle compress, " << bitshuffle_error_msg(bytes);
        // update header
        encode_fixed32_le(&compressed_data[0], _count);
        encode_fixed32_le(&compressed_data[4], BITSHUFFLE_PAGE_HEADER_SIZE + bytes);
        encode_fixed32_le(&compressed_data[8], num_elems_after_padding);
        encode_fixed32_le(&compressed_data[12], SIZE_OF_TYPE);
        _finished = true;
        // before build(), update buffer length to the actual compressed size
        _compressed_data.resize(_reserved_head_size + BITSHUFFLE_PAGE_HEADER_SIZE + bytes);
        return &_compressed_data;
    }

    CppType cell(int idx) const {
        DCHECK_GE(idx, 0);
        CppType ret;
        memcpy(&ret, &_data[idx * SIZE_OF_TYPE], SIZE_OF_TYPE);
        return ret;
    }

    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };
    uint8_t _reserved_head_size;
    uint32_t _max_count;
    uint32_t _count;
    faststring _data;
    faststring _compressed_data;
    CppType _first_value;
    CppType _last_value;
    bool _finished;
};

template <FieldType Type>
class BitShufflePageDecoder final : public PageDecoder {
public:
    BitShufflePageDecoder(Slice data, const PageDecoderOptions& options)
            : _data(data),
              _options(options),
              _parsed(false),
              _num_elements(0),
              _compressed_size(0),
              _num_element_after_padding(0),
              _size_of_element(0),
              _cur_index(0) {}

    Status init() override {
        CHECK(!_parsed);
        if (_data.size < BITSHUFFLE_PAGE_HEADER_SIZE) {
            std::stringstream ss;
            ss << "file corrupton: invalid data size:" << _data.size << ", header size:" << BITSHUFFLE_PAGE_HEADER_SIZE;
            return Status::InternalError(ss.str());
        }
        _num_elements = decode_fixed32_le((const uint8_t*)&_data[0]);
        _compressed_size = decode_fixed32_le((const uint8_t*)&_data[4]);
        if (_compressed_size != _data.size) {
            std::stringstream ss;
            ss << "Size information unmatched, _compressed_size:" << _compressed_size
               << ", _num_elements:" << _num_elements << ", data size:" << _data.size;
            return Status::InternalError(ss.str());
        }
        _num_element_after_padding = decode_fixed32_le((const uint8_t*)&_data[8]);
        if (_num_element_after_padding != ALIGN_UP(_num_elements, 8U)) {
            std::stringstream ss;
            ss << "num of element information corrupted,"
               << " _num_element_after_padding:" << _num_element_after_padding << ", _num_elements:" << _num_elements;
            return Status::InternalError(ss.str());
        }
        _size_of_element = decode_fixed32_le((const uint8_t*)&_data[12]);
        switch (_size_of_element) {
        case 1:
        case 2:
        case 3:
        case 4:
        case 8:
        case 12:
        case 16:
            break;
        default:
            std::stringstream ss;
            ss << "invalid size_of_elem:" << _size_of_element;
            return Status::InternalError(ss.str());
        }

        // Currently, only the UINT32 block encoder supports expanding size:
        if (UNLIKELY(Type != OLAP_FIELD_TYPE_UNSIGNED_INT && _size_of_element != SIZE_OF_TYPE)) {
            std::stringstream ss;
            ss << "invalid size info. size of element:" << _size_of_element << ", SIZE_OF_TYPE:" << SIZE_OF_TYPE
               << ", type:" << Type;
            return Status::InternalError(ss.str());
        }
        if (UNLIKELY(_size_of_element > SIZE_OF_TYPE)) {
            std::stringstream ss;
            ss << "invalid size info. size of element:" << _size_of_element << ", SIZE_OF_TYPE:" << SIZE_OF_TYPE;
            return Status::InternalError(ss.str());
        }

        RETURN_IF_ERROR(_decode());
        _parsed = true;
        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        DCHECK(_parsed) << "Must call init()";
        if (PREDICT_FALSE(_num_elements == 0)) {
            DCHECK_EQ(0, pos);
            return Status::InvalidArgument("invalid pos");
        }

        DCHECK_LE(pos, _num_elements);
        _cur_index = pos;
        return Status::OK();
    }

    Status seek_at_or_after_value(const void* value, bool* exact_match) override {
        DCHECK(_parsed) << "Must call init() firstly";

        if (_num_elements == 0) {
            return Status::NotFound("page is empty");
        }

        size_t left = 0;
        size_t right = _num_elements;

        // find the first value >= target. after loop,
        // - left == index of first value >= target when found
        // - left == _num_elements when not found (all values < target)
        while (left < right) {
            size_t mid = left + (right - left) / 2;
            void* mid_value = &_decoded[mid * SIZE_OF_TYPE];
            if (TypeTraits<Type>::cmp(mid_value, value) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if (left >= _num_elements) {
            return Status::NotFound("all value small than the value");
        }
        void* find_value = &_decoded[left * SIZE_OF_TYPE];
        *exact_match = TypeTraits<Type>::cmp(find_value, value) == 0;

        _cur_index = left;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(*n == 0 || _cur_index >= _num_elements)) {
            *n = 0;
            return Status::OK();
        }

        size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elements - _cur_index));
        _copy_next_values(max_fetch, dst->data());
        *n = max_fetch;
        _cur_index += max_fetch;

        return Status::OK();
    }

    Status next_batch(size_t* count, vectorized::Column* dst) override;

    size_t count() const override { return _num_elements; }

    size_t current_index() const override { return _cur_index; }

    EncodingTypePB encoding_type() const override { return BIT_SHUFFLE; }

private:
    void _copy_next_values(size_t n, void* data) {
        memcpy(data, &_decoded[_cur_index * SIZE_OF_TYPE], n * SIZE_OF_TYPE);
    }

    Status _decode() {
        if (_num_elements > 0) {
            int64_t bytes;
            _decoded.resize(_num_element_after_padding * _size_of_element);
            char* in = const_cast<char*>(&_data[BITSHUFFLE_PAGE_HEADER_SIZE]);
            bytes = bitshuffle::decompress_lz4(in, _decoded.data(), _num_element_after_padding, _size_of_element, 0);
            if (PREDICT_FALSE(bytes < 0)) {
                // Ideally, this should not happen.
                LOG(ERROR) << "bitshuffle decompress failed: " << bitshuffle_error_msg(bytes);
                return Status::RuntimeError("Unshuffle Process failed");
            }
        }
        return Status::OK();
    }

    typedef typename TypeTraits<Type>::CppType CppType;

    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };

    Slice _data;
    PageDecoderOptions _options;
    bool _parsed;
    size_t _num_elements;
    size_t _compressed_size;
    size_t _num_element_after_padding;

    int _size_of_element;
    size_t _cur_index;
    faststring _decoded;
};

template <FieldType Type>
inline Status BitShufflePageDecoder<Type>::next_batch(size_t* count, vectorized::Column* dst) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(_cur_index >= _num_elements)) {
        *count = 0;
        return Status::OK();
    }
    *count = std::min(*count, static_cast<size_t>(_num_elements - _cur_index));
    int n = dst->append_numbers(&_decoded[_cur_index * SIZE_OF_TYPE], *count * SIZE_OF_TYPE);
    DCHECK_EQ(*count, n);
    _cur_index += *count;
    return Status::OK();
}

} // namespace segment_v2
} // namespace starrocks
