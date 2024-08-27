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

#include "storage/rowset/dict_page.h"

#include <memory>
#include <vector>

#include "common/logging.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h" // for Substitute
#include "storage/chunk_helper.h"
#include "storage/range.h"
#include "storage/rowset/bitshuffle_page.h"
#include "util/slice.h" // for Slice
#include "util/unaligned_access.h"

namespace starrocks {

using strings::Substitute;

template <LogicalType Type>
DictPageBuilder<Type>::DictPageBuilder(const PageBuilderOptions& options)
        : _options(options),
          _finished(false),
          _data_page_builder(nullptr),
          _dict_builder(nullptr),
          _encoding_type(DICT_ENCODING) {
    // initially use DICT_ENCODING
    _data_page_builder = std::make_unique<BitshufflePageBuilder<DataTypeTraits<Type>::type>>(options);
    _data_page_builder->reserve_head(BINARY_DICT_PAGE_HEADER_SIZE);
    PageBuilderOptions dict_builder_options;
    dict_builder_options.data_page_size = _options.dict_page_size;
    _dict_builder = std::make_unique<BitshufflePageBuilder<Type>>(dict_builder_options);
    reset();
}

template <LogicalType Type>
bool DictPageBuilder<Type>::is_page_full() {
    if (_data_page_builder->is_page_full()) {
        return true;
    }
    return _encoding_type == DICT_ENCODING && _dict_builder->is_page_full();
}

template <LogicalType Type>
uint32_t DictPageBuilder<Type>::add(const uint8_t* vals, uint32_t count) {
    if (_encoding_type == DICT_ENCODING) {
        DCHECK(!_finished);
        DCHECK_GT(count, 0);
        ValueCodeType value_code = -1;
        // Manually devirtualization.
        auto* code_page = down_cast<BitshufflePageBuilder<DataTypeTraits<Type>::type>*>(_data_page_builder.get());

        if (_data_page_builder->count() == 0) {
            memcpy(&_first_value, vals, sizeof(ValueType));
        }

        for (int i = 0; i < count; ++i) {
            auto& value = *reinterpret_cast<const ValueType*>(vals + i * SIZE_OF_TYPE);
            auto iter = _dictionary.find(value);
            if (iter != _dictionary.end()) {
                value_code = iter->second;
            } else if (_dict_builder->add((const uint8_t*)&value, 1) > 0) {
                value_code = _dictionary.size();
                _dictionary.insert_or_assign(value, value_code);
            } else {
                return i;
            }
            if (code_page->add_one(reinterpret_cast<const uint8_t*>(&value_code)) < 1) {
                return i;
            }
        }
        return count;
    } else {
        DCHECK_EQ(_encoding_type, BIT_SHUFFLE);
        return _data_page_builder->add(vals, count);
    }
}
template <LogicalType Type>
faststring* DictPageBuilder<Type>::finish() {
    DCHECK(!_finished);
    _finished = true;

    faststring* data_slice = _data_page_builder->finish();
    encode_fixed32_le(data_slice->data(), _encoding_type);
    return data_slice;
}

template <LogicalType Type>
void DictPageBuilder<Type>::reset() {
    _finished = false;
    if (_encoding_type == DICT_ENCODING && _dict_builder->is_page_full()) {
        _data_page_builder = std::make_unique<BitshufflePageBuilder<Type>>(_options);
        _data_page_builder->reserve_head(BINARY_DICT_PAGE_HEADER_SIZE);
        _encoding_type = BIT_SHUFFLE;
    } else {
        _data_page_builder->reset();
    }
    _finished = false;
}

template <LogicalType Type>
uint32_t DictPageBuilder<Type>::count() const {
    return _data_page_builder->count();
}

template <LogicalType Type>
uint64_t DictPageBuilder<Type>::size() const {
    return _data_page_builder->size();
}

template <LogicalType Type>
faststring* DictPageBuilder<Type>::get_dictionary_page() {
    return _dict_builder->finish();
}

template <LogicalType Type>
Status DictPageBuilder<Type>::get_first_value(void* value) const {
    DCHECK(_finished);
    if (_data_page_builder->count() == 0) {
        return Status::NotFound("page is empty");
    }
    if (_encoding_type != DICT_ENCODING) {
        return _data_page_builder->get_first_value(value);
    }
    memcpy(value, &_first_value, SIZE_OF_TYPE);
    return Status::OK();
}

template <LogicalType Type>
Status DictPageBuilder<Type>::get_last_value(void* value) const {
    DCHECK(_finished);
    if (_data_page_builder->count() == 0) {
        return Status::NotFound("page is empty");
    }
    if (_encoding_type != DICT_ENCODING) {
        return _data_page_builder->get_last_value(value);
    }
    ValueCodeType value_code;
    RETURN_IF_ERROR(_data_page_builder->get_last_value(&value_code));
    ValueType out = _dict_builder->cell(value_code);
    memcpy(value, &out, SIZE_OF_TYPE);
    return Status::OK();
}

template <LogicalType Type>
bool DictPageBuilder<Type>::is_valid_global_dict(const GlobalDictMap* global_dict) const {
    return false;
}

template <LogicalType Type>
DictPageDecoder<Type>::DictPageDecoder(Slice data)
        : _data(data), _data_page_decoder(nullptr), _parsed(false), _encoding_type(UNKNOWN_ENCODING) {}

template <LogicalType Type>
Status DictPageDecoder<Type>::init() {
    CHECK(!_parsed);
    if (_data.size < BINARY_DICT_PAGE_HEADER_SIZE) {
        return Status::Corruption(
                strings::Substitute("invalid data size:$0, header size:$1", _data.size, BINARY_DICT_PAGE_HEADER_SIZE));
    }
    size_t type = decode_fixed32_le((const uint8_t*)&_data.data[0]);
    _encoding_type = static_cast<EncodingTypePB>(type);
    _data.remove_prefix(BINARY_DICT_PAGE_HEADER_SIZE);
    if (_encoding_type == DICT_ENCODING) {
        // copy the codewords into a temporary buffer first
        // And then copy the strings corresponding to the codewords to the destination buffer
        _data_page_decoder = std::make_unique<BitShufflePageDecoder<DataTypeTraits<Type>::type>>(_data);
    } else if (_encoding_type == BIT_SHUFFLE) {
        _data_page_decoder.reset(new BitShufflePageDecoder<Type>(_data));
    } else {
        LOG(WARNING) << "invalid encoding type:" << _encoding_type;
        return Status::Corruption(strings::Substitute("invalid encoding type:$0", _encoding_type));
    }

    RETURN_IF_ERROR(_data_page_decoder->init());
    _parsed = true;
    return Status::OK();
}

template <LogicalType Type>
Status DictPageDecoder<Type>::seek_to_position_in_page(uint32_t pos) {
    return _data_page_decoder->seek_to_position_in_page(pos);
}

template <LogicalType Type>
void DictPageDecoder<Type>::set_dict_decoder(PageDecoder* dict_decoder) {
    _dict_decoder = down_cast<BitShufflePageDecoder<Type>*>(dict_decoder);
}

template <LogicalType Type>
Status DictPageDecoder<Type>::next_batch(size_t* n, Column* dst) {
    SparseRange<> read_range;
    uint32_t begin = current_index();
    read_range.add(Range<>(begin, begin + *n));
    RETURN_IF_ERROR(next_batch(read_range, dst));
    *n = current_index() - begin;
    return Status::OK();
}

template <LogicalType Type>
Status DictPageDecoder<Type>::next_batch(const SparseRange<>& range, Column* dst) {
    if (_encoding_type == BIT_SHUFFLE) {
        return _data_page_decoder->next_batch(range, dst);
    }

    DCHECK(_parsed);
    DCHECK(_dict_decoder != nullptr) << "dict decoder pointer is nullptr";
    if (_vec_code_buf == nullptr) {
        _vec_code_buf = ChunkHelper::column_from_field_type(DataTypeTraits<Type>::type, false);
    }
    _vec_code_buf->resize(0);
    _vec_code_buf->reserve(range.span_size());

    RETURN_IF_ERROR(_data_page_decoder->next_batch(range, _vec_code_buf.get()));
    size_t nread = _vec_code_buf->size();
    using cast_type = typename CppTypeTraits<DataTypeTraits<Type>::type>::CppType;
    const auto* codewords = reinterpret_cast<const cast_type*>(_vec_code_buf->raw_data());
    std::vector<ValueType> numbers;
    raw::stl_vector_resize_uninitialized(&numbers, nread);
    for (int i = 0; i < nread; ++i) {
        ValueType value;
        _dict_decoder->at_index(codewords[i], &value);
        numbers[i] = value;
    }
    size_t nappend = dst->append_numbers(numbers.data(), numbers.size() * SIZE_OF_TYPE);
    if (UNLIKELY(nappend != numbers.size())) {
        return Status::InternalError(
                fmt::format("append_numbers failed, expected rows[{}], actual rows[{}]", numbers.size(), nappend));
    }
    return Status::OK();
}

template <LogicalType Type>
Status DictPageDecoder<Type>::next_dict_codes(size_t* n, Column* dst) {
    DCHECK(_encoding_type == DICT_ENCODING);
    DCHECK(_parsed);
    return _data_page_decoder->next_batch(n, dst);
}

template <LogicalType Type>
Status DictPageDecoder<Type>::next_dict_codes(const SparseRange<>& range, Column* dst) {
    DCHECK(_encoding_type == DICT_ENCODING);
    DCHECK(_parsed);
    return _data_page_decoder->next_batch(range, dst);
}

template class DictPageDecoder<TYPE_SMALLINT>;
template class DictPageDecoder<TYPE_INT>;
template class DictPageDecoder<TYPE_BIGINT>;
template class DictPageDecoder<TYPE_LARGEINT>;
template class DictPageDecoder<TYPE_FLOAT>;
template class DictPageDecoder<TYPE_DOUBLE>;
template class DictPageDecoder<TYPE_DATE_V1>;
template class DictPageDecoder<TYPE_DATE>;
template class DictPageDecoder<TYPE_DATETIME_V1>;
template class DictPageDecoder<TYPE_DATETIME>;
template class DictPageDecoder<TYPE_DECIMALV2>;

template class DictPageBuilder<TYPE_SMALLINT>;
template class DictPageBuilder<TYPE_INT>;
template class DictPageBuilder<TYPE_BIGINT>;
template class DictPageBuilder<TYPE_LARGEINT>;
template class DictPageBuilder<TYPE_FLOAT>;
template class DictPageBuilder<TYPE_DOUBLE>;
template class DictPageBuilder<TYPE_DATE_V1>;
template class DictPageBuilder<TYPE_DATE>;
template class DictPageBuilder<TYPE_DATETIME_V1>;
template class DictPageBuilder<TYPE_DATETIME>;
template class DictPageBuilder<TYPE_DECIMALV2>;

} // namespace starrocks
