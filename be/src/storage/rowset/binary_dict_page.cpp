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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/binary_dict_page.cpp

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

#include "storage/rowset/binary_dict_page.h"

#include <memory>

#include "column/binary_column.h"
#include "column/nullable_column.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h" // for Substitute
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/range.h"
#include "storage/rowset/bitshuffle_page.h"
#include "types/logical_type.h"
#include "util/slice.h" // for Slice
#include "util/unaligned_access.h"

namespace starrocks {

using strings::Substitute;

BinaryDictPageBuilder::BinaryDictPageBuilder(const PageBuilderOptions& options)
        : _options(options),
          _finished(false),
          _data_page_builder(nullptr),
          _dict_builder(nullptr),
          _encoding_type(DICT_ENCODING) {
    // initially use DICT_ENCODING
    _data_page_builder = std::make_unique<BitshufflePageBuilder<TYPE_INT>>(options);
    _data_page_builder->reserve_head(BINARY_DICT_PAGE_HEADER_SIZE);
    PageBuilderOptions dict_builder_options;
    dict_builder_options.data_page_size = _options.dict_page_size;
    _dict_builder = std::make_unique<BinaryPlainPageBuilder>(dict_builder_options);
    reset();
}

bool BinaryDictPageBuilder::is_page_full() {
    if (_data_page_builder->is_page_full()) {
        return true;
    }
    return _encoding_type == DICT_ENCODING && _dict_builder->is_page_full();
}

uint32_t BinaryDictPageBuilder::add(const uint8_t* vals, uint32_t count) {
    if (_encoding_type == DICT_ENCODING) {
        DCHECK(!_finished);
        DCHECK_GT(count, 0);
        const auto* src = reinterpret_cast<const Slice*>(vals);
        uint32_t value_code = -1;
        // Manually devirtualization.
        auto* code_page = down_cast<BitshufflePageBuilder<TYPE_INT>*>(_data_page_builder.get());

        if (_data_page_builder->count() == 0) {
            auto s = unaligned_load<Slice>(src);
            _first_value.assign_copy(reinterpret_cast<const uint8_t*>(s.get_data()), s.get_size());
        }

        for (int i = 0; i < count; ++i, ++src) {
            auto s = unaligned_load<Slice>(src);
            auto iter = _dictionary.find(s);
            if (iter != _dictionary.end()) {
                value_code = iter->second;
            } else if (_dict_builder->add_slice(s)) {
                value_code = _dictionary.size();
                _dictionary.insert_or_assign(std::string(s.data, s.size), value_code);
            } else {
                return i;
            }
            if (code_page->add_one(reinterpret_cast<const uint8_t*>(&value_code)) < 1) {
                return i;
            }
        }
        return count;
    } else {
        DCHECK_EQ(_encoding_type, PLAIN_ENCODING);
        return _data_page_builder->add(vals, count);
    }
}

faststring* BinaryDictPageBuilder::finish() {
    DCHECK(!_finished);
    _finished = true;

    faststring* data_slice = _data_page_builder->finish();
    encode_fixed32_le(data_slice->data(), _encoding_type);
    return data_slice;
}

void BinaryDictPageBuilder::reset() {
    _finished = false;
    if (_encoding_type == DICT_ENCODING && _dict_builder->is_page_full()) {
        _data_page_builder = std::make_unique<BinaryPlainPageBuilder>(_options);
        _data_page_builder->reserve_head(BINARY_DICT_PAGE_HEADER_SIZE);
        _encoding_type = PLAIN_ENCODING;
    } else {
        _data_page_builder->reset();
    }
    _finished = false;
}

uint32_t BinaryDictPageBuilder::count() const {
    return _data_page_builder->count();
}

uint64_t BinaryDictPageBuilder::size() const {
    return _data_page_builder->size();
}

faststring* BinaryDictPageBuilder::get_dictionary_page() {
    return _dict_builder->finish();
}

Status BinaryDictPageBuilder::get_first_value(void* value) const {
    DCHECK(_finished);
    if (_data_page_builder->count() == 0) {
        return Status::NotFound("page is empty");
    }
    if (_encoding_type != DICT_ENCODING) {
        return _data_page_builder->get_first_value(value);
    }
    *reinterpret_cast<Slice*>(value) = Slice(_first_value);
    return Status::OK();
}

Status BinaryDictPageBuilder::get_last_value(void* value) const {
    DCHECK(_finished);
    if (_data_page_builder->count() == 0) {
        return Status::NotFound("page is empty");
    }
    if (_encoding_type != DICT_ENCODING) {
        return _data_page_builder->get_last_value(value);
    }
    uint32_t value_code;
    RETURN_IF_ERROR(_data_page_builder->get_last_value(&value_code));
    *reinterpret_cast<Slice*>(value) = _dict_builder->get_value(value_code);
    return Status::OK();
}

bool BinaryDictPageBuilder::is_valid_global_dict(const GlobalDictMap* global_dict) const {
    for (const auto& it : _dictionary) {
        if (auto iter = global_dict->find(it.first); iter == global_dict->end()) {
            return false;
        }
    }
    return true;
}

template <LogicalType Type>
BinaryDictPageDecoder<Type>::BinaryDictPageDecoder(Slice data)
        : _data(data), _data_page_decoder(nullptr), _parsed(false), _encoding_type(UNKNOWN_ENCODING) {}

template <LogicalType Type>
Status BinaryDictPageDecoder<Type>::init() {
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
        _data_page_decoder = std::make_unique<BitShufflePageDecoder<TYPE_INT>>(_data);
    } else if (_encoding_type == PLAIN_ENCODING) {
        DCHECK_EQ(_encoding_type, PLAIN_ENCODING);
        _data_page_decoder.reset(new BinaryPlainPageDecoder<Type>(_data));
    } else {
        LOG(WARNING) << "invalid encoding type:" << _encoding_type;
        return Status::Corruption(strings::Substitute("invalid encoding type:$0", _encoding_type));
    }

    RETURN_IF_ERROR(_data_page_decoder->init());
    _parsed = true;
    return Status::OK();
}

template <LogicalType Type>
Status BinaryDictPageDecoder<Type>::seek_to_position_in_page(uint32_t pos) {
    return _data_page_decoder->seek_to_position_in_page(pos);
}

template <LogicalType Type>
void BinaryDictPageDecoder<Type>::set_dict_decoder(PageDecoder* dict_decoder) {
    _dict_decoder = down_cast<BinaryPlainPageDecoder<Type>*>(dict_decoder);
    _max_value_length = _dict_decoder->max_value_length();
}

template <LogicalType Type>
Status BinaryDictPageDecoder<Type>::next_batch(size_t* n, Column* dst) {
    SparseRange<> read_range;
    uint32_t begin = current_index();
    read_range.add(Range<>(begin, begin + *n));
    RETURN_IF_ERROR(next_batch(read_range, dst));
    *n = current_index() - begin;
    return Status::OK();
}

template <LogicalType Type>
Status BinaryDictPageDecoder<Type>::next_batch(const SparseRange<>& range, Column* dst) {
    if (_encoding_type == PLAIN_ENCODING) {
        return _data_page_decoder->next_batch(range, dst);
    }

    DCHECK(_parsed);
    DCHECK(_dict_decoder != nullptr) << "dict decoder pointer is nullptr";
    if (_vec_code_buf == nullptr) {
        _vec_code_buf = ChunkHelper::column_from_field_type(TYPE_INT, false);
    }
    _vec_code_buf->resize(0);
    _vec_code_buf->reserve(range.span_size());

    RETURN_IF_ERROR(_data_page_decoder->next_batch(range, _vec_code_buf.get()));
    size_t nread = _vec_code_buf->size();
    using cast_type = CppTypeTraits<TYPE_INT>::CppType;
    const auto* codewords = reinterpret_cast<const cast_type*>(_vec_code_buf->raw_data());

    static_assert(sizeof(Slice) == sizeof(int128_t));
    auto slices_data = std::make_unique_for_overwrite<uint8_t[]>(nread * sizeof(Slice));
    Slice* slices = reinterpret_cast<Slice*>(slices_data.get());

    if constexpr (Type == TYPE_CHAR) {
        for (int i = 0; i < nread; ++i) {
            Slice element = _dict_decoder->string_at_index(codewords[i]);
            // Strip trailing '\x00'
            element.size = strnlen(element.data, element.size);
            slices[i] = element;
        }
    } else {
        _dict_decoder->batch_string_at_index(slices, codewords, nread);
    }

    class SliceContainerAdaptor {
    public:
        using value_type = Slice;
        SliceContainerAdaptor(Slice* slices, size_t size) : _slices(slices), _size(size) {}

        Slice* data() const { return _slices; }
        size_t size() const { return _size; }

    private:
        Slice* _slices;
        size_t _size;
    };

    SliceContainerAdaptor adaptor(slices, nread);
    bool ok = dst->append_strings_overflow(adaptor, _max_value_length);
    DCHECK(ok) << "append_strings_overflow failed";
    RETURN_IF(!ok, Status::InternalError("BinaryDictPageDecoder::next_batch failed"));
    return Status::OK();
}

template <LogicalType Type>
Status BinaryDictPageDecoder<Type>::next_batch_with_filter(
        Column* column, const SparseRange<>& range, const std::vector<const ColumnPredicate*>& compound_and_predicates,
        NullColumn* null, uint8_t* selection, uint16_t* selected_idx, bool* data_filtered) {
    if (_encoding_type == PLAIN_ENCODING) {
        return _data_page_decoder->next_batch_with_filter(column, range, compound_and_predicates, null, selection,
                                                          selected_idx, data_filtered);
    }

    if constexpr (Type == TYPE_CHAR) {
        *data_filtered = false;
        return next_batch(range, column);
    }

    DCHECK(_parsed);
    DCHECK(_dict_decoder != nullptr) << "dict decoder pointer is nullptr";
    DCHECK(null == nullptr); // don't support nullable column right now

    *data_filtered = true;

    if (PREDICT_FALSE(_data_page_decoder->current_index() >= _data_page_decoder->count())) {
        return Status::OK();
    }

    // Step 1: Create dictionary column and apply predicates to dictionary
    uint32_t dict_size = _dict_decoder->count();
    if (dict_size == 0) {
        return Status::OK();
    }

    // For other types, use true zero-copy construction directly from decoder data
    const void* data_ptr = _dict_decoder->get_raw_data();
    size_t data_length = _dict_decoder->get_data_length();

    BinaryColumn::Offsets temp_offsets;
    _dict_decoder->get_offsets_for_zero_copy(temp_offsets);

    // Create zero-copy BinaryColumn directly from decoder's data
    auto dict_column = BinaryColumn::create(data_ptr, data_length, std::move(temp_offsets));

    // Apply predicates to dictionary
    std::vector<uint8_t> dict_selection(dict_size, 0);
    std::vector<uint16_t> dict_selected_idx(dict_size);
    RETURN_IF_ERROR(compound_and_predicates_evaluate(compound_and_predicates, dict_column.get(), dict_selection.data(),
                                                     dict_selected_idx.data(), 0, dict_size));

    // Step 2: Read dictionary codes for the range (we must do this regardless of dict selection)
    if (_vec_code_buf == nullptr) {
        _vec_code_buf = ChunkHelper::column_from_field_type(TYPE_INT, false);
    }
    _vec_code_buf->resize(0);
    _vec_code_buf->reserve(range.span_size());

    RETURN_IF_ERROR(_data_page_decoder->next_batch(range, _vec_code_buf.get()));
    size_t nread = _vec_code_buf->size();

    if (nread == 0) {
        return Status::OK();
    }

    // Count selected dictionary entries
    uint32_t dict_selected_count = SIMD::count_nonzero(dict_selection.data(), dict_size);
    if (dict_selected_count == 0) {
        // No dictionary entries match, so no rows will match
        memset(selection, 0, nread);
        return Status::OK();
    }

    using cast_type = CppTypeTraits<TYPE_INT>::CppType;
    const auto* codewords = reinterpret_cast<const cast_type*>(_vec_code_buf->raw_data());

    // Step 3: Update selection based on dictionary selection and collect matching slices
    std::vector<Slice> selected_slices;
    selected_slices.reserve(nread);

    for (size_t i = 0; i < nread; ++i) {
        uint32_t code = codewords[i];
        if (code < dict_size && dict_selection[code]) {
            selection[i] = 1;
            Slice element = _dict_decoder->string_at_index(code);
            if constexpr (Type == TYPE_CHAR) {
                // Strip trailing '\x00' for CHAR type
                element.size = strnlen(element.data, element.size);
            }
            selected_slices.emplace_back(element);
        } else {
            selection[i] = 0;
        }
    }

    // Step 4: Append selected strings to column using append_strings_overflow
    if (!selected_slices.empty()) {
        class SliceContainerAdaptor {
        public:
            using value_type = Slice;
            SliceContainerAdaptor(const std::vector<Slice>& slices) : _slices(slices) {}

            const Slice* data() const { return _slices.data(); }
            size_t size() const { return _slices.size(); }

        private:
            const std::vector<Slice>& _slices;
        };

        SliceContainerAdaptor adaptor(selected_slices);
        bool ok = column->append_strings_overflow(adaptor, _max_value_length);
        RETURN_IF(!ok, Status::InternalError("BinaryDictPageDecoder::next_batch_with_filter failed"));
    }

    return Status::OK();
}

template <LogicalType Type>
Status BinaryDictPageDecoder<Type>::read_by_rowids(const ordinal_t first_ordinal_in_page, const rowid_t* rowids,
                                                   size_t* count, Column* column) {
    if (_encoding_type == PLAIN_ENCODING) {
        return _data_page_decoder->read_by_rowids(first_ordinal_in_page, rowids, count, column);
    }
    DCHECK(_parsed);
    DCHECK(_dict_decoder != nullptr) << "dict decoder pointer is nullptr";
    if (PREDICT_FALSE(*count == 0)) {
        return Status::OK();
    }
    if (_vec_code_buf == nullptr) {
        _vec_code_buf = ChunkHelper::column_from_field_type(TYPE_INT, false);
    }
    _vec_code_buf->resize(0);
    _vec_code_buf->reserve(*count);
    size_t read_count = *count;
    RETURN_IF_ERROR(
            _data_page_decoder->read_by_rowids(first_ordinal_in_page, rowids, &read_count, _vec_code_buf.get()));
    DCHECK_EQ(_vec_code_buf->size(), read_count);

    if (PREDICT_FALSE(read_count == 0)) {
        *count = 0;
        return Status::OK();
    }
    using cast_type = CppTypeTraits<TYPE_INT>::CppType;
    const auto* codewords = reinterpret_cast<const cast_type*>(_vec_code_buf->raw_data());
    auto slices_data = std::make_unique_for_overwrite<uint8_t[]>(read_count * sizeof(Slice));
    Slice* slices = reinterpret_cast<Slice*>(slices_data.get());
    if constexpr (Type == TYPE_CHAR) {
        for (size_t i = 0; i < read_count; i++) {
            Slice element = _dict_decoder->string_at_index(codewords[i]);
            element.size = strnlen(element.data, element.size);
            slices[i] = element;
        }
    } else {
        _dict_decoder->batch_string_at_index(slices, codewords, read_count);
    }

    class SliceContainerAdaptor {
    public:
        using value_type = Slice;
        SliceContainerAdaptor(Slice* slices, size_t size) : _slices(slices), _size(size) {}

        Slice* data() const { return _slices; }
        size_t size() const { return _size; }

    private:
        Slice* _slices;
        size_t _size;
    };

    SliceContainerAdaptor adaptor(slices, read_count);
    bool ok = column->append_strings_overflow(adaptor, _max_value_length);
    RETURN_IF(!ok, Status::InternalError("BinaryDictPageDecoder::read_by_rowids failed"));
    *count = read_count;
    return Status::OK();
}

template <LogicalType Type>
Status BinaryDictPageDecoder<Type>::next_dict_codes(size_t* n, Column* dst) {
    DCHECK(_encoding_type == DICT_ENCODING);
    DCHECK(_parsed);
    return _data_page_decoder->next_batch(n, dst);
}

template <LogicalType Type>
Status BinaryDictPageDecoder<Type>::read_dict_codes_by_rowids(const ordinal_t first_ordinal_in_page,
                                                              const rowid_t* rowids, size_t* count, Column* dst) {
    DCHECK(_encoding_type == DICT_ENCODING);
    DCHECK(_parsed);
    return _data_page_decoder->read_by_rowids(first_ordinal_in_page, rowids, count, dst);
}

template <LogicalType Type>
Status BinaryDictPageDecoder<Type>::next_dict_codes(const SparseRange<>& range, Column* dst) {
    DCHECK(_encoding_type == DICT_ENCODING);
    DCHECK(_parsed);
    return _data_page_decoder->next_batch(range, dst);
}

template <LogicalType Type>
void BinaryDictPageDecoder<Type>::reserve_col(size_t n, Column* column) {
    DCHECK(_encoding_type == DICT_ENCODING);
    DCHECK(_parsed);
    size_t estimated_row_size = _dict_decoder->estimate_row_size();
    Column* data_col;
    if (column->is_nullable()) {
        // This is NullableColumn, get its data_column
        auto* nullable_col = down_cast<NullableColumn*>(column);
        data_col = nullable_col->data_column().get();

    } else {
        data_col = column;
    }

    if (data_col->is_binary()) {
        BinaryColumn* binary_col = down_cast<BinaryColumn*>(data_col);
        binary_col->reserve(n, estimated_row_size * n);
    }
}

template class BinaryDictPageDecoder<TYPE_CHAR>;
template class BinaryDictPageDecoder<TYPE_VARCHAR>;
template class BinaryDictPageDecoder<TYPE_JSON>;

} // namespace starrocks
