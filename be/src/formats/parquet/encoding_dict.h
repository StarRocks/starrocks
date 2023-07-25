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

#pragma once

#include <map>
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "simd/simd.h"
#include "util/coding.h"
#include "util/rle_encoding.h"
#include "util/slice.h"

namespace starrocks::parquet {

struct SliceHasher {
    uint32_t operator()(const Slice& s) const { return HashUtil::hash(s.data, s.size, 397); }
};

template <typename T>
class DictEncoder final : public Encoder {
public:
    DictEncoder() = default;
    ~DictEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        const T* ptr = (const T*)vals;
        for (int i = 0; i < count; ++i) {
            int index = 0;
            auto it = _dict.find(*ptr);
            if (it == _dict.end()) {
                _dict[*ptr] = _num_dict;
                _dict_vector.push_back(*ptr);
                index = _num_dict++;
            } else {
                index = it->second;
            }
            _indexes.push_back(index);
            ptr++;
        }
        return Status::OK();
    }

    Slice build() override {
        // TODO(zc): modify RLE encoder to avoid another copy
        uint8_t bit_width = BitUtil::log2(_num_dict);
        faststring encoded_data;
        RleEncoder<int32_t> rle_encoder(&encoded_data, bit_width);
        for (auto index : _indexes) {
            rle_encoder.Put(index);
        }
        int length = rle_encoder.Flush();
        _buffer.append(&bit_width, 1);
        _buffer.append(encoded_data.data(), length);
        return {_buffer.data(), _buffer.size()};
    }

    Status encode_dict(Encoder* dict_encoder, size_t* num_dicts) override {
        RETURN_IF_ERROR(dict_encoder->append((const uint8_t*)&_dict_vector[0], _num_dict));
        *num_dicts = _num_dict;
        return Status::OK();
    }

private:
    size_t _num_dict{};
    std::map<T, int> _dict;
    std::vector<T> _dict_vector;
    std::vector<int> _indexes;
    faststring _buffer;
};

// TODO(zc): support read run later. however should add more interface to Column first
template <typename T>
class DictDecoder final : public Decoder {
public:
    DictDecoder() = default;
    ~DictDecoder() override = default;

    // initialize dictionary
    Status set_dict(int chunk_size, size_t num_values, Decoder* decoder) override {
        _dict.resize(num_values);
        _indexes.resize(chunk_size);
        RETURN_IF_ERROR(decoder->next_batch(num_values, (uint8_t*)&_dict[0]));
        return Status::OK();
    }

    Status set_data(const Slice& data) override {
        if (data.size > 0) {
            uint8_t bit_width = *data.data;
            _rle_batch_reader = RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data.data) + 1,
                                                          static_cast<int>(data.size) - 1, bit_width);
        } else {
            return Status::Corruption("input encoded data size is 0");
        }
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        //TODO(Smith) still heavy work load
        _indexes.reserve(values_to_skip);
        _rle_batch_reader.GetBatch(&_indexes[0], values_to_skip);
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst) override {
        FixedLengthColumn<T>* data_column /* = nullptr */;
        if (dst->is_nullable()) {
            auto nullable_column = down_cast<NullableColumn*>(dst);
            nullable_column->null_column()->append_default(count);
            data_column = down_cast<FixedLengthColumn<T>*>(nullable_column->data_column().get());
        } else {
            data_column = down_cast<FixedLengthColumn<T>*>(dst);
        }

        size_t cur_size = data_column->size();
        data_column->resize_uninitialized(cur_size + count);
        T* __restrict__ data = data_column->get_data().data() + cur_size;

        _rle_batch_reader.GetBatchWithDict(_dict.data(), _dict.size(), data, count);

        return Status::OK();
    }

private:
    enum { SIZE_OF_TYPE = sizeof(T) };

    RleBatchDecoder<uint32_t> _rle_batch_reader;
    std::vector<T> _dict;
    std::vector<uint32_t> _indexes;
};

template <>
class DictDecoder<Slice> final : public Decoder {
public:
    DictDecoder() = default;
    ~DictDecoder() override = default;

    Status set_dict(int chunk_size, size_t num_values, Decoder* decoder) override {
        _indexes.resize(chunk_size);
        _slices.resize(chunk_size);
        std::vector<Slice> slices(num_values);
        RETURN_IF_ERROR(decoder->next_batch(num_values, (uint8_t*)&slices[0]));

        size_t total_length = 0;
        for (int i = 0; i < num_values; ++i) {
            total_length += slices[i].size;
        }

        _dict.resize(num_values);
        _dict_code_by_value.reserve(num_values);

        // reserve enough memory to use append_strings_overflow
        _dict_data.resize(total_length + Column::APPEND_OVERFLOW_MAX_SIZE);
        size_t offset = 0;
        _max_value_length = 0;
        for (int i = 0; i < num_values; ++i) {
            memcpy(&_dict_data[offset], slices[i].data, slices[i].size);
            _dict[i].data = reinterpret_cast<char*>(&_dict_data[offset]);
            _dict[i].size = slices[i].size;
            offset += slices[i].size;
            _dict_code_by_value[_dict[i]] = i;

            if (slices[i].size > _max_value_length) {
                _max_value_length = slices[i].size;
            }
        }

        return Status::OK();
    }

    Status get_dict_values(Column* column) override {
        auto ret = column->append_strings_overflow(_dict, _max_value_length);
        if (UNLIKELY(!ret)) {
            return Status::InternalError("DictDecoder append strings to column failed");
        }
        return Status::OK();
    }

    Status get_dict_values(const std::vector<int32_t>& dict_codes, const NullableColumn& nulls,
                           Column* column) override {
        const std::vector<uint8_t>& null_data = nulls.immutable_null_column_data();
        bool has_null = nulls.has_null();
        bool all_null = false;

        if (has_null) {
            size_t count = SIMD::count_nonzero(null_data);
            all_null = (count == null_data.size());
        }

        // dict codes size and column size HAVE TO BE EXACTLY SAME.
        std::vector<Slice> slices(dict_codes.size());
        if (!has_null) {
            for (size_t i = 0; i < dict_codes.size(); i++) {
                slices[i] = _dict[dict_codes[i]];
            }
        } else if (!all_null) {
            size_t size = dict_codes.size();
            const uint8_t* null_data_ptr = null_data.data();
            for (size_t i = 0; i < size; i++) {
                // if null, we assign dict code 0(there should be at least one value?)
                // null = 0, mask = 0xffffffff
                // null = 1, mask = 0x00000000
                uint32_t mask = ~(static_cast<uint32_t>(-null_data_ptr[i]));
                int32_t code = mask & dict_codes[i];
                slices[i] = _dict[code];
            }
        }

        // if all null, then slices[i] is Slice(), and we can not call `append_strings_overflow`
        // and for other cases, slices[i] is dict value, then we can call `append_strings_overflow`
        bool ret = false;
        if (!all_null) {
            ret = column->append_strings_overflow(slices, _max_value_length);
        } else {
            ret = column->append_strings(slices);
        }

        if (UNLIKELY(!ret)) {
            return Status::InternalError("DictDecoder append strings to column failed");
        }
        return Status::OK();
    }

    Status get_dict_codes(const std::vector<Slice>& dict_values, const NullableColumn& nulls,
                          std::vector<int32_t>* dict_codes) override {
        const std::vector<uint8_t>& null_data = nulls.immutable_null_column_data();
        bool has_null = nulls.has_null();
        bool all_null = false;

        // dict values size and dict codes size don't need to be matched.
        // if nulls[i], then there is no need to get code of dict_values[i]

        if (has_null) {
            size_t count = SIMD::count_nonzero(null_data);
            all_null = (count == null_data.size());
        }
        if (all_null) {
            return Status::OK();
        }

        if (!has_null) {
            dict_codes->reserve(dict_values.size());
            for (size_t i = 0; i < dict_values.size(); i++) {
                dict_codes->emplace_back(_dict_code_by_value[dict_values[i]]);
            }
        } else {
            for (size_t i = 0; i < dict_values.size(); i++) {
                if (!null_data[i]) {
                    dict_codes->emplace_back(_dict_code_by_value[dict_values[i]]);
                }
            }
        }
        return Status::OK();
    }

    Status set_data(const Slice& data) override {
        if (data.size > 0) {
            uint8_t bit_width = *data.data;
            _rle_batch_reader = RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data.data) + 1,
                                                          static_cast<int>(data.size) - 1, bit_width);
        } else {
            return Status::Corruption("input encoded data size is 0");
        }
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        //TODO(Smith) still heavy work load
        _indexes.reserve(values_to_skip);
        _rle_batch_reader.GetBatch(&_indexes[0], values_to_skip);
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst) override {
        switch (content_type) {
        case DICT_CODE: {
            FixedLengthColumn<int32_t>* data_column;
            if (dst->is_nullable()) {
                auto nullable_column = down_cast<NullableColumn*>(dst);
                nullable_column->null_column()->append_default(count);
                data_column = down_cast<FixedLengthColumn<int32_t>*>(nullable_column->data_column().get());
            } else {
                data_column = down_cast<FixedLengthColumn<int32_t>*>(dst);
            }
            size_t cur_size = data_column->size();
            data_column->resize_uninitialized(cur_size + count);
            int32_t* __restrict__ data = data_column->get_data().data() + cur_size;
            _rle_batch_reader.GetBatch(reinterpret_cast<uint32_t*>(data), count);
            break;
        }
        case VALUE: {
            raw::stl_vector_resize_uninitialized(&_slices, count);
            auto ret = _rle_batch_reader.GetBatchWithDict(_dict.data(), _dict.size(), _slices.data(), count);
            if (UNLIKELY(ret == -1)) {
                return Status::InternalError("DictDecoder append strings to column failed");
            }
            ret = dst->append_strings_overflow(_slices, _max_value_length);
            if (UNLIKELY(!ret)) {
                return Status::InternalError("DictDecoder append strings to column failed");
            }
            break;
        }
        default:
            return Status::NotSupported("read type not supported");
        }

        return Status::OK();
    }

private:
    enum { SIZE_OF_DICT_CODE_TYPE = sizeof(int32_t) };
    std::unordered_map<Slice, int32_t, SliceHasher> _dict_code_by_value;

    RleBatchDecoder<uint32_t> _rle_batch_reader;
    std::vector<uint8_t> _dict_data;
    std::vector<Slice> _dict;
    std::vector<uint32_t> _indexes;
    std::vector<Slice> _slices;

    size_t _max_value_length = 0;
};

} // namespace starrocks::parquet
