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
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "simd/simd.h"
#include "util/coding.h"
#include "util/cpu_info.h"
#include "util/rle_encoding.h"
#include "util/slice.h"

namespace starrocks::parquet {

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

class CacheAwareDictDecoder : public Decoder {
public:
    CacheAwareDictDecoder() { _dict_size_threshold = CpuInfo::get_l2_cache_size(); }
    ~CacheAwareDictDecoder() override = default;

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
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
            auto decoded_num = _rle_batch_reader.GetBatch(reinterpret_cast<uint32_t*>(data), count);
            if (decoded_num < count) {
                return Status::InternalError("didn't get enough data from dict-decoder");
            }
            break;
        }
        case VALUE: {
#ifdef BE_TEST
            return _next_batch_value(count, dst, filter);
#else
            if (_get_dict_size() > _dict_size_threshold && config::parquet_cache_aware_dict_decoder_enable) {
                return _next_batch_value(count, dst, filter);
            } else {
                return _next_batch_value(count, dst, nullptr);
            }
#endif // BE_TEST
        }
        default:
            return Status::NotSupported("read type not supported");
        }
        return Status::OK();
    }

    Status next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type, Column* dst,
                                 const FilterData* filter) override {
        if (_get_dict_size() > _dict_size_threshold && config::parquet_cache_aware_dict_decoder_enable) {
            return _do_next_batch_with_nulls(count, null_infos, content_type, dst, filter);
        } else {
            return _do_next_batch_with_nulls(count, null_infos, content_type, dst, nullptr);
        }
        return Status::OK();
    }

protected:
    void _next_null_column(size_t count, const NullInfos& null_infos, NullableColumn* dst) {
        size_t null_cnt = null_infos.num_nulls;
        NullColumn* null_column = down_cast<NullableColumn*>(dst)->mutable_null_column();
        const uint8_t* __restrict is_nulls = null_infos.nulls_data();
        auto& null_data = null_column->get_data();
        size_t prev_num_rows = null_data.size();
        raw::stl_vector_resize_uninitialized(&null_data, count + prev_num_rows);
        uint8_t* __restrict__ dst_nulls = null_data.data() + prev_num_rows;
        memcpy(dst_nulls, is_nulls, count);
        down_cast<NullableColumn*>(dst)->set_has_null(null_cnt > 0);
    }

    virtual size_t _get_dict_size() const = 0;
    virtual Status _next_batch_value(size_t count, Column* dst, const FilterData* filter) = 0;
    virtual Status _do_next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type,
                                             Column* dst, const FilterData* filter) = 0;
    RleBatchDecoder<uint32_t> _rle_batch_reader;

private:
    size_t _dict_size_threshold = 0;
};

// TODO(zc): support read run later. however should add more interface to Column first
template <typename T>
class DictDecoder final : public CacheAwareDictDecoder {
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
            // PARQUET-2115: [C++] Parquet dictionary bit widths are limited to 32 bits
            // https://github.com/apache/arrow/pull/12274/files
            if (PREDICT_FALSE(bit_width > 32)) {
                return Status::Corruption("bit width is larger than 32");
            }
            _rle_batch_reader = RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data.data) + 1,
                                                          static_cast<int>(data.size) - 1, bit_width);
        } else {
            return Status::Corruption("input encoded data size is 0");
        }
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        auto ret = _rle_batch_reader.SkipBatch(values_to_skip);
        if (UNLIKELY(ret != values_to_skip)) {
            return Status::InternalError("rle skip error, not enough values");
        }
        return Status::OK();
    }

    Status _do_next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type,
                                     Column* dst, const FilterData* filter) override {
        CHECK(dst->is_nullable());
        if (null_infos.num_ranges <= 2) {
            return Decoder::next_batch_with_nulls(count, null_infos, content_type, dst, filter);
        }
        size_t cur_size = dst->size();
        _next_null_column(count, null_infos, down_cast<NullableColumn*>(dst));

        switch (content_type) {
        case DICT_CODE: {
            return next_dict_code_batch_with_nulls(count, cur_size, null_infos, dst);
        }
        case VALUE: {
            return next_value_batch_with_nulls(count, cur_size, null_infos, dst, filter);
        }
        default:
            return Status::NotSupported("read type not supported");
        }
        return Status::OK();
    }

    template <class DataType>
    void assign_data_with_nulls(size_t count, size_t num_non_nulls, const uint8_t* nulls, const DataType* src_data,
                                DataType* dst_data) {
        // opt branch for process sparse column
        if (num_non_nulls < count / 10) {
            size_t cnt = 0;
            size_t i = 0;
#ifdef __AVX2__
            for (i = 0; i + 32 <= count; i += 32) {
                // Load the next 32 elements of is_nulls into a mask
                __m256i loaded = _mm256_loadu_si256((__m256i*)&nulls[i]);
                int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(loaded, _mm256_setzero_si256()));
                phmap::priv::BitMask<uint32_t, 32> bitmask(mask);
                for (auto idx : bitmask) {
                    dst_data[i + idx] = src_data[cnt++];
                }
            }
#endif
            // process tail elements
            for (; i < count; ++i) {
                dst_data[i] = src_data[cnt];
                cnt += !nulls[i];
            }
            CHECK_EQ(cnt, num_non_nulls) << "count:" << count << " null_cnt:" << count - num_non_nulls;
        } else {
            size_t cnt = 0;
            for (size_t i = 0; i < count; ++i) {
                dst_data[i] = src_data[cnt];
                cnt += !nulls[i];
            }
            CHECK_EQ(cnt, num_non_nulls) << "count:" << count << " null_cnt:" << count - num_non_nulls;
        }
    }

    Status next_dict_code_batch_with_nulls(size_t count, size_t cur_size, const NullInfos& null_infos, Column* dst) {
        size_t null_cnt = null_infos.num_nulls;
        auto nullable_column = down_cast<NullableColumn*>(dst);

        size_t read_count = count - null_cnt;
        Int32Column* data_column = down_cast<Int32Column*>(nullable_column->data_column().get());
        // resize data
        data_column->resize_uninitialized(cur_size + count);
        int32_t* __restrict__ data = data_column->get_data().data() + cur_size;

        uint32_t read_dict_data[read_count + 1];
        if (read_count == 0) {
            return Status::OK();
        }
        auto decoded_num = _rle_batch_reader.GetBatch(read_dict_data, read_count);
        if (decoded_num < read_count) {
            return Status::InternalError("didn't get enough data from dict-decoder");
        }

        assign_data_with_nulls(count, read_count, null_infos.nulls_data(), (int32_t*)read_dict_data, data);

        return Status::OK();
    }

    Status next_value_batch_with_nulls(size_t count, size_t cur_size, const NullInfos& null_infos, Column* dst,
                                       const FilterData* filter) {
        CHECK(dst->is_nullable());
        const uint8_t* __restrict is_nulls = null_infos.nulls_data();
        // assign null infos
        size_t null_cnt = null_infos.num_nulls;
        auto nullable_column = down_cast<NullableColumn*>(dst);
        FixedLengthColumn<T>* data_column = down_cast<FixedLengthColumn<T>*>(nullable_column->data_column().get());
        // resize data
        data_column->resize_uninitialized(cur_size + count);
        T* __restrict__ data = data_column->get_data().data() + cur_size;

        size_t read_count = count - null_cnt;

        if (read_count == 0) {
            return Status::OK();
        }

        if (filter) {
            _indexes.reserve(read_count);
            auto decoded_num = _rle_batch_reader.GetBatch(&_indexes[0], read_count);
            if (decoded_num < read_count) {
                return Status::InternalError("didn't get enough data from dict-decoder");
            }

            auto flag = 0;
            size_t size = _dict.size();
            for (int i = 0; i < read_count; i++) {
                flag |= _indexes[i] >= size;
            }
            if (UNLIKELY(flag)) {
                return Status::InternalError("Index not in dictionary bounds");
            }

            size_t cnt = 0;
            for (int i = 0; i < count; i++) {
                if (filter[i] & !is_nulls[i]) {
                    data[i] = _dict[_indexes[cnt]];
                }
                cnt += !is_nulls[i];
            }
        } else {
            T read_data[read_count + 1];
            auto ret = _rle_batch_reader.GetBatchWithDict(_dict.data(), _dict.size(), read_data, read_count);
            if (UNLIKELY(ret <= 0)) {
                return Status::InternalError("DictDecoder GetBatchWithDict failed");
            }

            assign_data_with_nulls(count, read_count, null_infos.nulls_data(), read_data, data);
        }

        return Status::OK();
    }

private:
    size_t _get_dict_size() const override { return _dict.size() * SIZE_OF_TYPE; }

    Status _next_batch_value(size_t count, Column* dst, const FilterData* filter) override {
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

        if (filter) {
            _indexes.reserve(count);
            auto decoded_num = _rle_batch_reader.GetBatch(&_indexes[0], count);
            if (decoded_num < count) {
                return Status::InternalError("didn't get enough data from dict-decoder");
            }

            auto flag = 0;
            size_t size = _dict.size();
            for (int i = 0; i < count; i++) {
                flag |= _indexes[i] >= size;
            }
            if (UNLIKELY(flag)) {
                return Status::InternalError("Index not in dictionary bounds");
            }

            for (int i = 0; i < count; i++) {
                if (filter[i]) {
                    data[i] = _dict[_indexes[i]];
                }
            }
        } else {
            auto ret = _rle_batch_reader.GetBatchWithDict(_dict.data(), _dict.size(), data, count);
            if (UNLIKELY(ret <= 0)) {
                return Status::InternalError("DictDecoder<> GetBatchWithDict failed");
            }
        }

        return Status::OK();
    }

    enum { SIZE_OF_TYPE = sizeof(T) };

    std::vector<T> _dict;
    std::vector<uint32_t> _indexes;
};

template <>
class DictDecoder<Slice> final : public CacheAwareDictDecoder {
public:
    DictDecoder() = default;
    ~DictDecoder() override = default;

    Status set_dict(int chunk_size, size_t num_values, Decoder* decoder) override {
        auto slices_data = std::make_unique_for_overwrite<uint8_t[]>(num_values * sizeof(Slice));
        Slice* slices = reinterpret_cast<Slice*>(slices_data.get());
        RETURN_IF_ERROR(decoder->next_batch(num_values, (uint8_t*)slices));

        size_t total_length = 0;
        for (int i = 0; i < num_values; ++i) {
            total_length += slices[i].size;
        }

        _dict.resize(num_values);

        // reserve enough memory to use append_strings_overflow
        raw::stl_vector_resize_uninitialized(&_dict_data, total_length + Column::APPEND_OVERFLOW_MAX_SIZE);

        size_t offset = 0;
        _max_value_length = 0;
        for (int i = 0; i < num_values; ++i) {
            memcpy(&_dict_data[offset], slices[i].data, slices[i].size);
            _dict[i].data = reinterpret_cast<char*>(&_dict_data[offset]);
            _dict[i].size = slices[i].size;
            offset += slices[i].size;

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

    Status get_dict_values(const Buffer<int32_t>& dict_codes, const NullableColumn& nulls, Column* column) override {
        const NullData& null_data = nulls.immutable_null_column_data();
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

    Status set_data(const Slice& data) override {
        if (data.size > 0) {
            uint8_t bit_width = *data.data;
            // PARQUET-2115: [C++] Parquet dictionary bit widths are limited to 32 bits
            // https://github.com/apache/arrow/pull/12274/files
            if (PREDICT_FALSE(bit_width > 32)) {
                return Status::Corruption("bit width is larger than 32");
            }
            _rle_batch_reader = RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data.data) + 1,
                                                          static_cast<int>(data.size) - 1, bit_width);
        } else {
            return Status::Corruption("input encoded data size is 0");
        }
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        auto ret = _rle_batch_reader.SkipBatch(values_to_skip);
        if (UNLIKELY(ret != values_to_skip)) {
            return Status::InternalError("rle skip error, not enough values");
        }
        return Status::OK();
    }

private:
    size_t _get_dict_size() const override { return _dict_data.size(); }

    Status _do_next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type,
                                     Column* dst, const FilterData* filter) override {
        return Decoder::next_batch_with_nulls(count, null_infos, content_type, dst, filter);
    }
    Status _next_batch_value(size_t count, Column* dst, const FilterData* filter) override {
        if (filter) {
            _indexes.reserve(count);
            auto decoded_num = _rle_batch_reader.GetBatch(&_indexes[0], count);
            if (decoded_num < count) {
                return Status::InternalError("didn't get enough data from dict-decoder");
            }
            auto flag = 0;
            size_t size = _dict.size();
            for (int i = 0; i < count; i++) {
                flag |= _indexes[i] >= size;
            }
            if (UNLIKELY(flag)) {
                return Status::InternalError("Index not in dictionary bounds");
            }
            if (dst->is_nullable()) {
                down_cast<NullableColumn*>(dst)->mutable_null_column()->append_default(count);
            }
            auto* binary_column = ColumnHelper::get_binary_column(dst);
            for (int i = 0; i < count; ++i) {
                if (filter[i]) {
                    binary_column->append(_dict[_indexes[i]]);
                } else {
                    binary_column->append_default();
                }
            }
        } else {
            raw::stl_vector_resize_uninitialized(&_slices, count);
            auto ret = _rle_batch_reader.GetBatchWithDict(_dict.data(), _dict.size(), _slices.data(), count);
            if (UNLIKELY(ret <= 0)) {
                return Status::InternalError("DictDecoder<Slice> GetBatchWithDict failed");
            }
            ret = dst->append_strings_overflow(_slices, _max_value_length);
            if (UNLIKELY(!ret)) {
                return Status::InternalError("DictDecoder append strings to column failed");
            }
        }
        return Status::OK();
    }

    enum { SIZE_OF_DICT_CODE_TYPE = sizeof(int32_t) };

    std::vector<uint8_t> _dict_data;
    std::vector<Slice> _dict;
    std::vector<uint32_t> _indexes;
    std::vector<Slice> _slices;
    size_t _max_value_length = 0;
};

} // namespace starrocks::parquet
