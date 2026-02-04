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

#include <cstdint>

#include "base/simd/byte_stream_split.h"
#include "base/string/faststring.h"
#include "base/string/slice.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/types.h"
#include "util/bit_util.h"

namespace starrocks::parquet {

template <tparquet::Type::type PT>
class ByteStreamSplitEncoder : public Encoder {
public:
    using T = typename PhysicalTypeTraits<PT>::CppType;
    static constexpr bool IS_FLBA = (PT == tparquet::Type::FIXED_LEN_BYTE_ARRAY);

    ByteStreamSplitEncoder() {
        if constexpr (!IS_FLBA) {
            byte_width_ = sizeof(T);
        }
    }
    ~ByteStreamSplitEncoder() override = default;

    std::string to_string() const override { return fmt::format("ByteStreamSplitEncoder<{}>", typeid(T).name()); }

    void set_type_length(int byte_width) override {
        if constexpr (IS_FLBA) {
            byte_width_ = byte_width;
        }
    }

    Status append(const uint8_t* vals, size_t count) override {
        sink_sealed_ = false;
        Put(reinterpret_cast<const T*>(vals), count);
        return Status::OK();
    }

    Slice build() override {
        FlushValues();
        if (byte_width_ <= 1) {
            return Slice(sink_.data(), sink_.size());
        } else {
            return Slice(shuffle_buffer_.data(), shuffle_buffer_.size());
        }
    }

private:
    bool sink_sealed_ = false;
    faststring sink_;
    // Required because type_length_ is only filled in for FLBA
    int byte_width_ = 0;
    int64_t num_values_in_buffer_ = 0;
    faststring shuffle_buffer_;

private:
    void FlushValues() {
        if (sink_sealed_) {
            return;
        }
        sink_sealed_ = true;
        if (byte_width_ <= 1) {
            return;
        }
        shuffle_buffer_.resize(sink_.size());
        uint8_t* shuffle_buffer_raw = shuffle_buffer_.data();
        const uint8_t* raw_values = sink_.data();
        ByteStreamSplitUtil::ByteStreamSplitEncode(raw_values, byte_width_, num_values_in_buffer_, shuffle_buffer_raw);
    }

    void Put(const T* buffer, int num_values) {
        if (num_values == 0) {
            return;
        }
        if constexpr (!IS_FLBA) {
            sink_.append(buffer, num_values * static_cast<int64_t>(sizeof(T)));
        } else {
            for (int i = 0; i < num_values; ++i) {
                const Slice& slice = buffer[i];
                DCHECK_EQ(byte_width_, slice.size);
                sink_.append(slice.data, slice.size);
            }
        }
        num_values_in_buffer_ += num_values;
    }
};

template <tparquet::Type::type PT>
class ByteStreamSplitDecoder : public Decoder {
public:
    using T = typename PhysicalTypeTraits<PT>::CppType;
    static constexpr bool IS_FLBA = (PT == tparquet::Type::FIXED_LEN_BYTE_ARRAY);

    ByteStreamSplitDecoder() {
        if constexpr (!IS_FLBA) {
            byte_width_ = sizeof(T);
        }
    }
    ~ByteStreamSplitDecoder() override = default;

    std::string to_string() const override { return fmt::format("ByteStreamSplitDecoder<{}>", typeid(T).name()); }

    void set_type_length(int byte_width) override {
        if constexpr (IS_FLBA) {
            byte_width_ = byte_width;
        }
    }

    Status set_data(const Slice& data) override {
        len_ = data.size;
        data_ = (uint8_t*)data.data;
        if ((len_ % byte_width_) != 0) {
            return Status::Corruption(fmt::format(
                    "ByteStreamSplit data size {} not aligned with type {} and byte_width: {}", len_, PT, byte_width_));
        }
        num_valid_values_ = stride_ = (len_ / byte_width_);
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        if (count > num_valid_values_) {
            return Status::InvalidArgument("not enough values to read");
        }
        if constexpr (IS_FLBA) {
            // decoded result is in decode_buffer_ if we pass nullptr.
            RETURN_IF_ERROR(Decode(nullptr, count));
            if (dst->is_nullable()) {
                down_cast<NullableColumn*>(dst)->null_column_raw_ptr()->append_default(count);
            }
            auto* binary_column = ColumnHelper::get_binary_column(dst);
            const char* string_buffer = (const char*)decode_buffer_.data();
            binary_column->append_continuous_fixed_length_strings(string_buffer, count, byte_width_);
        } else {
            size_t cur_size = dst->size();
            dst->resize(cur_size + count);
            T* data = reinterpret_cast<T*>(dst->mutable_raw_data()) + cur_size;
            RETURN_IF_ERROR(Decode(data, count));
        }
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        if (count > num_valid_values_) {
            return Status::InvalidArgument("not enough values to read");
        }
        T* data = reinterpret_cast<T*>(dst);
        RETURN_IF_ERROR(Decode(data, count));
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        if (values_to_skip > num_valid_values_) {
            return Status::InvalidArgument("not enough values to skip");
        }
        if constexpr (IS_FLBA) {
            RETURN_IF_ERROR(Decode(nullptr, values_to_skip));
        } else {
            skip_buffer_.reserve(values_to_skip);
            RETURN_IF_ERROR(Decode(skip_buffer_.data(), values_to_skip));
        }
        return Status::OK();
    }

private:
    // Required because type_length_ is only filled in for FLBA
    int byte_width_ = 0;
    int stride_ = 0;
    int num_valid_values_ = 0;
    faststring decode_buffer_;
    std::vector<T> skip_buffer_;

    const uint8_t* data_ = nullptr;
    size_t len_ = 0;

private:
    Status Decode(T* buffer, int max_values) {
        max_values = std::min(max_values, num_valid_values_);
        if constexpr (IS_FLBA) {
            int64_t decode_buffer_size = max_values * byte_width_;
            // note(yanz): don't use reveser here because in ASAN mode this region will marked as poisoned.
            decode_buffer_.resize(decode_buffer_size);
            ByteStreamSplitUtil::ByteStreamSplitDecode(data_, byte_width_, max_values, stride_, decode_buffer_.data());
            if (buffer != nullptr) {
                Slice* slices = reinterpret_cast<Slice*>(buffer);
                for (int i = 0; i < max_values; i++) {
                    slices[i].data = (char*)(decode_buffer_.data() + i * byte_width_);
                    slices[i].size = byte_width_;
                }
            }
        } else {
            ByteStreamSplitUtil::ByteStreamSplitDecode(data_, byte_width_, max_values, stride_, (uint8_t*)buffer);
        }
        data_ += max_values;
        num_valid_values_ -= max_values;
        return Status::OK();
    }
};

} // namespace starrocks::parquet