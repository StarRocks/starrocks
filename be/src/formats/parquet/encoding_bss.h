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

#include "column/column.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/types.h"
#include "util/byte_stream_split.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace starrocks::parquet {

template <tparquet::Type::type PT>
class ByteStreamSplitEncoder : public Encoder {
public:
    using T = typename PhysicalTypeTraits<PT>::CppType;
    ByteStreamSplitEncoder() {
        if (!is_flba()) {
            byte_width_ = sizeof(T);
        }
    }
    ~ByteStreamSplitEncoder() override = default;

    void set_type_length(int byte_width) override {
        if (is_flba()) {
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
        return Slice(sink_.data(), sink_.size());
    }

private:
    bool sink_sealed_ = false;
    faststring sink_;
    // Required because type_length_ is only filled in for FLBA
    int byte_width_ = 0;
    int64_t num_values_in_buffer_;
    faststring shuffle_buffer_;

private:
    bool constexpr is_flba() const { return PT == tparquet::Type::FIXED_LEN_BYTE_ARRAY; }
    void FlushValues() {
        if (sink_sealed_) {
            return;
        }
        sink_sealed_ = true;
        if (byte_width_ <= 1) {
            return;
        }
        shuffle_buffer_.reserve(sink_.size());
        uint8_t* shuffle_buffer_raw = shuffle_buffer_.data();
        const uint8_t* raw_values = sink_.data();
        ByteStreamSplitUtil::ByteStreamSplitEncode(raw_values, byte_width_, num_values_in_buffer_, shuffle_buffer_raw);
    }

    void Put(const T* buffer, int num_values) {
        if (num_values == 0) {
            return;
        }
        if constexpr (!is_flba()) {
            sink_.append(reinterpret_cast<const uint8_t*>(buffer), num_values * static_cast<int64_t>(sizeof(T)));
        } else {
            Slice* slices = reinterpret_cast<Slice*>(buffer);
            for (int i = 0; i < num_values; ++i) {
                const Slice& slice = slices[i];
                DCHECK_EQ(byte_width_, slice.size);
                sink_.append(slice.data, slice.size);
            }
        }
        this->num_values_in_buffer_ += num_values;
    }
};

} // namespace starrocks::parquet