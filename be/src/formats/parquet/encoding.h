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

#include <functional>
#include <memory>

#include "common/status.h"
#include "gen_cpp/parquet_types.h"
#include "utils.h"

namespace starrocks {

class Slice;
class Column;
class NullableColumn;

} // namespace starrocks

namespace starrocks::parquet {

// NOTE: This class is only used for unit test
class Encoder {
public:
    Encoder() = default;
    virtual ~Encoder() = default;

    virtual Status append(const uint8_t* vals, size_t count) = 0;

    virtual Slice build() = 0;

    virtual Status encode_dict(Encoder* dict_encoder, size_t* num_dicts) {
        return Status::NotSupported("encode_dict is not supported");
    }
};

class Decoder {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    virtual Status set_dict(int chunk_size, size_t num_values, Decoder* decoder) {
        return Status::NotSupported("set_dict is not supported");
    }

    virtual Status get_dict_values(Column* column) { return Status::NotSupported("get_dict_values is not supported"); }

    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, const NullableColumn& nulls,
                                   Column* column) {
        return Status::NotSupported("get_dict_values is not supported");
    }

    // used to set fixed length
    virtual void set_type_legth(int32_t type_length) {}

    // Set a new page to decoded.
    virtual Status set_data(const Slice& data) = 0;

    // For history reason, decoder don't known how many elements encoded in one page.
    // Caller must assure that no out-of-bounds access.
    // It will return ERROR if caller wants to read out-of-bound data.
    virtual Status next_batch(size_t count, ColumnContentType content_type, Column* dst) = 0;

    // Currently, this function is only used to read dictionary values.
    virtual Status next_batch(size_t count, uint8_t* dst) {
        return Status::NotSupported("next_batch is not supported");
    }
};

class EncodingInfo {
public:
    // Get EncodingInfo for TypeInfo and EncodingTypePB
    static Status get(tparquet::Type::type type, tparquet::Encoding::type encoding, const EncodingInfo** info);

    Status create_decoder(std::unique_ptr<Decoder>* decoder) const { return _create_decoder_func(decoder); }

    Status create_encoder(std::unique_ptr<Encoder>* encoder) const { return _create_encoder_func(encoder); }

    tparquet::Type::type type() const { return _type; }
    tparquet::Encoding::type encoding() const { return _encoding; }

private:
    friend class EncodingInfoResolver;

    template <typename TypeEncodingTraits>
    explicit EncodingInfo(TypeEncodingTraits traits);

    using CreateDecoderFunc = std::function<Status(std::unique_ptr<Decoder>*)>;
    CreateDecoderFunc _create_decoder_func;
    using CreateEncoderFunc = std::function<Status(std::unique_ptr<Encoder>*)>;
    CreateEncoderFunc _create_encoder_func;

    tparquet::Type::type _type;
    tparquet::Encoding::type _encoding;
};

} // namespace starrocks::parquet
