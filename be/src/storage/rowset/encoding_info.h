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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/encoding_info.h

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

#include <cmath>
#include <functional>

#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "storage/types.h"
#include "util/slice.h"

namespace starrocks {

class TypeInfo;

class PageBuilder;
class PageDecoder;
class PageBuilderOptions;

inline bool enable_non_string_column_dict_encoding() {
    double epsilon = 0.0001;
    return std::abs(config::dictionary_encoding_ratio_for_non_string_column - 0) > epsilon;
}

// We dont make TYPE_TINYINT support dict encoding. The reason is that TYPE_TINYINT is only have
// 256 different values, that is too small to make our speculation mechanism work. And according
// test results, when TINY_INT column is encoded using dict, the space usage is not necessarily
// better than bitshuffle.
inline bool numeric_types_support_dict_encoding(LogicalType type) {
    switch (type) {
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_DECIMALV2:
        return true;
    default:
        return false;
    }
}

inline bool supports_dict_encoding(LogicalType type) {
    if (type == TYPE_VARCHAR || type == TYPE_CHAR) {
        return true;
    }
    return numeric_types_support_dict_encoding(type);
}

class EncodingInfo {
public:
    // Get EncodingInfo for TypeInfo and EncodingTypePB
    static Status get(const LogicalType& data_type, EncodingTypePB encoding_type, const EncodingInfo** encoding);

    // optimize_value_search: whether the encoding scheme should optimize for ordered data
    // and support fast value seek operation
    static EncodingTypePB get_default_encoding(const LogicalType& data_type, bool optimize_value_seek);

    Status create_page_builder(const PageBuilderOptions& opts, PageBuilder** builder) const {
        return _create_builder_func(opts, builder);
    }
    Status create_page_decoder(const Slice& data, PageDecoder** decoder) const {
        return _create_decoder_func(data, decoder);
    }
    LogicalType type() const { return _type; }
    EncodingTypePB encoding() const { return _encoding; }

private:
    friend class EncodingInfoResolver;

    template <typename TypeEncodingTraits>
    explicit EncodingInfo(TypeEncodingTraits traits);

    using CreateBuilderFunc = std::function<Status(const PageBuilderOptions&, PageBuilder**)>;
    CreateBuilderFunc _create_builder_func;

    using CreateDecoderFunc = std::function<Status(const Slice&, PageDecoder**)>;
    CreateDecoderFunc _create_decoder_func;

    LogicalType _type;
    EncodingTypePB _encoding;
};

} // namespace starrocks
