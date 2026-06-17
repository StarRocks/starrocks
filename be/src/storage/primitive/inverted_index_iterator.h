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

#include <roaring/roaring.hh>
#include <string_view>

#include "common/status.h"
#include "storage/primitive/inverted_index_common.h"

namespace starrocks {

class InvertedIndexIterator {
public:
    virtual ~InvertedIndexIterator() = default;

    virtual Status read_from_inverted_index(std::string_view column_name, const void* query_value,
                                            InvertedIndexQueryType query_type, roaring::Roaring* bit_map) = 0;

    virtual Status read_null(std::string_view column_name, roaring::Roaring* bit_map) = 0;

    virtual bool is_untokenized() const = 0;

    virtual Status close() { return Status::OK(); }
};

} // namespace starrocks
