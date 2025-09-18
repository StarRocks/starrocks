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

#include "storage/index/inverted/char_filter/char_replace_char_filter.h"
#include "storage/index/inverted/inverted_index_common.h"

namespace starrocks {

class CharFilterFactory {
public:
    template <typename... Args>
    static lucene::analysis::CharFilter* create(const std::string& name, Args&&... args) {
        if (name == INVERTED_INDEX_CHAR_FILTER_TYPE_REPLACE) {
            return new CharReplaceCharFilter(std::forward<Args>(args)...);
        }
        return nullptr;
    }
};

} // namespace starrocks