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

#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/rowset/bitmap_index_reader.h"

namespace starrocks {
class FunctionContext;

class BuiltinInvertedIndexIterator final : public InvertedIndexIterator {
public:
    BuiltinInvertedIndexIterator(const std::shared_ptr<TabletIndex>& index_meta, InvertedReader* reader,
                                 std::unique_ptr<BitmapIndexIterator>& bitmap_itr) :
            InvertedIndexIterator(index_meta, reader), _bitmap_itr(std::move(bitmap_itr)), _like_context(nullptr) {}

    ~BuiltinInvertedIndexIterator() override = default;

    Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, roaring::Roaring* bit_map) override;

    Status read_null(const std::string& column_name, roaring::Roaring* bit_map) override;

    Status close() override;
private:
    Status _equal_query(const Slice* search_query, roaring::Roaring* bit_map);

    Status _wildcard_query(const Slice* search_query, roaring::Roaring* bit_map);

    Status _init_like_context(const Slice& s);

    std::unique_ptr<BitmapIndexIterator> _bitmap_itr;
    std::unique_ptr<FunctionContext> _like_context;
};

} // namespace starrocks