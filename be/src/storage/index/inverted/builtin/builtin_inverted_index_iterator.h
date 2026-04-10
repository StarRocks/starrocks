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

#include "base/string/slice.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/rowset/bitmap_index_reader.h"

namespace starrocks {
class FunctionContext;

std::string get_next_prefix(const Slice& prefix_s);

class BuiltinInvertedIndexIterator final : public InvertedIndexIterator {
public:
    BuiltinInvertedIndexIterator(const std::shared_ptr<TabletIndex>& index_meta, InvertedReader* reader,
                                 OlapReaderStatistics* stats, std::unique_ptr<BitmapIndexIterator>& bitmap_itr,
                                 const size_t& segment_rows)
            : InvertedIndexIterator(index_meta, reader, stats),
              _bitmap_itr(std::move(bitmap_itr)),
              _segment_rows(segment_rows) {}

    ~BuiltinInvertedIndexIterator() override = default;

    Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, roaring::Roaring* bit_map) override;

    Status read_null(const std::string& column_name, roaring::Roaring* bit_map) override;

private:
    Status _equal_query(const Slice* search_query, roaring::Roaring* bit_map);

    Status _wildcard_query(const Slice* search_query, roaring::Roaring* bit_map);

    std::unique_ptr<BitmapIndexIterator> _bitmap_itr;
    size_t _segment_rows;
};

} // namespace starrocks