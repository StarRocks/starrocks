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

#include <CLucene.h>

#include <utility>

#include "storage/index/inverted/inverted_reader.h"
#include "storage/rowset/bitmap_index_reader.h"

namespace starrocks {

class InvertedIndexIterator;
enum class InvertedIndexQueryType;
enum class InvertedIndexReaderType;
class IndexReadOptions;
class FunctionContext;

class BuiltinInvertedReader : public InvertedReader {
public:
    explicit BuiltinInvertedReader(const uint32_t index_id) : InvertedReader("", index_id), _bitmap_index(nullptr) {}

    ~BuiltinInvertedReader() override {}

    static Status create(const std::shared_ptr<TabletIndex>& tablet_index,
                         LogicalType field_type, std::unique_ptr<InvertedReader>* res);

    Status new_iterator(const std::shared_ptr<TabletIndex> index_meta, InvertedIndexIterator** iterator,
                        const IndexReadOptions& index_opt) override;

    Status load(const IndexReadOptions& opt, void* meta) override;

    /*
       Implemented in BuiltinInvertedIndexIterator. For builtin inverted index, we have to define bitmap index iterator in BuiltinInvertedIndexIterator.
       Because BuiltinInvertedReader may be accessed by multiple threads, and bitmap index iterator is not thread-safe.
    */
    Status query(OlapReaderStatistics* stats, const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, roaring::Roaring* bit_map) override {
        return Status::InternalError("Unreachable");
    }

    Status query_null(OlapReaderStatistics* stats, const std::string& column_name, roaring::Roaring* bit_map) override {
        return Status::InternalError("Unreachable");
    }

    InvertedIndexReaderType get_inverted_index_reader_type() override { return InvertedIndexReaderType::TEXT; }

private:
    std::unique_ptr<BitmapIndexReader> _bitmap_index;
};

} // namespace starrocks