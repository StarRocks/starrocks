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

#include "storage/inverted/inverted_reader.h"

namespace starrocks {

#define CLOSE_INPUT(x)  \
    if (x != nullptr) { \
        x->close();     \
        _CLDELETE(x);   \
    }
#define FINALLY_CLOSE_INPUT(x) \
    try {                      \
        CLOSE_INPUT(x)         \
    } catch (...) {            \
    }

class InvertedIndexIterator;
enum class InvertedIndexQueryType;
enum class InvertedIndexReaderType;

class CLuceneInvertedReader : public InvertedReader {
public:
    explicit CLuceneInvertedReader(std::string path, const uint32_t index_id)
            : InvertedReader(std::move(path), index_id) {}

    static Status create(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index,
                         LogicalType field_type, std::unique_ptr<InvertedReader>* res);

    Status new_iterator(const std::shared_ptr<TabletIndex> index_meta, InvertedIndexIterator** iterator) override;
};

class FullTextCLuceneInvertedReader : public CLuceneInvertedReader {
public:
    explicit FullTextCLuceneInvertedReader(std::string path, const uint32_t index_id,
                                           InvertedIndexParserType parser_type)
            : CLuceneInvertedReader(std::move(path), index_id), _parser_type(parser_type) {}

    Status query(OlapReaderStatistics* stats, const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, roaring::Roaring* bit_map) override;

    Status query_null(OlapReaderStatistics* stats, const std::string& column_name, roaring::Roaring* bit_map) override;

    InvertedIndexReaderType get_inverted_index_reader_type() override { return InvertedIndexReaderType::TEXT; }

private:
    InvertedIndexParserType _parser_type;
};

} // namespace starrocks