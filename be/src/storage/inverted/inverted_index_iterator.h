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
#include <string>

#include "storage/inverted/inverted_index_option.h"
#include "storage/inverted/inverted_reader.h"

namespace starrocks {

class InvertedReader;
enum class InvertedIndexParserType;
enum class InvertedIndexReaderType;

class InvertedIndexIterator {
public:
    InvertedIndexIterator(const std::shared_ptr<TabletIndex>& index_meta, InvertedReader* reader)
            : _index_meta(index_meta), _reader(reader) {
        _analyser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_index_meta->common_properties()));
    }

    Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, roaring::Roaring* bit_map);

    Status read_null(const std::string& column_name, roaring::Roaring* bit_map);

    InvertedIndexParserType get_inverted_index_analyser_type() const;

    InvertedIndexReaderType get_inverted_index_reader_type() const;

private:
    const std::shared_ptr<TabletIndex> _index_meta;
    OlapReaderStatistics* _stats;
    InvertedReader* _reader;
    InvertedIndexParserType _analyser_type;
};

} // namespace starrocks