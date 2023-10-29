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

#include "inverted_index_iterator.h"

namespace starrocks {

Status InvertedIndexIterator::read_from_inverted_index(const std::string& column_name, const void* query_value,
                                                       InvertedIndexQueryType query_type, roaring::Roaring* bit_map,
                                                       bool skip_try) {
    RETURN_IF_ERROR(_reader->query(_stats, column_name, query_value, query_type, bit_map));
    return Status::OK();
}

Status InvertedIndexIterator::try_read_from_inverted_index(const std::string& column_name, const void* query_value,
                                                           InvertedIndexQueryType query_type, uint32_t* count) {
    RETURN_IF_ERROR(_reader->try_query(_stats, column_name, query_value, query_type, count));
    return Status::OK();
}

Status InvertedIndexIterator::read_null(const std::string& column_name, roaring::Roaring* bit_map) {
    //    RETURN_IF_ERROR(_reader->query_null(_stats, column_name, bit_map));
    return Status::NotSupported("not implemented");
}

InvertedIndexParserType InvertedIndexIterator::get_inverted_index_analyser_type() const {
    return _analyser_type;
}

InvertedIndexReaderType InvertedIndexIterator::get_inverted_index_reader_type() const {
    return _reader->get_inverted_index_reader_type();
}

} // namespace starrocks