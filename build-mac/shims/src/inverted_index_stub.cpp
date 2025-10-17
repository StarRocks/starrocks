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

#include "common/status.h"
#include "fmt/format.h"
#include "storage/index/inverted/inverted_index_iterator.h"

namespace starrocks {

Status InvertedIndexIterator::read_from_inverted_index(const std::string& column_name, const void* /*query_value*/,
                                                       InvertedIndexQueryType /*query_type*/,
                                                       roaring::Roaring* bit_map) {
    if (bit_map != nullptr) {
        bit_map->clear();
    }
    return Status::NotSupported(
            fmt::format("Inverted index is disabled on macOS (column: {})", column_name));
}

Status InvertedIndexIterator::read_null(const std::string& column_name, roaring::Roaring* bit_map) {
    if (bit_map != nullptr) {
        bit_map->clear();
    }
    return Status::NotSupported(
            fmt::format("Inverted index null query is disabled on macOS (column: {})", column_name));
}

InvertedIndexReaderType InvertedIndexIterator::get_inverted_index_reader_type() const {
    return InvertedIndexReaderType::UNKNOWN;
}

} // namespace starrocks
