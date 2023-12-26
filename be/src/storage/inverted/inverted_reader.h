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

#include <utility>

#include "fs/fs_util.h"
#include "roaring/roaring.hh"
#include "storage/inverted/inverted_index_common.hpp"
#include "storage/inverted/inverted_index_iterator.h"
#include "storage/inverted/inverted_index_option.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class InvertedIndexIterator;
enum class InvertedIndexReaderType;
enum class InvertedIndexQueryType;

class InvertedReader {
public:
    InvertedReader(std::string index_path, uint32_t index_id)
            : _index_path(std::move(index_path)), _index_id(index_id) {}

    virtual ~InvertedReader() = default;

    // create a new column iterator. Client should delete returned iterator
    virtual Status new_iterator(const std::shared_ptr<TabletIndex> index_meta, InvertedIndexIterator** iterator) = 0;

    virtual Status query(OlapReaderStatistics* stats, const std::string& column_name, const void* query_value,
                         InvertedIndexQueryType query_type, roaring::Roaring* bit_map) = 0;

    virtual Status query_null(OlapReaderStatistics* stats, const std::string& column_name,
                              roaring::Roaring* bit_map) = 0;

    virtual InvertedIndexReaderType get_inverted_index_reader_type() = 0;

    bool indexExists(const std::string& index_file_path) { return fs::path_exist(index_file_path); }

    std::string get_index_path() { return _index_path; }

    uint32_t get_index_id() { return _index_id; }

protected:
    std::string _index_path;
    uint32_t _index_id;
};

} // namespace starrocks