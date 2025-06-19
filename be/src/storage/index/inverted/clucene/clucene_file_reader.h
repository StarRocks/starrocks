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

#include <memory>
#include <shared_mutex>

#include "CLucene.h"
#include "common/config.h"
#include "common/statusor.h"
#include "storage/index/inverted/clucene/clucene_common.h"

namespace starrocks {

class FileSystem;
class TabletIndex;

class ReaderFileEntry;
class CLuceneCompoundReader;

class CLuceneFileReader {
public:
    // Map to hold the file entries for each index ID.
    using IndicesEntriesMap = std::map<int64_t, std::unique_ptr<EntriesType>>;

    CLuceneFileReader(std::shared_ptr<FileSystem> fs, std::string index_file_path);

    Status init(int32_t read_buffer_size = config::inverted_index_read_buffer_size);
    StatusOr<std::unique_ptr<CLuceneCompoundReader>> open(const std::shared_ptr<TabletIndex>& index_meta) const;
    void debug_file_entries();
    std::string get_index_file_path(const TabletIndex* index_meta) const;
    Status index_file_exist(const TabletIndex* index_meta, bool* res) const;
    Status has_null(const TabletIndex* index_meta, bool* res) const;
    StatusOr<InvertedIndexDirectoryMap> get_all_directories();
    // open file v2, init _stream
    int64_t get_inverted_file_size() const { return _stream == nullptr ? 0 : _stream->length(); }

protected:
    Status _init_from(int32_t read_buffer_size);
    StatusOr<std::unique_ptr<CLuceneCompoundReader>> _open(const int64_t& index_id) const;

private:
    IndicesEntriesMap _indices_entries;
    std::unique_ptr<CL_NS(store)::IndexInput> _stream = nullptr;
    std::shared_ptr<FileSystem> _fs;
    std::string _index_file_path;
    int32_t _read_buffer_size = -1;
    mutable std::shared_mutex _mutex; // Use mutable for const read operations
    bool _inited = false;
};

} // namespace starrocks
