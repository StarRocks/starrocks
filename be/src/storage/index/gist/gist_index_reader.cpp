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

#include "storage/index/gist/gist_index_reader.h"

#include <fstream>
#include <sstream>

#include "common/logging.h"

namespace starrocks {

Status GiSTIndexReader::init(const std::string& index_file_path) {
    std::ifstream ifs(index_file_path, std::ios::binary | std::ios::ate);
    if (!ifs) {
        return Status::IOError("GiSTIndexReader: cannot open " + index_file_path);
    }
    std::streamsize file_size = ifs.tellg();
    if (file_size <= 0) {
        return Status::IOError("GiSTIndexReader: empty file " + index_file_path);
    }
    ifs.seekg(0, std::ios::beg);
    _index_data.resize(static_cast<size_t>(file_size));
    if (!ifs.read(&_index_data[0], file_size)) {
        return Status::IOError("GiSTIndexReader: read failed for " + index_file_path);
    }
    return Status::OK();
}

Status GiSTIndexReader::search_intersects(const MBR& query_mbr,
                                          std::vector<uint32_t>* result_row_ids) const {
    rtree_search_intersects(_index_data.data(), _index_data.size(), query_mbr, result_row_ids);
    return Status::OK();
}

Status GiSTIndexReader::search_within(const MBR& query_mbr,
                                      std::vector<uint32_t>* result_row_ids) const {
    rtree_search_within(_index_data.data(), _index_data.size(), query_mbr, result_row_ids);
    return Status::OK();
}

Status GiSTIndexReader::search_contains(const MBR& query_mbr,
                                        std::vector<uint32_t>* result_row_ids) const {
    rtree_search_contains(_index_data.data(), _index_data.size(), query_mbr, result_row_ids);
    return Status::OK();
}

} // namespace starrocks
