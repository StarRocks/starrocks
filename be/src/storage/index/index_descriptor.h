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

#include <fmt/format.h>

#include <cstdint>
#include <string>

#include "storage/olap_common.h"

namespace starrocks {
class IndexDescriptor {
public:
    inline static const std::string mark_word = "TENANNEMPTYMARK";
    inline static const int64_t mark_word_len = 15;

    static StatusOr<std::string> get_index_file_path(const IndexType index_type, const std::string& rowset_dir,
                                                     const std::string& rowset_id, int segment_id, int64_t index_id) {
        switch (index_type) {
        case VECTOR:
            return vector_index_file_path(rowset_dir, rowset_id, segment_id, index_id);
        case GIN:
            return inverted_index_file_path(rowset_dir, rowset_id, segment_id, index_id);
        default:
            return Status::NotSupported("Not supported");
        }
    }

    inline static const std::unordered_map<std::string, int32_t> index_file_info_map = {
            {"null_bitmap", 1}, {"segments.gen", 2}, {"segments_", 3}, {"fnm", 4},
            {"tii", 5},         {"bkd_meta", 6},     {"bkd_index", 7}};

    static std::string get_inverted_index_file_parent(const std::string& inverted_index_file_path) {
        auto idx = inverted_index_file_path.find_last_of('/');
        if (idx == std::string::npos) {
            return inverted_index_file_path;
        }
        return inverted_index_file_path.substr(0, idx);
    }

    static std::string get_inverted_index_file_name(const std::string& inverted_index_file_path) {
        auto idx = inverted_index_file_path.find_last_of('/');
        if (idx == std::string::npos) {
            return inverted_index_file_path;
        }
        return inverted_index_file_path.substr(idx + 1);
    }

    static std::string tmp_inverted_index_file_path(const std::string& tmp_dir_path,
                                                    const std::string& inverted_index_file_name) {
        // temporary inverted index is a directory, it's path like below:
        // {tmp_dir}/{rowset_id}_{seg_id}_{index_id}.ivt
        // NOTE: maybe should add tablet id for easier deletion?
        return fmt::format("{}/{}", tmp_dir_path, inverted_index_file_name);
    }

    static std::string inverted_index_file_path(const std::string& segment_location, int index_id) {
        // segment location will end with '.dat', should remove first.
        std::string segment_path = segment_location;
        if (segment_path.ends_with(".dat")) {
            segment_path = segment_path.substr(0, segment_path.find_last_of('.'));
        }
        return fmt::format("{}_{}.{}", segment_path, index_id, INVERTED_INDEX_MARK_NAME);
    }

    static std::string inverted_index_file_path(const std::string& rowset_dir, const std::string& rowset_id,
                                                int segment_id, int64_t index_id) {
        // inverted index is a directory, it's path likes below
        // {rowset_dir}/{schema_hash}/{rowset_id}_{seg_num}_{index_id}
        return fmt::format("{}/{}_{}_{}.{}", rowset_dir, rowset_id, segment_id, index_id, "ivt");
    }

    static std::string vector_index_file_path(const std::string& rowset_dir, const std::string& rowset_id,
                                              int segment_id, int64_t index_id) {
        // {rowset_dir}/{schema_hash}/{rowset_id}_{seg_num}_{index_id}.vi
        return fmt::format("{}/{}_{}_{}.{}", rowset_dir, rowset_id, segment_id, index_id, "vi");
    }

    static std::string get_temporary_null_bitmap_file_name() { return "null_bitmap"; }
};

} // namespace starrocks
