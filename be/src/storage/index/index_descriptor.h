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

    // Compound index file path — one .idx per (rowset, segment), holding all
    // tantivy / future-vector indexes for that segment. Distinct from
    // inverted_index_file_path so clucene/builtin paths stay untouched.
    static std::string compound_index_file_path(const std::string& rowset_dir,
                                                const std::string& rowset_id, int segment_id) {
        return fmt::format("{}/{}_{}.{}", rowset_dir, rowset_id, segment_id, "idx");
    }

    // Derive the compound .idx path from the segment .dat path by replacing
    // the extension. Works for both local paths and remote URIs (s3://...).
    static std::string compound_index_file_path_from_segment(const std::string& segment_path) {
        auto dot_pos = segment_path.rfind('.');
        if (dot_pos != std::string::npos) {
            return segment_path.substr(0, dot_pos) + ".idx";
        }
        return segment_path + ".idx";
    }

    static std::string lake_compound_index_build_dir(const std::string& tmp_root, int64_t tablet_id,
                                                     int64_t txn_id, int segment_id, int64_t index_id,
                                                     uintptr_t instance_key) {
        if (tablet_id != 0 && txn_id != 0) {
            return fmt::format("{}/{}_{}_{}_{}_{:x}.ivt", tmp_root, tablet_id, txn_id, segment_id, index_id,
                               instance_key);
        }
        return fmt::format("{}/{}_{}_{}.ivt", tmp_root, instance_key, segment_id, index_id);
    }

    static const std::string get_temporary_null_bitmap_file_name() { return "null_bitmap"; }
};

} // namespace starrocks
