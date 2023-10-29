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

#include <cstdint>
#include <string>

#include "storage/olap_common.h"

#define VECTOR_INDEX_SUFFIX "vi"
#define INVERTED_INDEX_MARK_NAME "ivt"
#define INVERTED_INDEX_SUFFIX ".ividx"

namespace starrocks {
class IndexDescriptor {
public:
    static std::string inverted_index_file_path(const std::string& rowset_dir, const std::string& rowset_id,
                                                int segment_id, int64_t index_id);

    static const std::string get_temporary_null_bitmap_file_name() { return "null_bitmap"; }

    static const std::string get_temporary_bkd_index_data_file_name() { return "bkd"; }

    static const std::string get_temporary_bkd_index_meta_file_name() { return "bkd_meta"; }

    static const std::string get_temporary_bkd_index_file_name() { return "bkd_index"; }
};

} // namespace starrocks