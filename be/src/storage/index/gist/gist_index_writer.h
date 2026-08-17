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
#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "storage/index/gist/rtree.h"
#include "storage/tablet_schema.h"

namespace starrocks {

/**
 * GiSTIndexWriter accumulates geometry values from a GEOMETRY column during
 * segment writing, then bulk-builds a packed STR R-Tree and writes it as a
 * standalone .gst file.
 *
 * Lifecycle:  create() → init() → append()* → finish()
 */
class GiSTIndexWriter {
public:
    static void create(const std::shared_ptr<TabletIndex>& tablet_index,
                       const std::string& index_file_path,
                       std::unique_ptr<GiSTIndexWriter>* res);

    GiSTIndexWriter(const std::shared_ptr<TabletIndex>& tablet_index,
                    std::string index_file_path);

    Status init();

    /// Append all rows from a BinaryColumn of TYPE_GEOMETRY values.
    Status append(const Column& src);

    /// Bulk-build the R-Tree and write the .gst file.  Sets *index_size.
    Status finish(uint64_t* index_size);

    uint64_t estimate_buffer_size() const;

private:
    std::shared_ptr<TabletIndex> _tablet_index;
    std::string _index_file_path;
    int _node_capacity{50};
    uint32_t _next_row_id{0};
    std::vector<RTreeLeafEntry> _entries;
};

} // namespace starrocks
