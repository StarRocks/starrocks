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

#include <algorithm>
#include <memory>
#include <vector>

#include "storage/olap_common.h"

namespace starrocks {

class Segment;
class BaseRowset;

using SegmentSharedPtr = std::shared_ptr<Segment>;
using BaseRowsetSharedPtr = std::shared_ptr<BaseRowset>;

class BaseRowset {
public:
    virtual ~BaseRowset() = default;
    virtual RowsetId rowset_id() const = 0;
    virtual int64_t num_rows() const = 0;
    virtual bool is_overlapped() const = 0;
    //virtual StatusOr<std::vector<SegmentSharedPtr>> get_segments() = 0;
    virtual std::vector<SegmentSharedPtr> get_segments() = 0;

    // get_segments() with the nullptr placeholders removed. A null entry appears only for a lake rowset
    // when experimental_lake_ignore_lost_segment dropped a physically-missing segment (local rowsets
    // never produce nulls). Use this from consumers that just iterate the segments and do NOT need
    // positional alignment with the segment metadata (e.g. scan-split planning, compaction sizing);
    // consumers that derive an rssid from a segment's position must use get_segments() instead and
    // handle the null slots themselves.
    std::vector<SegmentSharedPtr> get_non_null_segments() {
        std::vector<SegmentSharedPtr> segments = get_segments();
        segments.erase(std::remove(segments.begin(), segments.end(), nullptr), segments.end());
        return segments;
    }

    virtual Status load() { return Status::OK(); };

    virtual bool has_data_files() const = 0;

    virtual int64_t start_version() const = 0;
    virtual int64_t end_version() const = 0;
};

} // namespace starrocks