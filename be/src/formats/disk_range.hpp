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
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <vector>

namespace starrocks {

class DiskRange {
public:
    DiskRange(const int64_t off, const int64_t len) : _offset(off), _length(len) {
        DCHECK(off >= 0);
        DCHECK(len >= 0);
    }

    /**
    * Returns the minimal DiskRange that encloses both this DiskRange
    * and otherDiskRange. If there was a gap between the ranges the
    * new range will cover that gap.
    */
    inline DiskRange span(const DiskRange& other_disk_range) const {
        const int64_t start_off = std::min(offset(), other_disk_range.offset());
        const int64_t end_off = std::max(end(), other_disk_range.end());
        return {start_off, end_off - start_off};
    }

    inline int64_t end() const { return _offset + _length; }

    inline int64_t offset() const { return _offset; }

    inline int64_t length() const { return _length; }

private:
    int64_t _offset = 0;
    int64_t _length = 0;
};

class DiskRangeHelper {
public:
    /**
     * Merge disk ranges that are closer than max_merge_distance.
     * If merged disk range is larger than max_merged_size, we will not merge it anymore
     */
    static void merge_adjacent_disk_ranges(std::vector<DiskRange> disk_ranges, const int64_t max_merge_distance,
                                           const int64_t max_merged_size, std::vector<DiskRange>& merged_disk_ranges) {
        if (disk_ranges.empty()) {
            return;
        }

        std::sort(disk_ranges.begin(), disk_ranges.end(),
                  [](const DiskRange& a, const DiskRange& b) { return a.offset() < b.offset(); });

        DiskRange last = disk_ranges[0];
        for (size_t i = 1; i < disk_ranges.size(); i++) {
            DiskRange current = disk_ranges[i];
            DiskRange merged = last.span(current);
            if (merged.length() <= max_merged_size && last.end() + max_merge_distance >= current.offset()) {
                last = merged;
            } else {
                merged_disk_ranges.emplace_back(last.offset(), last.length());
                last = current;
            }
        }
        merged_disk_ranges.emplace_back(last.offset(), last.length());
    }
};

} // namespace starrocks
