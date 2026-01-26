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

#include "storage/lake/sst_seek_range.h"

namespace starrocks::lake {

bool SstSeekRange::has_overlap(const PersistentIndexSstableRangePB& range) const {
    const auto* cmp = sstable::BytewiseComparator();
    // range is on the right side of [seek_key, stop_key)
    if (!stop_key.empty() && cmp->Compare(Slice(stop_key), Slice(range.start_key())) <= 0) {
        return false;
    }
    // range is on the left side of [seek_key, stop_key)
    if (cmp->Compare(Slice(range.end_key()), Slice(seek_key)) < 0) {
        return false;
    }
    return true;
}

bool SstSeekRange::full_contains(const PersistentIndexSstableRangePB& range) const {
    const auto* cmp = sstable::BytewiseComparator();
    if (cmp->Compare(Slice(seek_key), Slice(range.start_key())) > 0) {
        return false;
    }
    if (!stop_key.empty() && cmp->Compare(Slice(stop_key), Slice(range.end_key())) <= 0) {
        return false;
    }
    return true;
}

SstSeekRange& SstSeekRange::operator&=(const SstSeekRange& rhs) {
    const auto* cmp = sstable::BytewiseComparator();

    if (seek_key.empty()) {
        seek_key = rhs.seek_key;
    } else if (!rhs.seek_key.empty() && cmp->Compare(Slice(seek_key), Slice(rhs.seek_key)) < 0) {
        seek_key = rhs.seek_key;
    }

    if (stop_key.empty()) {
        stop_key = rhs.stop_key;
    } else if (!rhs.stop_key.empty() && cmp->Compare(Slice(stop_key), Slice(rhs.stop_key)) > 0) {
        stop_key = rhs.stop_key;
    }
    return *this;
}

} // namespace starrocks::lake
