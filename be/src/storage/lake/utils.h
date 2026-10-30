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

#include <limits>

#include "common/statusor.h"
#include "storage/persistent_index.h"

namespace starrocks::lake {

static constexpr const char* const kNotFoundPrompt = R"DEL(
Common causes for this error are:
1. A schema change has been made to the table
2. The current query is a slow query
For the first case, you can retry the current query.
For the second case, see "admin show frontend config like 'lake_autovacuum_grace_period_minutes'" for reference.)DEL";

inline Status enhance_error_prompt(Status st) {
    if (!st.is_not_found()) {
        return st;
    } else {
        return st.clone_and_append(kNotFoundPrompt);
    }
}

template <typename T>
inline StatusOr<T> enhance_error_prompt(StatusOr<T> res) {
    if (!res.status().is_not_found()) {
        return res;
    } else {
        return res.status().clone_and_append(kNotFoundPrompt);
    }
}

inline IndexValue build_index_value(const IndexValueWithVerPB& value) {
    return IndexValue(((uint64_t)value.rssid() << 32 | value.rowid()));
}

// A tombstone in the persistent-index sstable encodes NullIndexValue as the full
// pair (rssid=UINT32_MAX, rowid=UINT32_MAX). The packed 64-bit value compares
// equal to NullIndexValue only while *both* halves are UINT32_MAX, so any code
// that mutates rssid/rowid (e.g. shared_rssid overwrite, rssid_offset shift)
// must skip these entries — otherwise the caller's NullIndexValue check
// downgrades the tombstone to a live pointer and downstream upserts feed the
// publish delvec builder with bogus rowids. Project version freely; only the
// rssid/rowid pair is the sentinel.
inline bool is_index_tombstone(const IndexValueWithVerPB& value) {
    return value.rssid() == std::numeric_limits<uint32_t>::max() &&
           value.rowid() == std::numeric_limits<uint32_t>::max();
}

} // namespace starrocks::lake
