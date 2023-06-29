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

#include "common/statusor.h"

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

} // namespace starrocks::lake
