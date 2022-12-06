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

#include <assert.h> // NOLINT(modernize-deprecated-headers)
#include <fmt/format.h>

#include <string_view>

namespace starrocks::lake {

inline std::string join_path(std::string_view parent, std::string_view child) {
    assert(!parent.empty());
    assert(!child.empty());
    assert(child.back() != '/');
    if (parent.back() != '/') {
        return fmt::format("{}/{}", parent, child);
    }
    return fmt::format("{}{}", parent, child);
}

} // namespace starrocks::lake
