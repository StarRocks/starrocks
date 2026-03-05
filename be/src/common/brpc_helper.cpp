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

#include "common/brpc_helper.h"

#include "common/config.h"

namespace starrocks {

bool brpc_ignore_overcrowded(std::string_view module) {
    if (module == "query") {
        return config::brpc_query_ignore_overcrowded;
    }
    if (module == "load") {
        return config::brpc_load_ignore_overcrowded;
    }
    return false;
}

} // namespace starrocks
