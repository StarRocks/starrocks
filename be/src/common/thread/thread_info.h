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

namespace starrocks {

struct BeThreadInfo {
    std::string group;
    std::string name;
    int64_t pthread_id{0};
    int64_t tid{0};
    bool idle{false};
    int64_t finished_tasks{0};
    int64_t num_bound_cpu_cores{0};
};

} // namespace starrocks
