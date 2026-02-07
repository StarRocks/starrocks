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

#include <cstddef>
#include <cstdint>

namespace starrocks {

// A chunk of continuous memory.
// Almost all files depend on this struct, and each modification
// will result in recompilation of all files. So, we put it in a
// file to keep this file simple and infrequently changed.
struct MemChunk {
    uint8_t* data = nullptr;
    size_t size;
    int core_id;
};

} // namespace starrocks
