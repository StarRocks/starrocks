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

#include <stdint.h>

#include <vector>

namespace starrocks::lake {

using KeyIndexInfo = uint32_t;
struct KeyIndexesInfo {
    std::vector<KeyIndexInfo> key_index_infos;
    size_t size() const { return key_index_infos.size(); }
};

} // namespace starrocks::lake
