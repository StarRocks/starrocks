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

#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "fs/fs.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class QuerySpillManager {
public:
    QuerySpillManager(const TUniqueId& uid) : _uid(uid) {}
    size_t pending_spilled_bytes() { return _spilled_bytes; }
    void update_spilled_bytes(size_t spilled_bytes) { _spilled_bytes += spilled_bytes; }

private:
    TUniqueId _uid;
    std::atomic_size_t _spilled_bytes;
};
} // namespace starrocks