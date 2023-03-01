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

#include "exec/spill/spiller_path_provider.h"
#include "fs/fs.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {
class TUniqueId;
// TODO: RuntimeSpillManager to own spill root paths
// Spill Manger
// parse conf
class QuerySpillManager {
public:
    static Status init();
    Status init(const TUniqueId& uid);
    SpillPathProviderFactory provider(const std::string& prefix);

    size_t pending_spilled_bytes() { return _spilled_bytes; }
    void update_spilled_bytes(size_t spilled_bytes) { _spilled_bytes += spilled_bytes; }

private:
    static std::vector<std::string> _spill_root_paths;

    TUniqueId _uid;
    std::vector<std::string> _spill_paths(const TUniqueId& uid) const;
    std::unordered_map<std::string, SpillPathProviderFactory> _spill_provider_factorys;
    std::mutex _mutex;
    std::atomic_size_t _spilled_bytes;
    FileSystem* _fs = FileSystem::Default();
};
} // namespace starrocks