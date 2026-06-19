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

#include <set>
#include <string>

namespace starrocks {

class DataCache;
class ExecEnv;

class CoreDumpResourceSelector {
public:
    explicit CoreDumpResourceSelector(const std::string& config_value);

    bool should_release(const std::string& resource_name) const;
    bool release_all() const { return _release_all; }
    const std::set<std::string>& modules() const { return _modules; }

private:
    bool _release_all = false;
    std::set<std::string> _modules;
};

void try_release_resources_before_core_dump(ExecEnv* exec_env, DataCache* data_cache);

} // namespace starrocks
