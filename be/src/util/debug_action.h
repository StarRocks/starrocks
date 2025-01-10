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

#include <optional>

#include "util/hash.h"
#include "util/phmap/phmap.h"

namespace starrocks {
class TExecDebugOption;
enum class EnumDebugAction { WAIT };

struct DebugAction {
    int node_id;
    EnumDebugAction action;
    int value;
    bool is_wait_action() const { return action == EnumDebugAction::WAIT; }
};

class DebugActionMgr {
public:
    void add_action(const TExecDebugOption&);

    std::optional<DebugAction> get_debug_action(int plan_node_id) const {
        if (auto iter = _debug_actions.find(plan_node_id); iter != _debug_actions.end()) {
            return iter->second;
        }
        return {};
    }

private:
    phmap::flat_hash_map<int32_t, DebugAction, StdHash<int32_t>> _debug_actions;
};
} // namespace starrocks
