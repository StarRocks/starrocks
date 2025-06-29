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

#include "util/debug_action.h"

#include <optional>

#include "gen_cpp/InternalService_types.h"

namespace starrocks {
void DebugActionMgr::add_action(const TExecDebugOption& debug_action) {
    if (debug_action.debug_action == TDebugAction::WAIT) {
        DebugAction action;
        action.node_id = debug_action.debug_node_id;
        action.action = EnumDebugAction::WAIT;
        action.value = debug_action.value;
        _debug_actions.emplace(action.node_id, action);
    }
}

} // namespace starrocks
