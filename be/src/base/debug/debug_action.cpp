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

#include "base/debug/debug_action.h"

#include <optional>

#include "gen_cpp/InternalService_types.h"

namespace starrocks {
void DebugActionMgr::add_action(const TExecDebugOption& debug_action) {
    DebugAction action;
    action.node_id = debug_action.debug_node_id;
    action.value = debug_action.value;
    switch (debug_action.debug_action) {
    case TDebugAction::WAIT:
        action.action = EnumDebugAction::WAIT;
        break;
    case TDebugAction::BLOCK_SOURCE_OPERATOR:
        action.action = EnumDebugAction::BLOCK_SOURCE_OPERATOR;
        break;
    case TDebugAction::BLOCK_SINK_OPERATOR:
        action.action = EnumDebugAction::BLOCK_SINK_OPERATOR;
        break;
    default:
        return;
    }
    _debug_actions.emplace(action.node_id, action);
}

} // namespace starrocks
