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

#include "fs/remote_read_context.h"

namespace starrocks {

class RuntimeState;

// Whom the remote reads of `state`'s query are for. All of them form one flow of the remote read scheduler,
// whatever fragment, tablet or split they are for, and carry the resource group's cpu_weight and configuration
// version as they were when the query started: the weight the scan executor gives the group's scan tasks, so both
// schedulers favour the same groups.
RemoteReadContext query_remote_read_context(const RuntimeState* state);

} // namespace starrocks
