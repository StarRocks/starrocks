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

#include "runtime/tablets_channel.h"

namespace starrocks {

class LoadChannel;
class MemTracker;
struct TabletsChannelKey;

namespace lake {
class TabletManager;
}

std::shared_ptr<TabletsChannel> new_lake_tablets_channel(LoadChannel* load_channel, lake::TabletManager* tablet_manager,
                                                         const TabletsChannelKey& key, MemTracker* mem_tracker);

} // namespace starrocks
