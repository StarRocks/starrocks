// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "runtime/tablets_channel.h"

namespace starrocks {

class LoadChannel;
class MemTracker;
struct TabletsChannelKey;

std::shared_ptr<TabletsChannel> new_lake_tablets_channel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                                         MemTracker* mem_tracker);

} // namespace starrocks
