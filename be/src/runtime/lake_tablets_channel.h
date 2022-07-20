// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "runtime/tablets_channel.h"

namespace starrocks {

class LoadChannel;
class MemTracker;
class TabletsChannelKey;

scoped_refptr<TabletsChannel> new_lake_tablets_channel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                                       std::shared_ptr<MemTracker> mem_tracker);

} // namespace starrocks
