// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "runtime/tablets_channel.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class MemTracker;

std::shared_ptr<TabletsChannel> new_local_tablets_channel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                                          std::shared_ptr<MemTracker> mem_tracker);

} // namespace starrocks
