// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "runtime/tablets_channel.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class MemTracker;

scoped_refptr<TabletsChannel> new_local_tablets_channel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                                        MemTracker* mem_tracker);

} // namespace starrocks
