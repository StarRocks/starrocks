// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

namespace starrocks::starcache {

// lock
#define LOCK_IF(lck, condition)    \
    if (condition) {               \
        (lck)->lock();             \
    }

#define UNLOCK_IF(lck, condition)  \
    if (condition) {               \
        (lck)->unlock();           \
    }
    
} // namespace starrocks::starcache
