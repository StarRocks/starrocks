// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <glog/logging.h>

namespace starrocks::starcache {

#define STAR_VLOG VLOG(88)
#define STAR_VLOG_IF(cond)  VLOG_IF(88, (cond))
// #define STAR_VLOG LOG(INFO)
// #define STAR_VLOG_IF(cond)  LOG_IF(INFO, (cond))

#define LOG_IF_ERROR(stmt)                              \
    do {                                                \
        auto&& status__ = (stmt);                       \
        if (UNLIKELY(!status__.ok())) {                 \
            LOG(ERROR) << status__.get_error_msg();     \
        }                                               \
    } while (false);

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
