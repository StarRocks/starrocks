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

#include "common/status.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group.h"
#include "gutil/macros.h"

#define BREAK_IF_YIELD(wg, yield, time_spent_ns)                                                     \
    if (time_spent_ns >= workgroup::WorkGroup::YIELD_MAX_TIME_SPENT) {                               \
        *yield = true;                                                                               \
        break;                                                                                       \
    }                                                                                                \
    if (wg != nullptr && time_spent_ns >= workgroup::WorkGroup::YIELD_PREEMPT_MAX_TIME_SPENT &&      \
        wg->scan_sched_entity()->in_queue()->should_yield(wg->scan_sched_entity(), time_spent_ns)) { \
        *yield = true;                                                                               \
        break;                                                                                       \
    }

#define RETURN_OK_IF_NEED_YIELD(wg, yield, time_spent_ns)                                            \
    if (time_spent_ns >= workgroup::WorkGroup::YIELD_MAX_TIME_SPENT) {                               \
        *yield = true;                                                                               \
        return Status::OK();                                                                         \
    }                                                                                                \
    if (wg != nullptr && time_spent_ns >= workgroup::WorkGroup::YIELD_PREEMPT_MAX_TIME_SPENT &&      \
        wg->scan_sched_entity()->in_queue()->should_yield(wg->scan_sched_entity(), time_spent_ns)) { \
        *yield = true;                                                                               \
        return Status::OK();                                                                         \
    }
#define RETURN_IF_ERROR_EXCEPT_YIELD(stmt)                                                            \
    do {                                                                                              \
        auto&& status__ = (stmt);                                                                     \
        if (UNLIKELY(!status__.ok() && !status__.is_yield())) {                                       \
            return to_status(status__).clone_and_append_context(__FILE__, __LINE__, AS_STRING(stmt)); \
        }                                                                                             \
    } while (false)

#define RETURN_IF_YIELD(yield) \
    if (yield) {               \
        return Status::OK();   \
    }
