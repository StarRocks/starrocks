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
#include "common/config.h"
#include "common/logging.h"
#include "util/time.h"

namespace starrocks {

template <class LazyMsgCallBack>
class TimeGuard {
public:
    TimeGuard(const char* file_name, size_t line, int64_t timeout_ms, LazyMsgCallBack callback)
            : _file_name(file_name), _line(line), _timeout_ms(timeout_ms), _callback(callback) {
        if (_timeout_ms > 0) {
            _begin_time = MonotonicMillis();
        }
    }

    ~TimeGuard() {
        if (_timeout_ms > 0) {
            int64_t cost_ms = MonotonicMillis() - _begin_time;
            if (cost_ms > _timeout_ms) {
                LOG(WARNING) << _file_name << ":" << _line << " cost:" << cost_ms << " " << _callback();
            }
        }
    }

private:
    const char* _file_name;
    size_t _line;
    int64_t _timeout_ms;
    LazyMsgCallBack _callback;
    int64_t _begin_time = 0;
};

}; // namespace starrocks

#define WARN_IF_TIMEOUT_MS(timeout_ms, msg_callback) \
    auto VARNAME_LINENUM(once) = TimeGuard(__FILE__, __LINE__, timeout_ms, msg_callback)

#define WARN_IF_TIMEOUT(timeout_ms, lazy_msg) WARN_IF_TIMEOUT_MS(timeout_ms, [&]() { return lazy_msg; })

#define WARN_IF_POLLER_TIMEOUT(lazy_msg) WARN_IF_TIMEOUT(config::pipeline_poller_timeout_guard_ms, lazy_msg)