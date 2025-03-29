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

#include "runtime/diagnose_daemon.h"

#include <fmt/format.h>

#include "common/config.h"
#include "util/stack_util.h"

namespace starrocks {

static std::string diagnose_type_name(DiagnoseType type) {
    switch (type) {
    case DiagnoseType::STACK_TRACE:
        return "STACK_TRACE";
    default:
        return fmt::format("UNKNOWN({})", static_cast<int>(type));
    }
}

static void split_and_log_long_message(const std::string& raw_log) {
    const size_t max_message_size = google::LogMessage::kMaxLogMessageLen;
    if (raw_log.size() <= max_message_size) {
        LOG(INFO) << raw_log;
        return;
    }
    // split into multiple lines to ensure the log in one line to log in one message
    std::vector<std::string> lines;
    std::istringstream stream(raw_log);
    std::string buf;
    while (std::getline(stream, buf, '\n')) {
        lines.push_back(buf);
    }
    std::string message;
    for (auto& line : lines) {
        auto msg_size = message.size() + line.size() + (message.empty() ? 0 : 1);
        if (msg_size > max_message_size) {
            if (!message.empty()) {
                LOG(INFO) << message;
                message.clear();
            }
            if (line.size() > max_message_size) {
                LOG(INFO) << line;
            } else {
                message = line;
            }
        } else {
            message += (message.empty() ? "" : "\n") + line;
        }
    }
    if (!message.empty()) {
        LOG(INFO) << message;
    }
}

Status DiagnoseDaemon::init() {
    return ThreadPoolBuilder("diagnose")
            .set_min_threads(0)
            .set_max_threads(1)
            .set_idle_timeout(MonoDelta::FromSeconds(10))
            .build(&_single_thread_pool);
}

Status DiagnoseDaemon::diagnose(const DiagnoseRequest& request) {
    return _single_thread_pool->submit_func([this, request]() { _execute_request(request); });
}

void DiagnoseDaemon::stop() {
    if (_single_thread_pool) {
        _single_thread_pool->shutdown();
    }
}

void DiagnoseDaemon::_execute_request(const DiagnoseRequest& request) {
    switch (request.type) {
    case DiagnoseType::STACK_TRACE:
        _perform_stack_trace(request.context);
        break;
    default:
        LOG(WARNING) << "unknown diagnose type: " << diagnose_type_name(request.type)
                     << ", context: " << request.context;
    }
}

void DiagnoseDaemon::_perform_stack_trace(const std::string& context) {
    int64_t interval = MonotonicMillis() - _last_stack_trace_time_ms;
    if (_last_stack_trace_time_ms > 0 && interval < config::diagnose_stack_trace_interval_ms) {
        VLOG(2) << "skip to diagnose stack trace, last time: " << _last_stack_trace_time_ms
                << " ms, interval: " << interval << " ms";
        return;
    }
    int64_t start_time = MonotonicMillis();
    int64_t id = _diagnose_id.fetch_add(1);
    std::string stack_trace = get_stack_trace_for_all_threads(fmt::format("DIAGNOSE {} - ", id));
    _last_stack_trace_time_ms = MonotonicMillis();
    LOG(INFO) << "diagnose stack trace, id: " << id << ", cost: " << (_last_stack_trace_time_ms - start_time)
              << " ms, size: " << stack_trace.size() << ", context: [" << context << "]";
    split_and_log_long_message(stack_trace);
}

} // namespace starrocks