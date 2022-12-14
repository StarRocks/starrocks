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

#include <deque>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>

#include "common/status.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/Types_types.h"
#include "util/hash_util.hpp"
#include "util/spinlock.h"

namespace starrocks::debug {

struct QueryTraceContext;

struct QueryTraceEvent {
    std::string name;
    std::string category;
    int64_t id; // used for async event
    char phase;
    int64_t timestamp;
    int64_t duration = -1; // used for compelete event
    // TUniqueId::hi is all the same in one query, so we use TUniqueId::lo to specific one fragment instance
    int64_t instance_id;
    // driver pointer
    std::uintptr_t driver;
    std::thread::id thread_id;
    std::vector<std::pair<std::string, std::string>> args;

    std::string to_string();

    static QueryTraceEvent create(const std::string& name, const std::string& category, int64_t id, char phase,
                                  int64_t timestamp, int64_t duration, int64_t instance_id, std::uintptr_t driver,
                                  std::vector<std::pair<std::string, std::string>>&& args);

    static QueryTraceEvent create_with_ctx(const std::string& name, const std::string& category, int64_t id, char phase,
                                           const QueryTraceContext& ctx);

    static QueryTraceEvent create_with_ctx(const std::string& name, const std::string& category, int64_t id, char phase,
                                           int64_t start_ts, int64_t duration, const QueryTraceContext& ctx);

private:
    std::string args_to_string();
};

// event buffer for a single pipeline driver
class EventBuffer {
public:
    EventBuffer() = default;
    ~EventBuffer() = default;

    void add(QueryTraceEvent&& event);

private:
    friend class QueryTrace;
    typedef SpinLock Mutex;
    Mutex _mutex;
    std::deque<QueryTraceEvent> _buffer;
};

class QueryTrace {
public:
    QueryTrace(const TUniqueId& query_id, bool is_enable);
    ~QueryTrace() = default;

    // init event buffer for all drivers in a single fragment instance
    void register_drivers(const TUniqueId& fragment_instance_id, starrocks::pipeline::Drivers& drivers);

    Status dump();

    static void set_tls_trace_context(QueryTrace* query_trace, const TUniqueId& fragment_instance_id,
                                      std::uintptr_t driver);

private:
#ifdef ENABLE_QUERY_DEBUG_TRACE
    TUniqueId _query_id;
    [[maybe_unused]] bool _is_enable = false;
    [[maybe_unused]] int64_t _start_ts = -1;

    std::shared_mutex _mutex;
    std::unordered_map<std::uintptr_t, std::unique_ptr<EventBuffer>> _buffers;

    // fragment_instance_id => driver list, it will be used to generate meta event
    std::unordered_map<TUniqueId, std::shared_ptr<std::unordered_set<std::uintptr_t>>> _fragment_drivers;
#endif
};

class ScopedTracer {
public:
    ScopedTracer(std::string name, std::string category);
    ~ScopedTracer() noexcept;

private:
    std::string _name;
    std::string _category;
    int64_t _start_ts;
    int64_t _duration = -1;
};

struct QueryTraceContext {
    static constexpr int64_t DEFAULT_EVENT_ID = 0;

    int64_t start_ts = -1;
    int64_t fragment_instance_id = -1;
    std::uintptr_t driver = 0;
    int64_t id = DEFAULT_EVENT_ID; // used for async event.
    EventBuffer* event_buffer = nullptr;

    void reset() {
        start_ts = -1;
        fragment_instance_id = -1;
        driver = 0;
        id = DEFAULT_EVENT_ID;
        event_buffer = nullptr;
    }
};

inline thread_local QueryTraceContext tls_trace_ctx;

#define INTERNAL_CREATE_EVENT_WITH_CTX(name, category, phase, ctx) \
    starrocks::debug::QueryTraceEvent::create_with_ctx(name, category, ctx.DEFAULT_EVENT_ID, phase, ctx)

#define INTERNAL_CREATE_ASYNC_EVENT_WITH_CTX(name, category, id, phase, ctx) \
    starrocks::debug::QueryTraceEvent::create_with_ctx(name, category, id, phase, ctx)

#define INTERNAL_ADD_EVENT_INTO_THREAD_LOCAL_BUFFER(event) \
    INTERNAL_ADD_EVENT_INFO_BUFFER(starrocks::debug::tls_trace_ctx.event_buffer, event)

#define INTERNAL_ADD_EVENT_INFO_BUFFER(buffer, event) \
    do {                                              \
        if (buffer) {                                 \
            buffer->add(event);                       \
        }                                             \
    } while (0);
} // namespace starrocks::debug
