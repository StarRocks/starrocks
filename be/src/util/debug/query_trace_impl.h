// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

namespace starrocks {
namespace debug {

class QueryTraceContext;

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
    TUniqueId _query_id;
    bool _is_enable = false;
    int64_t _start_ts = -1;

    std::shared_mutex _mutex;
    std::unordered_map<std::uintptr_t, std::unique_ptr<EventBuffer>> _buffers;

    // fragment_instance_id => driver list, it will be used to generate meta event
    std::unordered_map<TUniqueId, std::shared_ptr<std::unordered_set<std::uintptr_t>>> _fragment_drivers;
};

class ScopedTracer {
public:
    ScopedTracer(const std::string& name, const std::string& category);
    ~ScopedTracer();

private:
    std::string _name;
    std::string _category;
    int64_t _start_ts;
    int64_t _duration = -1;
};

struct QueryTraceContext {
    int64_t start_ts = -1;
    int64_t fragment_instance_id = -1;
    std::uintptr_t driver = 0;
    int64_t id = -1; // used for async event
    EventBuffer* event_buffer = nullptr;

    void reset() {
        start_ts = -1;
        fragment_instance_id = -1;
        driver = 0;
        id = -1;
        event_buffer = nullptr;
    }
};

inline thread_local QueryTraceContext tls_trace_ctx;

#define INTERNAL_CREATE_EVENT_WITH_CTX(name, category, phase, ctx) \
    starrocks::debug::QueryTraceEvent::create_with_ctx(name, category, -1, phase, ctx)

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
} // namespace debug
} // namespace starrocks
