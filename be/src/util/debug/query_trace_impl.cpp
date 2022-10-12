// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <filesystem>
#include <fstream>
#include <mutex>

#include "common/config.h"
#include "exec/pipeline/pipeline_driver.h"
#include "fmt/printf.h"
#include "io/fd_output_stream.h" // write trace to file
#include "util/debug/query_trace.h"
#include "util/time.h"

namespace starrocks {
namespace debug {

QueryTraceEvent QueryTraceEvent::create(const std::string& name, const std::string& category, int64_t id, char phase,
                                        int64_t timestamp, int64_t duration, int64_t instance_id, std::uintptr_t driver,
                                        std::vector<std::pair<std::string, std::string>>&& args) {
    QueryTraceEvent event;
    event.name = name;
    event.category = category;
    event.id = id;
    event.phase = phase;
    event.timestamp = timestamp;
    event.duration = duration;
    event.instance_id = instance_id;
    event.driver = driver;
    event.args = std::move(args);
    return event;
}

QueryTraceEvent QueryTraceEvent::create_with_ctx(const std::string& name, const std::string& category, int64_t id,
                                                 char phase, const QueryTraceContext& ctx) {
    return create(name, category, id, phase, MonotonicMicros() - ctx.start_ts, -1, ctx.fragment_instance_id, ctx.driver,
                  {});
}

QueryTraceEvent QueryTraceEvent::create_with_ctx(const std::string& name, const std::string& category, int64_t id,
                                                 char phase, int64_t start_ts, int64_t duration,
                                                 const QueryTraceContext& ctx) {
    return create(name, category, id, phase, start_ts, duration, ctx.fragment_instance_id, ctx.driver, {});
}

static const char* kSimpleEventFormat =
        "{\"cat\":\"%s\",\"name\":\"%s\",\"pid\":\"%ld\",\"tid\":\"%ld\",\"id\":\"%ld\",\"ts\":%ld,\"ph\":\"%c\","
        "\"args\":%s}";
static const char* kCompleteEventFormat =
        "{\"cat\":\"%s\",\"name\":\"%s\",\"pid\":\"%ld\",\"tid\":\"%ld\",\"id\":\"%ld\",\"ts\":%ld,\"dur\":%ld,\"ph\":"
        "\"%c\",\"args\":%s}";

std::string QueryTraceEvent::to_string() {
    std::string args_str = args_to_string();
    if (phase == 'X') {
        return fmt::sprintf(kCompleteEventFormat, category.c_str(), name.c_str(), instance_id, (int64_t)driver, id,
                            timestamp, duration, phase, args_str.c_str());
    } else {
        return fmt::sprintf(kSimpleEventFormat, category.c_str(), name.c_str(), instance_id, (int64_t)driver, id,
                            timestamp, phase, args_str.c_str());
    }
}

std::string QueryTraceEvent::args_to_string() {
    if (args.empty()) {
        return "{}";
    }
    std::ostringstream oss;
    oss << "{";
    oss << fmt::sprintf("\"%s\":\"%s\"", args[0].first.c_str(), args[0].second.c_str());
    for (size_t i = 1; i < args.size(); i++) {
        oss << fmt::sprintf(",\"%s\":\"%s\"", args[i].first.c_str(), args[i].second.c_str());
    }
    oss << "}";
    return oss.str();
}

void EventBuffer::add(QueryTraceEvent&& event) {
    std::lock_guard<SpinLock> l(_mutex);
    _buffer.emplace_back(std::move(event));
}

QueryTrace::QueryTrace(const TUniqueId& query_id, bool is_enable) : _query_id(query_id), _is_enable(is_enable) {
#ifdef ENABLE_QUERY_DEBUG_TRACE
    if (_is_enable) {
        _start_ts = MonotonicMicros();
    }
#endif
}

void QueryTrace::register_drivers(TUniqueId fragment_instance_id, starrocks::pipeline::Drivers& drivers) {
#ifdef ENABLE_QUERY_DEBUG_TRACE
    if (!_is_enable) {
        return;
    }
    std::unique_lock l(_mutex);
    auto iter = _fragment_drivers.find(fragment_instance_id);
    if (iter == _fragment_drivers.end()) {
        _fragment_drivers.insert({fragment_instance_id, std::make_shared<std::unordered_set<std::uintptr_t>>()});
        iter = _fragment_drivers.find(fragment_instance_id);
    }
    for (auto& driver : drivers) {
        std::uintptr_t ptr = reinterpret_cast<std::uintptr_t>(driver.get());
        iter->second->insert(ptr);
        _buffers.insert({ptr, std::make_unique<EventBuffer>()});
    }
#endif
}

Status QueryTrace::dump() {
#ifdef ENABLE_QUERY_DEBUG_TRACE
    if (!_is_enable) {
        return Status::OK();
    }
    static const char* kProcessNameMetaEventFormat =
            "{\"name\":\"process_name\",\"ph\":\"M\",\"pid\":\"%ld\",\"args\":{\"name\":\"%s\"}}";
    static const char* kThreadNameMetaEventFormat =
            "{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":\"%ld\",\"tid\":\"%ld\",\"args\":{\"name\":\"%s\"}}";
    try {
        std::filesystem::create_directory(starrocks::config::query_debug_trace_dir);
        std::string file_name =
                fmt::format("{}/{}.json", starrocks::config::query_debug_trace_dir, print_id(_query_id));
        std::ofstream oss(file_name.c_str(), std::ios::out | std::ios::binary);
        oss << "{\"traceEvents\":[";
        bool is_first = true;
        for (auto& [fragment_id, driver_set] : _fragment_drivers) {
            std::string fragment_id_str = print_id(fragment_id);
            oss << (is_first ? "" : ",\n");
            oss << fmt::sprintf(kProcessNameMetaEventFormat, fragment_id.lo, fragment_id_str.c_str());
            is_first = false;
            for (auto& driver : *driver_set) {
                starrocks::pipeline::DriverRawPtr ptr = reinterpret_cast<starrocks::pipeline::DriverRawPtr>(driver);
                oss << (is_first ? "" : ",\n");
                oss << fmt::sprintf(kThreadNameMetaEventFormat, fragment_id.lo, (int64_t)driver,
                                    ptr->get_name().c_str());
            }
        }

        for (auto& [_, buffer_ptr] : _buffers) {
            auto& buffer = buffer_ptr->_buffer;
            for (auto iter : buffer) {
                oss << (is_first ? "" : ",\n");
                oss << iter.to_string();
            }
        }
        oss << "]}";

        oss.close();
    } catch (std::exception& e) {
        return Status::IOError(fmt::format("dump trace log error {}", e.what()));
    }
#endif
    return Status::OK();
}

void QueryTrace::set_tls_trace_context(QueryTrace* query_trace, TUniqueId fragment_instance_id, std::uintptr_t driver) {
#ifdef ENABLE_QUERY_DEBUG_TRACE
    if (!query_trace->_is_enable) {
        tls_trace_ctx.reset();
        return;
    }
    {
        std::shared_lock l(query_trace->_mutex);
        auto iter = query_trace->_buffers.find(driver);
        DCHECK(iter != query_trace->_buffers.end());
        tls_trace_ctx.event_buffer = iter->second.get();
    }
    tls_trace_ctx.start_ts = query_trace->_start_ts;
    tls_trace_ctx.fragment_instance_id = fragment_instance_id.lo;
    tls_trace_ctx.driver = driver;
#endif
}

ScopedTracer::ScopedTracer(const std::string& name, const std::string& category) : _name(name), _category(category) {
    _start_ts = MonotonicMicros();
}

ScopedTracer::~ScopedTracer() {
    if (tls_trace_ctx.event_buffer != nullptr) {
        _duration = MonotonicMicros() - _start_ts;
        tls_trace_ctx.event_buffer->add(QueryTraceEvent::create_with_ctx(
                _name, _category, -1, 'X', _start_ts - tls_trace_ctx.start_ts, _duration, tls_trace_ctx));
    }
}

} // namespace debug
} // namespace starrocks