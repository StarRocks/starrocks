#pragma once

#include "util/debug/query_trace_impl.h"

#ifdef ENABLE_QUERY_DEBUG_TRACE
#define SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_trace, fragment_instance_id, driver_ptr) \
    starrocks::debug::QueryTrace::set_tls_trace_context(query_trace, fragment_instance_id,  \
                                                        reinterpret_cast<std::uintptr_t>(driver))

#define QUERY_TRACE_BEGIN(category, name)        \
    INTERNAL_ADD_EVENT_INTO_THREAD_LOCAL_BUFFER( \
            INTERNAL_CREATE_EVENT_WITH_CTX(category, name, 'B', starrocks::debug::tls_trace_ctx))

#define QUERY_TRACE_END(category, name)          \
    INTERNAL_ADD_EVENT_INTO_THREAD_LOCAL_BUFFER( \
            INTERNAL_CREATE_EVENT_WITH_CTX(category, name, 'E', starrocks::debug::tls_trace_ctx))

#define QUERY_TRACE_SCOPED(category, name) starrocks::debug::ScopedTracer _scoped_tracer(category, name)

#define QUERY_TRACE_ASYNC_START(category, name, ctx)                                                            \
    do {                                                                                                        \
        INTERNAL_ADD_EVENT_INFO_BUFFER(ctx.event_buffer,                                                        \
                                       INTERNAL_CREATE_ASYNC_EVENT_WITH_CTX(category, name, ctx.id, 'b', ctx)); \
    } while (0);

#define QUERY_TRACE_ASYNC_FINISH(category, name, ctx)                                                           \
    do {                                                                                                        \
        INTERNAL_ADD_EVENT_INFO_BUFFER(ctx.event_buffer,                                                        \
                                       INTERNAL_CREATE_ASYNC_EVENT_WITH_CTX(category, name, ctx.id, 'e', ctx)); \
    } while (0);
#else
#define SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_trace, fragment_instance_id, driver_ptr)
#define QUERY_TRACE_BEGIN(category, name)
#define QUERY_TRACE_END(category, name)
#define QUERY_TRACE_SCOPED(category, name)
#define QUERY_TRACE_ASYNC_START(category, name, ctx)
#define QUERY_TRACE_ASYNC_FINISH(category, name, ctx)
#endif