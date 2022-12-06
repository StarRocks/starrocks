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

#include "util/debug/query_trace_impl.h"
// A lightweight tool for analyzing query performance under the pipeline engine.
// The basic background and usage can refer to the description of https://github.com/StarRocks/starrocks/pull/7649
// How to add trace point?
// Currently only duration events and async events are supported.
// An example of adding duration event is as follows
// void doSomething() {
//     SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_trace, fragment_instance_id, driver_ptr);
//     QUERY_TRACE_BEGIN("category", "name");
//     process_function();
//     QUERY_TRACE_END("category", "name");
// }
// or just use QUERY_TRACE_SCOPED
// void process_function () {
//     QUERY_TRACE_SCOPED("category", "name");
//     do something
// }
// void doSomething() {
//     SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_trace, fragment_instance_id, driver_ptr);
//     process_function();
// }
// It should be noted that these QUERY_TRACE_* macros rely on thread local trace context to read some information,
// remember to set it before use.
// The async event is a bit different, It may record the start and finish in different threads.
// Each event needs to be identified with a different id, so you need to pass a context when using it.
// In many cases, you can directly use the pointer address of an object as id, because it must be unique within the same process.
// An example is as follows
// Thread 1
// void doSomethingAsync() {
//     starrocks::debug::QueryTraceContext ctx;
//     init ctx
//     QUERY_TRACE_ASYNC_START("category", "name", ctx);
//     async_process(ctx);
// }
// Thread 2
// void async_process(ctx) {
//    do something...
//    QUERY_TRACE_ASYNC_FINISH("category", "name", ctx);
// }

#ifdef ENABLE_QUERY_DEBUG_TRACE
#define SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_trace, fragment_instance_id, driver_ptr) \
    starrocks::debug::QueryTrace::set_tls_trace_context(query_trace, fragment_instance_id,  \
                                                        reinterpret_cast<std::uintptr_t>(driver))

#define QUERY_TRACE_BEGIN(name, category)        \
    INTERNAL_ADD_EVENT_INTO_THREAD_LOCAL_BUFFER( \
            INTERNAL_CREATE_EVENT_WITH_CTX(name, category, 'B', starrocks::debug::tls_trace_ctx))

#define QUERY_TRACE_END(name, category)          \
    INTERNAL_ADD_EVENT_INTO_THREAD_LOCAL_BUFFER( \
            INTERNAL_CREATE_EVENT_WITH_CTX(name, category, 'E', starrocks::debug::tls_trace_ctx))

#define QUERY_TRACE_SCOPED(name, category) starrocks::debug::ScopedTracer _scoped_tracer(name, category)

#define QUERY_TRACE_ASYNC_START(name, category, ctx)                                                            \
    do {                                                                                                        \
        INTERNAL_ADD_EVENT_INFO_BUFFER(ctx.event_buffer,                                                        \
                                       INTERNAL_CREATE_ASYNC_EVENT_WITH_CTX(name, category, ctx.id, 'b', ctx)); \
    } while (0);

#define QUERY_TRACE_ASYNC_FINISH(name, category, ctx)                                                           \
    do {                                                                                                        \
        INTERNAL_ADD_EVENT_INFO_BUFFER(ctx.event_buffer,                                                        \
                                       INTERNAL_CREATE_ASYNC_EVENT_WITH_CTX(name, category, ctx.id, 'e', ctx)); \
    } while (0);
#else
#define SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_trace, fragment_instance_id, driver_ptr)
#define QUERY_TRACE_BEGIN(name, category)
#define QUERY_TRACE_END(name, category)
#define QUERY_TRACE_SCOPED(name, category)
#define QUERY_TRACE_ASYNC_START(name, category, ctx)
#define QUERY_TRACE_ASYNC_FINISH(name, category, ctx)
#endif
