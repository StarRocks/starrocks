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

// libevent shims for macOS build
// Purpose: Provide macOS-compatible implementations for libevent API differences

#pragma once

#ifdef __APPLE__

#include <event2/http.h>
#include <event2/http_struct.h>

// On macOS, some libevent APIs are different or missing
// We provide alternative implementations or stubs

// evhttp_request on macOS doesn't have on_free_cb_arg field
// We'll use a different approach for request cleanup
#ifndef EVHTTP_REQUEST_NEEDS_SHIM
#define EVHTTP_REQUEST_NEEDS_SHIM

// Use the evhttp_request's own callback mechanism to store user data
// We'll abuse the on_complete_cb field to store our request pointer
static inline void* evhttp_request_get_user_data(struct evhttp_request* req) {
    // This is a hack - we store the pointer in the cb_arg field
    // The actual callback function is not used on macOS
    return req->on_complete_cb_arg;
}

static inline void evhttp_request_set_user_data(struct evhttp_request* req, void* data) {
    // Store the request pointer in the on_complete_cb_arg field
    req->on_complete_cb_arg = data;
}

#endif // EVHTTP_REQUEST_NEEDS_SHIM

// evhttp_set_newreqcb might not be available on macOS
#ifndef evhttp_set_newreqcb
// Match Linux signature: returns int in the callback
static inline void evhttp_set_newreqcb(struct evhttp* http,
                                       int (*cb)(struct evhttp_request*, void*),
                                       void* arg) {
    // No-op shim when the symbol is missing on macOS
    (void)http;
    (void)cb;
    (void)arg;
}
#endif

// evhttp_request_set_on_free_cb might have different signature
#ifndef evhttp_request_set_on_free_cb
// Match Linux signature and forward to on_complete callback on macOS
static inline void evhttp_request_set_on_free_cb(struct evhttp_request* req,
                                                 void (*cb)(struct evhttp_request*, void*),
                                                 void* arg) {
    // Forward to the available completion callback API
    evhttp_request_set_on_complete_cb(req, cb, arg);
}
#endif

#endif // __APPLE__
