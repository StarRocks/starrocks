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

#ifdef __APPLE__

#include <event2/http.h>
#include <event2/http_struct.h>

#ifndef EVHTTP_REQUEST_NEEDS_SHIM
#define EVHTTP_REQUEST_NEEDS_SHIM

static inline void* evhttp_request_get_user_data(struct evhttp_request* req) {
    return req->on_complete_cb_arg;
}

static inline void evhttp_request_set_user_data(struct evhttp_request* req, void* data) {
    req->on_complete_cb_arg = data;
}

#endif

#ifndef evhttp_set_newreqcb
static inline void evhttp_set_newreqcb(struct evhttp* http, int (*cb)(struct evhttp_request*, void*), void* arg) {
    (void)http;
    (void)cb;
    (void)arg;
}
#endif

#ifndef evhttp_request_set_on_free_cb
static inline void evhttp_request_set_on_free_cb(struct evhttp_request* req, void (*cb)(struct evhttp_request*, void*),
                                                 void* arg) {
    evhttp_request_set_on_complete_cb(req, cb, arg);
}
#endif

#endif
