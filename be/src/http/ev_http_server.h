// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <event2/event.h>

#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "http/http_handler.h"
#include "http/http_method.h"
#include "http/http_status.h"
#include "util/path_trie.hpp"

namespace starrocks {

class HttpRequest;

class EvHttpServer {
public:
    // Result returned by an AuthVerifier when authentication or authorization fails.
    // The verifier (injected by the service layer) is responsible for shaping the
    // response body — HttpCore stays format-agnostic.
    struct AuthVerifyFailure {
        HttpStatus http_status = HttpStatus::UNAUTHORIZED;
        // Value for the response's `WWW-Authenticate` header. Empty means the server
        // will not add the header (e.g. for 403 where the header is semantically wrong).
        std::string www_authenticate;
        // Pre-serialized response body sent to the client as-is (Content-Type: application/json).
        std::string body;
    };

    // Callback used to verify Basic Auth credentials (plus an optional role/privilege
    // requirement) carried by an HTTP request. Returning std::nullopt means OK and the
    // server proceeds to dispatch to the handler; returning a populated AuthVerifyFailure
    // causes the server to short-circuit with that response. Service layer injects an
    // implementation that talks to FE.
    using AuthVerifier = std::function<std::optional<AuthVerifyFailure>(HttpRequest*, HttpHandler::RequiredPrivilege)>;

    EvHttpServer(int port, int num_workers = 1);
    EvHttpServer(std::string host, int port, int num_workers = 1);
    ~EvHttpServer();

    // register handler for an a path-method pair
    bool register_handler(const HttpMethod& method, const std::string& path, HttpHandler* handler);

    void register_static_file_handler(HttpHandler* handler);

    // Wire a Basic-Auth verifier. When set, EvHttpServer invokes it before dispatching
    // any request whose handler reports `need_auth() == true`.
    // Must be called BEFORE `start()`; not thread-safe to change after worker threads
    // begin reading `_auth_verifier`.
    void set_auth_verifier(AuthVerifier verifier) {
        DCHECK(!_started) << "set_auth_verifier must be called before start()";
        _auth_verifier = std::move(verifier);
    }

    Status start();
    void stop();
    void join();

    // callback
    int on_header(struct evhttp_request* ev_req);

    // get real port
    int get_real_port() { return _real_port; }

private:
    Status _bind();
    HttpHandler* _find_handler(HttpRequest* req);

private:
    // input param
    std::string _host;
    int _port;
    int _num_workers;
    // used for unittest, set port to 0, os will choose a free port;
    int _real_port;

    int _server_fd = -1;
    std::vector<std::thread> _workers;

    pthread_rwlock_t _rw_lock;

    PathTrie<HttpHandler*> _get_handlers;
    HttpHandler* _static_file_handler = nullptr;
    PathTrie<HttpHandler*> _put_handlers;
    PathTrie<HttpHandler*> _post_handlers;
    PathTrie<HttpHandler*> _delete_handlers;
    PathTrie<HttpHandler*> _head_handlers;
    PathTrie<HttpHandler*> _options_handlers;
    std::vector<struct event_base*> _event_bases;
    std::vector<struct evhttp*> _https;
    AuthVerifier _auth_verifier;
    // Set true once `start()` begins; gates `set_auth_verifier` so the verifier
    // can't be swapped after worker threads start reading it.
    bool _started = false;
};

} // namespace starrocks
