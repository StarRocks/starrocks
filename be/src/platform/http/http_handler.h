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

namespace starrocks {

class HttpRequest;
class HttpChannel;

// Handler for on http request
class HttpHandler {
public:
    virtual ~HttpHandler() = default;
    virtual void handle(HttpRequest* req) = 0;

    virtual bool request_will_be_read_progressively() { return false; }

    // This funciton will called when all headers are recept.
    // return 0 if process successfully. otherwise return -1;
    // If return -1, on_header function should send_reply to HTTP client
    // and function wont send any reply any more.
    virtual int on_header(HttpRequest* req) { return 0; }

    virtual void on_chunk_data(HttpRequest* req) {}
    virtual void free_handler_ctx(void* handler_ctx) {}

    // Whether this handler requires Basic Auth when `config::enable_http_auth` is on.
    // Default true. Internal endpoints (BE-to-BE clone, internal load download,
    // health probe, Prometheus metrics) should override and return false.
    virtual bool need_auth() const { return true; }

    // Additional role/privilege required on top of identity AuthN, evaluated by FE
    // via the `required_privilege` field of the checkAuth RPC. Mirrors the thrift
    // `TPrivilegeRequirement` enum without dragging the thrift header into this base.
    // Default NONE: identity-only auth.
    enum class RequiredPrivilege {
        NONE,
        OPERATE, // user must hold System OPERATE privilege
        NODE,    // user must hold System NODE privilege
    };
    virtual RequiredPrivilege required_privilege() const { return RequiredPrivilege::NONE; }
};

} // namespace starrocks
