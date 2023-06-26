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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/http_service.h

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

#include <memory>

#include "common/status.h"
#include "util/concurrent_limiter.h"

namespace starrocks {

class ExecEnv;
class EvHttpServer;
class HttpHandler;
class WebPageHandler;

// HTTP service for StarRocks BE
class HttpServiceBE {
public:
    HttpServiceBE(ExecEnv* env, int port, int num_threads);
    ~HttpServiceBE();

    Status start();

private:
    ExecEnv* _env;

    std::unique_ptr<EvHttpServer> _ev_http_server;
    std::unique_ptr<WebPageHandler> _web_page_handler;

    std::vector<HttpHandler*> _http_handlers;

    std::unique_ptr<ConcurrentLimiter> _http_concurrent_limiter;
};

} // namespace starrocks
