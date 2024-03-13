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
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/metrics_action.h

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

#include <bvar/variable.h>

#include <string>

#include "http/http_handler.h"

namespace starrocks {

class ExecEnv;
class HttpRequest;
class MetricRegistry;

typedef void (*MockFunc)(const std::string&);

class MetricsAction : public HttpHandler {
public:
    explicit MetricsAction(MetricRegistry* metrics) : _metrics(metrics), _mock_func(nullptr) {
        // The option can be removed if PBackendService is final removed.
        _options.black_wildcards = "*_pbackend_service*";
    }
    // for tests
    explicit MetricsAction(MetricRegistry* metrics, MockFunc func) : _metrics(metrics), _mock_func(func) {
        // The option can be removed if PBackendService is final removed.
        _options.black_wildcards = "*_pbackend_service*";
    }
    ~MetricsAction() override = default;

    void handle(HttpRequest* req) override;

private:
    MetricRegistry* _metrics;
    MockFunc _mock_func;
    bvar::DumpOptions _options;
};

} // namespace starrocks
