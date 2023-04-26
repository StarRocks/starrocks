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
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/update_config_action.h

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

#include <functional>
#include <mutex>
#include <unordered_map>

#include "http/http_handler.h"
#include "runtime/exec_env.h"

namespace starrocks {

// Update BE config.
class UpdateConfigAction : public HttpHandler {
public:
    explicit UpdateConfigAction(ExecEnv* exec_env) : _exec_env(exec_env) { _instance.store(this); }
    ~UpdateConfigAction() override = default;

    void handle(HttpRequest* req) override;

    Status update_config(const std::string& name, const std::string& value);

    // hack to share update_config method with be_configs schema table sink
    static UpdateConfigAction* instance() { return _instance.load(); }

private:
    static std::atomic<UpdateConfigAction*> _instance;
    ExecEnv* _exec_env;
    std::once_flag _once_flag;
    std::unordered_map<std::string, std::function<void()>> _config_callback;
};

} // namespace starrocks
