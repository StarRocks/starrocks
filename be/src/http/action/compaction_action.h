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
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/compaction_action.h

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

#include <atomic>

#include "common/status.h"
#include "http/http_handler.h"

namespace starrocks {

enum CompactionActionType { SHOW_INFO = 1, RUN_COMPACTION = 2, SHOW_REPAIR = 3, SUBMIT_REPAIR = 4 };

// This action is used for viewing the compaction status.
// See compaction-action.md for details.
class CompactionAction : public HttpHandler {
public:
    explicit CompactionAction(CompactionActionType type) : _type(type) {}

    ~CompactionAction() override = default;

    void handle(HttpRequest* req) override;

private:
    Status _handle_show_compaction(HttpRequest* req, std::string* json_result);
    Status _handle_compaction(HttpRequest* req, std::string* json_result);
    Status _handle_show_repairs(HttpRequest* req, std::string* json_result);
    Status _handle_submit_repairs(HttpRequest* req, std::string* json_result);

private:
    CompactionActionType _type;
    static std::atomic_bool _running;
};

} // end namespace starrocks
