// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/reload_tablet_action.h

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

#ifndef STARROCKS_BE_SRC_HTTP_RELOAD_TABLET_ACTION_H
#define STARROCKS_BE_SRC_HTTP_RELOAD_TABLET_ACTION_H

#include "gen_cpp/AgentService_types.h"
#include "http/http_handler.h"

namespace starrocks {

class ExecEnv;

class ReloadTabletAction : public HttpHandler {
public:
    ReloadTabletAction(ExecEnv* exec_env);

    ~ReloadTabletAction() override {}

    void handle(HttpRequest* req) override;

private:
    void reload(const std::string& path, int64_t tablet_id, int32_t schema_hash, HttpRequest* req);

    ExecEnv* _exec_env;

}; // end class ReloadTabletAction

} // end namespace starrocks
#endif // STARROCKS_BE_SRC_COMMON_UTIL_DOWNLOAD_ACTION_H
