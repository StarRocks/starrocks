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

#include "gen_cpp/StarrocksExternalService_types.h"

namespace starrocks {

class ExecEnv;

namespace orchestration {

class ExternalScanContextMgr;

class ExternalScanOrchestrator {
public:
    ExternalScanOrchestrator(ExecEnv* exec_env, ExternalScanContextMgr* context_mgr);

    void open_scanner(TScanOpenResult& result, const TScanOpenParams& params);
    void get_next(TScanBatchResult& result, const TScanNextBatchParams& params);
    void close_scanner(TScanCloseResult& result, const TScanCloseParams& params);

private:
    ExecEnv* _exec_env;
    ExternalScanContextMgr* _context_mgr;
};

} // namespace orchestration
} // namespace starrocks
