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

#include "exec/data_sink.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class BlackHoleTableSink final : public DataSink {
public:
    explicit BlackHoleTableSink(ObjectPool* pool) : _pool(pool){};
    ~BlackHoleTableSink() override = default;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override { return Status::OK(); }

<<<<<<< HEAD:be/src/runtime/blackhole_table_sink.h
    RuntimeProfile* profile() override { return _profile; }
=======
// Whether to enable the BE `/api/_stop_be` HTTP endpoint. When `false`, requests
// to that endpoint are rejected with HTTP 403 and the BE process is not exited.
// This config is static and requires a BE restart to take effect.
CONF_Bool(enable_stop_be_action, "true");

// Whether `/api/_stop_be` requires HTTP Basic Auth credentials that are then
// validated against the FE (password + NODE privilege on SYSTEM). Default
// `false` to preserve historical behavior of accepting unauthenticated shutdown
// requests; set to `true` to require FE-validated authentication. This config
// is static and requires a BE restart to take effect.
CONF_Bool(enable_stop_be_action_fe_auth, "false");

// to forward compatibility, will be removed later
CONF_mBool(enable_token_check, "true");
>>>>>>> 82ccf15b17 ([BugFix] Add enable_stop_be_action config to control /api/_stop_be (#73499)):be/src/common/config_http_fwd.h

private:
    ObjectPool* _pool;
    RuntimeProfile* _profile = nullptr;
};
} // namespace starrocks
