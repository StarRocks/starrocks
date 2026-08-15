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

#include "base/concurrency/concurrent_limiter.h"
#include "gutil/macros.h"
#include "platform/http/http_handler.h"

namespace starrocks {
class ExecEnv;
class HttpRequest;
} // namespace starrocks

namespace starrocks::lake {

class TabletManager;

class DumpTabletMetadataAction : public HttpHandler {
public:
    explicit DumpTabletMetadataAction(ExecEnv*, TabletManager* tablet_manager = nullptr)
            : _tablet_manager(tablet_manager) {}
    ~DumpTabletMetadataAction() override = default;

    DISALLOW_COPY_AND_MOVE(DumpTabletMetadataAction);

    int on_header(HttpRequest* req) override;
    void handle(HttpRequest* req) override;
    void free_handler_ctx(void* handler_ctx) override;

    RequiredPrivilege required_privilege() const override { return RequiredPrivilege::OPERATE; }

private:
    TabletManager* _tablet_manager;
    ConcurrentLimiter _limiter{1};
};

} // namespace starrocks::lake
