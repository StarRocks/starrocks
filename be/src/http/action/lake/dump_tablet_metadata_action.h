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

#include <atomic>
#include <cstdint>

#include "gutil/macros.h"
#include "platform/http/http_handler.h"

namespace starrocks {
class ExecEnv;
class HttpRequest;
} // namespace starrocks

namespace starrocks::lake {

class TabletManager;

/**
 * Returns one exact TabletMetadataPB from this compute node's in-memory metadata cache.
 *
 * The tablet id comes from the request path and the metadata version comes from the `version` query parameter. A
 * cache miss does not fall back to StarOS, object storage, the FE, or another compute node. To inspect metadata stored
 * in object storage, download the object with the AWS CLI and parse it with meta_tool.
 *
 * Example request:
 *   GET /api/cloudnative/dump_tablet_metadata/12345?version=2
 *
 * Abbreviated success response (other TabletMetadataPB fields are omitted):
 * {
 *   "status": "OK",
 *   "message": "",
 *   "metadata": {
 *     "id": 12345,
 *     "version": 2,
 *     "schema": {"id": 10001},
 *     "rowsets": [{"id": 1, "num_rows": 10, "data_size": 1024}]
 *   }
 * }
 *
 * Encryption metadata is redacted before serialization. Per-request memory, JSON response size, and concurrency are
 * bounded by the lake_dump_tablet_metadata_* configurations. Access requires the OPERATE privilege.
 */
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
    std::atomic<int32_t> _active_requests{0};
};

} // namespace starrocks::lake
