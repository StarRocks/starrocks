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

#include "common/statusor.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/schema_change_utils.h"

namespace starrocks {
class TxnLogPB_OpSchemaChange;
}

namespace starrocks::lake {

class TabletManager;
class VersionedTablet;
struct SchemaChangeParams;

class SchemaChangeHandler {
public:
    explicit SchemaChangeHandler(TabletManager* tablet_manager) : _tablet_manager(tablet_manager) {}
    ~SchemaChangeHandler() = default;

    Status process_alter_tablet(const TAlterTabletReqV2& request);

    Status process_update_tablet_meta(const TUpdateTabletMetaInfoReq& request);

    DISALLOW_COPY_AND_MOVE(SchemaChangeHandler);

private:
    Status do_process_alter_tablet(const TAlterTabletReqV2& request);
    Status convert_historical_rowsets(const SchemaChangeParams& sc_params, TxnLogPB_OpSchemaChange* op_schema_change);

    // ADD INDEX fast path (lake-only). Skips data rewrite: builds standalone
    // .idx files (Index Delta Group) per segment and emits an OpAddIndex
    // TxnLog. Per-segment work runs in parallel on
    // _thread_pool_lake_schema_change. Caller is expected to have already
    // validated `request.only_add_index` and the index list.
    Status do_process_add_index_only(const TAlterTabletReqV2& request);

    // DROP INDEX fast path (lake-only). Pure metadata mutation: writes an
    // OpDropIndex TxnLog whose tombstones are merged into existing IDG
    // entries at publish time. Physical .idx cleanup happens at compaction.
    Status do_process_drop_index_only(const TAlterTabletReqV2& request);

    Status do_process_update_tablet_meta(const TTabletMetaInfo& request, int64_t txn_id);

    TabletManager* _tablet_manager;
};

} // namespace starrocks::lake
