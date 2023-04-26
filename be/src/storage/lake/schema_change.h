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
#include "storage/lake/tablet.h"
#include "storage/schema_change_utils.h"

namespace starrocks::lake {

class TabletManager;

class SchemaChangeHandler {
public:
    explicit SchemaChangeHandler(TabletManager* tablet_manager) : _tablet_manager(tablet_manager) {}
    ~SchemaChangeHandler() = default;

    Status process_alter_tablet(const TAlterTabletReqV2& request);

    DISALLOW_COPY_AND_MOVE(SchemaChangeHandler);

private:
    struct SchemaChangeParams {
        Tablet* base_tablet;
        Tablet* new_tablet;
        int64_t version;
        MaterializedViewParamMap materialized_params_map;
        bool sc_sorting = false;
        bool sc_directly = false;
        std::unique_ptr<ChunkChanger> chunk_changer = nullptr;
    };

    Status do_process_alter_tablet(const TAlterTabletReqV2& request);
    Status convert_historical_rowsets(const SchemaChangeParams& sc_params, TxnLogPB_OpSchemaChange* op_schema_change);

    TabletManager* _tablet_manager;
};

} // namespace starrocks::lake
