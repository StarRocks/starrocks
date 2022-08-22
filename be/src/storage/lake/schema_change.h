// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/statusor.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/lake/tablet.h"
#include "storage/schema_change_utils.h"

namespace starrocks::lake {

class SchemaChangeHandler {
public:
    SchemaChangeHandler() = default;
    ~SchemaChangeHandler() = default;

    Status process_alter_tablet(const TAlterTabletReqV2& request);

private:
    struct SchemaChangeParams {
        Tablet* base_tablet;
        Tablet* new_tablet;
        int64_t version;
        vectorized::MaterializedViewParamMap materialized_params_map;
        bool sc_sorting = false;
        bool sc_directly = false;
        std::unique_ptr<vectorized::ChunkChanger> chunk_changer = nullptr;
    };

    Status do_process_alter_tablet(const TAlterTabletReqV2& request);
    Status convert_historical_rowsets(const SchemaChangeParams& sc_params, TxnLogPB_OpSchemaChange* op_schema_change);
};

} // namespace starrocks::lake
