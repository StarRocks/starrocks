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

#include "orchestration/query_orchestrator.h"

#include <fmt/format.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "base/url_coding.h"
#include "common/compiler_util.h"
#include "common/config_network_fwd.h"
#include "common/config_runtime_fwd.h"
#include "common/constexpr.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/system/backend_options.h"
#include "common/util/thrift_util.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Planner_types.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "orchestration/fragment_executor.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "types/logical_type.h"

namespace starrocks::orchestration {

QueryOrchestrator::QueryOrchestrator(ExecEnv* exec_env) : _exec_env(exec_env) {
    DCHECK(_exec_env != nullptr);
}

/*
 * 1. resolve opaqued_query_plan to thrift structure
 * 2. build TExecPlanFragmentParams
 */
Status QueryOrchestrator::exec_external_plan_fragment(const TScanOpenParams& params,
                                                      const TUniqueId& fragment_instance_id,
                                                      std::vector<TScanColumnDesc>* selected_columns,
                                                      TUniqueId* query_id) {
    // check chunk size first
    if (UNLIKELY(!params.__isset.batch_size)) {
        return Status::InvalidArgument("batch_size is not set");
    }
    auto batch_size = params.batch_size;
    if (UNLIKELY(batch_size <= 0 || batch_size > MAX_CHUNK_SIZE)) {
        return Status::InvalidArgument(
                fmt::format("batch_size is out of range, it must be in the range (0, {}], current value is [{}]",
                            MAX_CHUNK_SIZE, batch_size));
    }
    const std::string& opaqued_query_plan = params.opaqued_query_plan;
    std::string query_plan_info;
    // base64 decode query plan
    if (!base64_decode(opaqued_query_plan, &query_plan_info)) {
        LOG(WARNING) << "open context error: base64_decode decode opaqued_query_plan failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " validate error, should not be modified after returned StarRocks FE processed";
        return Status::InvalidArgument(msg.str());
    }
    TQueryPlanInfo t_query_plan_info;
    const auto* buf = (const uint8_t*)query_plan_info.data();
    uint32_t len = query_plan_info.size();
    // deserialize TQueryPlanInfo
    auto st = deserialize_thrift_msg(buf, &len, TProtocolType::BINARY, &t_query_plan_info);
    if (!st.ok()) {
        LOG(WARNING) << "open context error: deserialize TQueryPlanInfo failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " deserialize error, should not be modified after returned StarRocks FE processed";
        return Status::InvalidArgument(msg.str());
    }

    VLOG_ROW << "BackendService execute open() TQueryPlanInfo: "
             << apache::thrift::ThriftDebugString(t_query_plan_info);

    *query_id = t_query_plan_info.query_id;

    // set up desc tbl
    DescriptorTbl* desc_tbl = nullptr;
    ObjectPool obj_pool;
    st = DescriptorTbl::create(nullptr, &obj_pool, t_query_plan_info.desc_tbl, &desc_tbl, params.batch_size);
    if (!st.ok()) {
        LOG(WARNING) << "open context error: extract DescriptorTbl failure";
        std::stringstream msg;
        msg << "query_plan_info: " << query_plan_info
            << " create DescriptorTbl error, should not be modified after returned StarRocks FE "
               "processed";
        return Status::InvalidArgument(msg.str());
    }

    const auto& output_names = t_query_plan_info.output_names;
    int i = 0;
    for (const auto& expr : t_query_plan_info.plan_fragment.output_exprs) {
        const auto& nodes = expr.nodes;
        if (nodes.empty() || nodes[0].node_type != TExprNodeType::SLOT_REF) {
            LOG(WARNING) << "output expr is not slot ref";
            return Status::InvalidArgument("output expr is not slot ref");
        }
        const auto& slot_ref = nodes[0].slot_ref;
        auto* tuple_desc = desc_tbl->get_tuple_descriptor(slot_ref.tuple_id);
        if (tuple_desc == nullptr) {
            LOG(WARNING) << "tuple descriptor is null. id: " << slot_ref.tuple_id;
            return Status::InvalidArgument("tuple descriptor is null");
        }
        auto* slot_desc = desc_tbl->get_slot_descriptor(slot_ref.slot_id);
        if (slot_desc == nullptr) {
            LOG(WARNING) << "slot descriptor is null. id: " << slot_ref.slot_id;
            return Status::InvalidArgument("slot descriptor is null");
        }

        TScanColumnDesc col;
        if (!output_names.empty()) {
            col.__set_name(output_names[i]);
        } else {
            col.__set_name(std::string(slot_desc->col_name()));
        }
        col.__set_type(to_thrift(slot_desc->type().type));
        selected_columns->emplace_back(std::move(col));
        i++;
    }

    // assign the param used to execute PlanFragment
    TExecPlanFragmentParams exec_fragment_params;
    exec_fragment_params.protocol_version = (InternalServiceVersion::type)0;
    exec_fragment_params.__set_fragment(t_query_plan_info.plan_fragment);
    exec_fragment_params.__set_desc_tbl(t_query_plan_info.desc_tbl);
    exec_fragment_params.__set_backend_num(1);
    exec_fragment_params.__set_pipeline_dop(1);

    // assign the param used for executing of PlanFragment-self
    TPlanFragmentExecParams fragment_exec_params;
    fragment_exec_params.query_id = t_query_plan_info.query_id;
    fragment_exec_params.fragment_instance_id = fragment_instance_id;
    std::map<TPlanNodeId, std::vector<TScanRangeParams>> per_node_scan_ranges;
    std::vector<TScanRangeParams> scan_ranges;
    TNetworkAddress address;
    address.hostname = BackendOptions::get_localhost();
    address.port = config::be_port;
    std::map<int64_t, TTabletVersionInfo> tablet_info = t_query_plan_info.tablet_info;
    for (auto tablet_id : params.tablet_ids) {
        TInternalScanRange scan_range;
        scan_range.db_name = params.database;
        scan_range.table_name = params.table;
        auto iter = tablet_info.find(tablet_id);
        if (iter != tablet_info.end()) {
            TTabletVersionInfo info = iter->second;
            scan_range.tablet_id = tablet_id;
            scan_range.version = std::to_string(info.version);
            scan_range.schema_hash = std::to_string(info.schema_hash);
            scan_range.hosts.push_back(address);
        } else {
            std::stringstream msg;
            msg << "tablet_id: " << tablet_id << " not found";
            LOG(WARNING) << "tablet_id [ " << tablet_id << " ] not found";
            return Status::NotFound(msg.str());
        }
        TScanRange starrocks_scan_range;
        starrocks_scan_range.__set_internal_scan_range(scan_range);
        TScanRangeParams scan_range_params;
        scan_range_params.scan_range = starrocks_scan_range;
        scan_ranges.push_back(scan_range_params);
    }
    per_node_scan_ranges.insert(std::make_pair((TPlanNodeId)0, scan_ranges));
    fragment_exec_params.per_node_scan_ranges = per_node_scan_ranges;
    // set a mock sender id
    fragment_exec_params.__set_sender_id(0);
    fragment_exec_params.__set_instances_number(1);
    exec_fragment_params.__set_params(fragment_exec_params);
    // batch_size for one RowBatch
    TQueryOptions query_options;
    query_options.batch_size = params.batch_size;
    query_options.query_timeout = params.query_timeout;
    query_options.mem_limit = params.mem_limit;
    query_options.query_type = TQueryType::EXTERNAL;
    // For spark sql / flink sql, we dont use page cache.
    query_options.use_page_cache = false;
    query_options.enable_profile = config::enable_profile_for_external_plan;
    exec_fragment_params.__set_query_options(query_options);
    VLOG_ROW << "external exec_plan_fragment params is "
             << apache::thrift::ThriftDebugString(exec_fragment_params).c_str();
    FragmentExecutor fragment_executor;
    auto status = fragment_executor.prepare(_exec_env, exec_fragment_params, exec_fragment_params);
    if (status.ok()) {
        return fragment_executor.execute(_exec_env);
    }
    return status.is_duplicate_rpc_invocation() ? Status::OK() : status;
}

} // namespace starrocks::orchestration
