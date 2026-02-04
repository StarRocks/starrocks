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

#include "storage/lake/table_schema_service.h"

#include <bvar/bvar.h>
#include <fmt/format.h>

#include <algorithm>
#ifdef BE_TEST
#include <array>
#endif

#include "agent/master_info.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/lake_types.pb.h"
#include "runtime/client_cache.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/metadata_util.h"
#include "util/failpoint/fail_point.h"
#include "util/thrift_rpc_helper.h"
#include "util/uid_util.h"

namespace starrocks::lake {

// Number of schema lookups that hit the schema cache (fastest path).
bvar::Adder<int64_t> g_schema_cache_hit("table_schema_service", "schema_cache_hit");
// Number of schema lookups that hit the tablet metadata (fallback when schema cache misses).
bvar::Adder<int64_t> g_tablet_metadata_hit("table_schema_service", "tablet_metadata_hit");
// Number of schema lookups that miss both schema cache and tablet metadata, requiring remote fetch.
// Note: g_schema_cache_hit + g_tablet_metadata_hit + g_local_miss represents total local lookup attempts.
bvar::Adder<int64_t> g_local_miss("table_schema_service", "local_miss");
// Total latency (in microseconds) for remote schema fetches, including all retry attempts.
// This measures the end-to-end time from initiating remote fetch to completion.
bvar::LatencyRecorder g_remote_fetch_latency_us("table_schema_service", "remote_fetch_us");
// Number of retry attempts for remote schema fetches (excludes the initial attempt).
bvar::Adder<int64_t> g_remote_fetch_retries("table_schema_service", "remote_fetch_retries");
// Latency (in microseconds) for individual RPC calls to FE for schema fetching.
// This measures single RPC latency, while g_remote_fetch_latency_us measures total time including retries.
bvar::LatencyRecorder g_schema_rpc_latency_us("table_schema_service", "schema_rpc_us");

// Failpoint to disable remote schema fetching for load operations in tests.
// Previously, tests would fallback to local tablet metadata files when schema is not in schema cache
// and cached latest meta. After introducing remote schema service, these tests would normally need to
// mock thrift RPC calls to fetch schemas, which requires significant effort. As a compromise, this
// failpoint is introduced to maintain compatibility with existing tests.
DEFINE_FAIL_POINT(table_schema_service_disable_remote_schema_for_load);

std::string TableSchemaService::SingleFlightExecutionContext::to_string() const {
    std::stringstream ss;
    ss << "[target_fe: " << target_fe.hostname << ":" << target_fe.port
       << ", source_type: " << ::starrocks::to_string(request_source);
    if (request_source == TTableSchemaRequestSource::LOAD) {
        ss << ", txn_id: " << txn_id;
    } else if (request_source == TTableSchemaRequestSource::SCAN) {
        ss << ", query_id: " << print_id(query_id);
    }
    ss << "]";
    return ss.str();
}

StatusOr<TabletSchemaPtr> TableSchemaService::get_schema_for_load(const TableSchemaKeyPB& schema_key, int64_t tablet_id,
                                                                  int64_t txn_id,
                                                                  const TabletMetadataPtr& tablet_meta) {
    int64_t schema_id = schema_key.schema_id();
    TabletSchemaPtr schema = _get_local_schema(schema_id, tablet_meta);
    if (schema != nullptr) {
        VLOG(2) << "get load schema from local. db_id: " << schema_key.db_id()
                << ", table_id: " << schema_key.table_id() << ", schema_id: " << schema_id
                << ", tablet_id: " << tablet_id << ", txn_id: " << txn_id;
        return schema;
    }

    TTableSchemaKey thrift_schema_key;
    thrift_schema_key.__set_schema_id(schema_id);
    thrift_schema_key.__set_db_id(schema_key.db_id());
    thrift_schema_key.__set_table_id(schema_key.table_id());

    TGetTableSchemaRequest request;
    request.__set_schema_key(thrift_schema_key);
    request.__set_source(TTableSchemaRequestSource::LOAD);
    request.__set_txn_id(txn_id);
    request.__set_tablet_id(tablet_id);

    TNetworkAddress coordinator_fe = get_master_address();
    auto status_or_schema = _get_remote_schema(request, coordinator_fe);

    FAIL_POINT_TRIGGER_EXECUTE(table_schema_service_disable_remote_schema_for_load,
                               { status_or_schema = Status::NotSupported("disable remote schema for testing"); });

    if (status_or_schema.status().is_not_supported()) {
        // If FE doesn't support table schema service which indicates
        // fast schema change v2 does not work, fallback to schema file.
        return _fallback_load_to_schema_file(schema_id, tablet_id);
    }
    return status_or_schema;
}

StatusOr<TabletSchemaPtr> TableSchemaService::get_schema_for_scan(const TableSchemaKeyPB& schema_key, int64_t tablet_id,
                                                                  const TUniqueId& query_id,
                                                                  const TNetworkAddress& coordinator_fe,
                                                                  const TabletMetadataPtr& tablet_meta) {
    int64_t schema_id = schema_key.schema_id();
    TabletSchemaPtr schema = _get_local_schema(schema_id, tablet_meta);
    if (schema != nullptr) {
        VLOG(2) << "get scan schema from local. db_id: " << schema_key.db_id()
                << ", table_id: " << schema_key.table_id() << ", schema_id: " << schema_id
                << ", tablet_id: " << tablet_id << ", query_id: " << print_id(query_id);
        return schema;
    }

    TTableSchemaKey thrift_schema_key;
    thrift_schema_key.__set_schema_id(schema_id);
    thrift_schema_key.__set_db_id(schema_key.db_id());
    thrift_schema_key.__set_table_id(schema_key.table_id());

    TGetTableSchemaRequest request;
    request.__set_schema_key(thrift_schema_key);
    request.__set_source(TTableSchemaRequestSource::SCAN);
    request.__set_tablet_id(tablet_id);
    request.__set_query_id(query_id);

    // SCAN path has check whether FE supports table schema service in LakeDataSource::get_tablet,
    // so Status::is_not_supported should not happen, and not check it
    return _get_remote_schema(request, coordinator_fe);
}

TabletSchemaPtr TableSchemaService::_get_local_schema(int64_t schema_id, const TabletMetadataPtr& tablet_meta) {
    auto schema = _tablet_mgr->get_cached_schema(schema_id);
    if (schema != nullptr) {
        g_schema_cache_hit << 1;
        return schema;
    }

    if (tablet_meta != nullptr) {
        const TabletSchemaPB* schema_pb = nullptr;
        if (schema_id == tablet_meta->schema().id()) {
            schema_pb = &tablet_meta->schema();
        } else {
            auto it = tablet_meta->historical_schemas().find(schema_id);
            if (it != tablet_meta->historical_schemas().end()) {
                schema_pb = &it->second;
            }
        }
        if (schema_pb != nullptr) {
            schema = TabletSchema::create(*schema_pb);
            _tablet_mgr->cache_schema(schema);
            g_tablet_metadata_hit << 1;
            VLOG(2) << "get schema from tablet metadata. schema_id: " << schema_id
                    << ", tablet_id: " << tablet_meta->id() << ", metadata version: " << tablet_meta->version();
            return schema;
        }
    }

    g_local_miss << 1;
    return nullptr;
}

StatusOr<TabletSchemaPtr> TableSchemaService::_get_remote_schema(const TGetTableSchemaRequest& request,
                                                                 const TNetworkAddress& fe) {
    // Deduplication & Reliability Strategy:
    // 1. SingleFlight: Deduplicates concurrent requests for the same schema to the same FE.
    //    Grouping by FE (schema + FE) isolates failures/latency across different FEs, avoiding cross-FE interference
    //    and improving overall availability (one unhealthy FE won't block schema fetches from other FEs).
    // 2. Fail-Fast Conditions: thrift rpc error or table not exist.
    // 3. Interference Handling (Cross-Query/Txn):
    //    - If the "leader" request (the one that actually executed) belonged to the SAME query/txn as us,
    //      and it failed, we accept the failure and stop retrying.
    //    - If the "leader" belonged to a DIFFERENT query/txn, its failure might be specific to that context.
    //      We should retry.
    // 4. Isolation Strategy (Last Retry):
    //    - On the final retry attempt, we switch the grouping strategy to include QueryID or TxnID.
    //    - This ensures we execute a dedicated RPC for our own context, avoiding any interference from others.
    auto start = butil::gettimeofday_us();
    DeferOp defer([&]() { g_remote_fetch_latency_us << (butil::gettimeofday_us() - start); });
    const int max_retries = std::max(1, config::table_schema_service_max_retries);
    SingleFlightResultPtr sf_result;
    GroupStrategy group_strategy = GroupStrategy::SCHEMA_AND_FE;
    for (int attempt = 0; attempt < max_retries; ++attempt) {
        g_remote_fetch_retries << (attempt == 0 ? 0 : 1);
        const std::string group_key = _group_key(group_strategy, request, fe);
        auto t = butil::gettimeofday_us();
        sf_result = _select_single_flight_group(group_key).Do(group_key,
                                                              [&]() { return _fetch_schema_via_rpc(request, fe); });

        auto& rpc_result = sf_result->rpc_result;
        auto& exec_ctx = sf_result->execution_ctx;

        VLOG(2) << "get schema from remote. " << _print_request_info(request) << ", attempt: " << attempt + 1 << "/"
                << max_retries << ", latency: " << (butil::gettimeofday_us() - t)
                << "us, execution_ctx: " << exec_ctx.to_string() << ", status: " << rpc_result.status();

        if (rpc_result.ok()) {
            return rpc_result;
        }

        const auto& status = rpc_result.status();

        if (status.is_thrift_rpc_error()) {
            if (status.message().find("Invalid method name 'getTableSchema'") != std::string::npos) {
                return Status::NotSupported(fmt::format("FE [{}:{}] haven't upgraded to support table schema service.",
                                                        fe.hostname, fe.port));
            }
            break;
        }

        if (status.is_table_not_exist()) {
            break;
        }

        bool is_same_query_or_txn = false;
        if (request.source == TTableSchemaRequestSource::LOAD &&
            exec_ctx.request_source == TTableSchemaRequestSource::LOAD) {
            is_same_query_or_txn = request.txn_id == exec_ctx.txn_id;
        } else if (request.source == TTableSchemaRequestSource::SCAN &&
                   exec_ctx.request_source == TTableSchemaRequestSource::SCAN) {
            is_same_query_or_txn = request.query_id == exec_ctx.query_id;
        }

        if (is_same_query_or_txn) {
            break;
        }

        // The last retry is isolated by query/txn to avoid cross-query/txn interference.
        if (attempt == max_retries - 2) {
            if (request.source == TTableSchemaRequestSource::LOAD) {
                group_strategy = GroupStrategy::SCHEMA_AND_TXN;
            } else if (request.source == TTableSchemaRequestSource::SCAN) {
                group_strategy = GroupStrategy::SCHEMA_AND_QUERY;
            }
        }
    }

    if (sf_result != nullptr) {
        return sf_result->rpc_result;
    }
    // should not reach here normally.
    return Status::InternalError("get schema rpc result is null");
}

TableSchemaService::SingleFlightResultPtr TableSchemaService::_fetch_schema_via_rpc(
        const TGetTableSchemaRequest& request, const TNetworkAddress& fe) {
    // TODO: TBatchGetTableSchemaRequest currently contains only one schema request.
    // We haven't implemented merging different schema ids into a single RPC because requests are already deduplicated
    // by schema id via SingleFlight. Therefore, per FE, the effective RPC concurrency is mainly bounded by table count
    // and QPS, which is not expected to be a bottleneck currently. Keep it simple for now, and only add true batching
    // if/when FE schema RPC becomes a performance hotspot.
    TBatchGetTableSchemaRequest request_batch;
    request_batch.__set_requests(std::vector<TGetTableSchemaRequest>{request});

    TBatchGetTableSchemaResponse response_batch;
    Status status;
    bool mock_thrift_rpc = false;

#ifdef BE_TEST
    // Test hook payload: { request*, response_batch*, status*, mock_thrift_rpc* }
    std::array<void*, 4> test_ctx{(void*)&request, (void*)&response_batch, (void*)&status, (void*)&mock_thrift_rpc};
    TEST_SYNC_POINT_CALLBACK("TableSchemaService::_fetch_schema_via_rpc::test_hook", &test_ctx);
#endif

    int64_t rpc_latency_us = 0;
    if (!mock_thrift_rpc) {
        auto start = butil::gettimeofday_us();
        status = ThriftRpcHelper::rpc<FrontendServiceClient>(
                fe.hostname, fe.port,
                [&request_batch, &response_batch](FrontendServiceConnection& client) {
                    client->getTableSchema(response_batch, request_batch);
                },
                config::thrift_rpc_timeout_ms);
        rpc_latency_us = butil::gettimeofday_us() - start;
    }
    g_schema_rpc_latency_us << rpc_latency_us;

    auto result = std::make_shared<TableSchemaService::SingleFlightResult>();
    result->execution_ctx.target_fe = fe;
    result->execution_ctx.request_source = request.source;
    if (request.source == TTableSchemaRequestSource::SCAN) {
        result->execution_ctx.query_id = request.query_id;
    } else if (request.source == TTableSchemaRequestSource::LOAD) {
        result->execution_ctx.txn_id = request.txn_id;
    }

    auto log_helper = [&](const char* msg, const Status& st = Status::OK()) {
        std::stringstream ss;
        ss << msg << ". " << _print_request_info(request) << ", fe: " << fe.hostname
           << ", rpc latency: " << rpc_latency_us << "us";
        if (!st.ok()) {
            ss << ", error: " << st;
        }
        return ss.str();
    };

    if (!status.ok()) {
        LOG(WARNING) << log_helper("get schema rpc failed", status);
        result->rpc_result = status;
        return result;
    }

    status = Status(response_batch.status);
    if (!status.ok()) {
        LOG(WARNING) << log_helper("batch get schema failed", status);
        result->rpc_result = status;
        return result;
    }

    if (!response_batch.__isset.responses || response_batch.responses.empty()) {
        status = Status::InternalError("response is empty");
        LOG(WARNING) << log_helper("batch get schema failed", status);
        result->rpc_result = status;
        return result;
    }

    auto& response = response_batch.responses[0];
    status = Status(response.status);
    if (!status.ok()) {
        LOG(WARNING) << log_helper("failed to get schema", status);
        result->rpc_result = status;
        return result;
    }

    TabletSchemaPB schema_pb;
    status = convert_t_schema_to_pb_schema(response.schema, &schema_pb);
    if (!status.ok()) {
        LOG(WARNING) << log_helper("failed to get schema", status);
        result->rpc_result =
                Status::InternalError("Failed to convert thrift schema to proto schema: " + status.to_string());
        return result;
    }
    TabletSchemaSPtr schema = TabletSchema::create(schema_pb);
    _tablet_mgr->cache_schema(schema);
    VLOG(2) << log_helper("get schema success");
    result->rpc_result = schema;
    return result;
}

std::string TableSchemaService::_group_key(GroupStrategy strategy, const TGetTableSchemaRequest& request,
                                           const TNetworkAddress& fe) {
    int64_t schema_id = request.schema_key.schema_id;
    switch (strategy) {
    case GroupStrategy::SCHEMA_AND_QUERY:
        return fmt::format("q{}:{}:{}", schema_id, request.query_id.hi, request.query_id.lo);
    case GroupStrategy::SCHEMA_AND_TXN:
        return fmt::format("t{}:{}", schema_id, request.txn_id);
    case GroupStrategy::SCHEMA_AND_FE:
    default:
        return fmt::format("f{}:{}:{}", schema_id, fe.hostname, fe.port);
    }
}

std::string TableSchemaService::_print_request_info(const TGetTableSchemaRequest& request) {
    std::stringstream ss;
    auto& schema_key = request.schema_key;
    ss << "db_id: " << schema_key.db_id << ", table_id: " << schema_key.table_id
       << ", schema_id: " << schema_key.schema_id << ", tablet_id: " << request.tablet_id;
    if (request.source == TTableSchemaRequestSource::LOAD) {
        ss << ", txn_id: " << request.txn_id;
    } else if (request.source == TTableSchemaRequestSource::SCAN) {
        ss << ", query_id: " << print_id(request.query_id);
    }
    return ss.str();
}

StatusOr<TabletSchemaPtr> TableSchemaService::_fallback_load_to_schema_file(int64_t schema_id, int64_t tablet_id) {
    auto result = _tablet_mgr->get_tablet_schema_by_id(tablet_id, schema_id);
    if (result.ok() || !result.status().is_not_found()) {
        VLOG(2) << "get schema from schema file. schema_id: " << schema_id << ", tablet_id: " << tablet_id
                << ", status: " << result.status();
        return result;
    }
    // This is for the compatibility before introducing schema file
    auto tablet_res = _tablet_mgr->get_tablet(tablet_id);
    VLOG(2) << "get schema from tablet, schema_id: " << schema_id << ", tablet_id: " << tablet_id
            << ", status: " << tablet_res.status();
    if (!tablet_res.ok()) {
        return tablet_res.status();
    }
    auto tablet_schema_res = tablet_res->get_schema();
    if (!tablet_schema_res.ok()) {
        return tablet_schema_res.status();
    }
    auto tablet_schema = std::move(tablet_schema_res).value();
    if (tablet_schema->id() != schema_id) {
        return Status::NotFound(
                fmt::format("schema not match, tablet id: {}, tablet schema id/version: {}/{}, expected schema id: {}",
                            tablet_id, tablet_schema->id(), tablet_schema->schema_version(), schema_id));
    }
    return tablet_schema;
}

} // namespace starrocks::lake
