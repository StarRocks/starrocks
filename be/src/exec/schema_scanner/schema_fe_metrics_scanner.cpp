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

#include "exec/schema_scanner/schema_fe_metrics_scanner.h"

#include <simdjson.h>

#include "exec/schema_scanner/schema_helper.h"
#include "gen_cpp/FrontendService.h"
#include "gutil/strings/substitute.h"
#include "runtime/client_cache.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaFeMetricsScanner::_s_columns[] = {
        {"FE_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"LABELS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"VALUE", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
};

SchemaFeMetricsScanner::SchemaFeMetricsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaFeMetricsScanner::~SchemaFeMetricsScanner() = default;

Status SchemaFeMetricsScanner::_get_fe_metrics(RuntimeState* state) {
    int32_t timeout = state->query_options().query_timeout * 1000 / 2;
    for (const TFrontend& frontend : _param->frontends) {
        // Defensive guard: the FE planner always sets rpc_port (Config.rpc_port); a missing or
        // non-positive port only happens under FE/BE version skew. Skip such a frontend with a
        // warning rather than connecting to port 0 or failing the whole fe_metrics query.
        if (!frontend.__isset.rpc_port || frontend.rpc_port <= 0) {
            LOG(WARNING) << "skip fe_metrics for frontend " << frontend.id << " (" << frontend.ip
                         << "): missing/invalid rpc_port";
            continue;
        }
        // Fetch each FE's metrics over the FrontendService Thrift RPC. This replaces the
        // previous HTTP scrape of /metrics?type=json, so fe_metrics works regardless of
        // `enable_http_auth` (the RPC needs no HTTP Basic credentials). It takes no arguments —
        // FE process metrics are global, with no per-object RBAC to authorize.
        TFeMetricsResult result;
        auto rpc_cb = [&result](FrontendServiceConnection& client) { client->getFeMetrics(result); };
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(frontend.ip, frontend.rpc_port, rpc_cb, timeout,
                                                                    2 /* retry times */));
        if (result.__isset.status) {
            RETURN_IF_ERROR(Status(result.status));
        }
        if (!result.__isset.json_metrics) {
            return Status::InternalError("fe metrics response missing json_metrics from " + frontend.ip);
        }
        const std::string& metrics = result.json_metrics;
        VLOG(2) << "metrics: " << metrics;

        simdjson::ondemand::parser parser;
        simdjson::padded_string json_metrics(metrics);
        try {
            for (auto json_metric : parser.iterate(json_metrics)) {
                auto& info = _infos.emplace_back();
                info.id = frontend.id;
                info.value = static_cast<int64_t>(double(json_metric["value"]));
                auto n = std::string_view(json_metric["tags"]["metric"]);
                info.name = std::string(n.begin(), n.end());
                std::ostringstream oss;
                oss << simdjson::to_json_string(json_metric["tags"]);
                info.labels = oss.str();
                VLOG(2) << "id: " << info.id << "name: " << info.name << ", labels: " << info.labels
                        << ", value: " << info.value;
            }
        } catch (const simdjson::simdjson_error& e) {
            return Status::InternalError("Parse the result of fe metrics failed: " + std::string(e.what()));
        }
    }

    return Status::OK();
}

Status SchemaFeMetricsScanner::start(RuntimeState* state) {
    _infos.clear();
    _cur_idx = 0;
    RETURN_IF_ERROR(_get_fe_metrics(state));
    return Status::OK();
}

Status SchemaFeMetricsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 4) {
                return Status::InternalError(strings::Substitute("invalid slot id:$0", slot_id));
            }
            auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // fe name
                Slice v(info.id);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
                break;
            }
            case 2: {
                // name
                Slice v(info.name);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
                break;
            }
            case 3: {
                // labels
                Slice v(info.labels);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
                break;
            }
            case 4: {
                // value
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.value);
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaFeMetricsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_cur_idx >= _infos.size()) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
