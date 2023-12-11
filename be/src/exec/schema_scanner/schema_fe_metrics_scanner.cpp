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

#include "agent/master_info.h"
#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaFeMetricsScanner::_s_columns[] = {
        {"FE_ID", TYPE_VARCHAR, sizeof(StringValue), false},
        {"NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LABELS", TYPE_VARCHAR, sizeof(StringValue), false},
        {"VALUE", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaFeMetricsScanner::SchemaFeMetricsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaFeMetricsScanner::~SchemaFeMetricsScanner() = default;

Status SchemaFeMetricsScanner::_get_fe_metrics(RuntimeState* state) {
    for (TFrontend frontend : _param->frontends) {
        std::string metrics;
        std::string url = "http://" + frontend.ip + ":" + SimpleItoa(frontend.http_port) + "/metrics?type=json";
        auto timeout = state->query_options().query_timeout * 1000 / 2;
        auto mmetrics_cb = [&url, &metrics, &timeout](HttpClient* client) {
            RETURN_IF_ERROR(client->init(url));
            client->set_timeout_ms(timeout);
            RETURN_IF_ERROR(client->execute(&metrics));
            return Status::OK();
        };
        RETURN_IF_ERROR(HttpClient::execute_with_retry(2 /* retry times */, 1 /* sleep interval */, mmetrics_cb));
        VLOG(1) << "metrics: " << metrics;

        simdjson::ondemand::parser parser;
        simdjson::padded_string json_metrics(metrics);
        for (auto json_metric : parser.iterate(json_metrics)) {
            auto& info = _infos.emplace_back();
            info.id = frontend.id;
            info.value = static_cast<int64_t>(double(json_metric["value"]));
            auto n = std::string_view(json_metric["tags"]["metric"]);
            info.name = std::string(n.begin(), n.end());
            std::ostringstream oss;
            oss << simdjson::to_json_string(json_metric["tags"]);
            info.labels = oss.str();
            VLOG(1) << "id: " << info.id << "name: " << info.name << ", labels: " << info.labels
                    << ", value: " << info.value;
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
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // fe name
                Slice v(info.id);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 2: {
                // name
                Slice v(info.name);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 3: {
                // labels
                Slice v(info.labels);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 4: {
                // value
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.value);
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
