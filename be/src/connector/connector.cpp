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

#include "connector/connector.h"

#include "connector/binlog_connector.h"
#include "connector/es_connector.h"
#include "connector/file_connector.h"
#include "connector/hive_connector.h"
#include "connector/iceberg_connector.h"
#include "connector/jdbc_connector.h"
#include "connector/lake_connector.h"
#include "connector/mysql_connector.h"

namespace starrocks::connector {

static ConnectorManager _global_default_instance;

const Connector* ConnectorManager::get(const std::string& name) {
    auto it = _connectors.find(name);
    if (it == _connectors.end()) return nullptr;
    return it->second.get();
}

void ConnectorManager::put(const std::string& name, std::unique_ptr<Connector> connector) {
    _connectors.emplace(name, std::move(connector));
}

ConnectorManager* ConnectorManager::default_instance() {
    return &_global_default_instance;
}

const std::string Connector::HIVE = "hive";
const std::string Connector::ES = "es";
const std::string Connector::JDBC = "jdbc";
const std::string Connector::MYSQL = "mysql";
const std::string Connector::FILE = "file";
const std::string Connector::LAKE = "lake";
const std::string Connector::BINLOG = "binlog";
const std::string Connector::ICEBERG = "iceberg";

class ConnectorManagerInit {
public:
    ConnectorManagerInit() {
        ConnectorManager* cm = ConnectorManager::default_instance();
        cm->put(Connector::HIVE, std::make_unique<HiveConnector>());
        cm->put(Connector::ES, std::make_unique<ESConnector>());
        cm->put(Connector::JDBC, std::make_unique<JDBCConnector>());
        cm->put(Connector::MYSQL, std::make_unique<MySQLConnector>());
        cm->put(Connector::FILE, std::make_unique<FileConnector>());
        cm->put(Connector::LAKE, std::make_unique<LakeConnector>());
        cm->put(Connector::BINLOG, std::make_unique<BinlogConnector>());
        cm->put(Connector::ICEBERG, std::make_unique<IcebergConnector>());
    }
};

static ConnectorManagerInit _init;

const std::string DataSource::PROFILE_NAME = "DataSource";

void DataSource::update_has_any_predicate() {
    auto f = [&]() {
        if (_conjunct_ctxs.size() > 0) return true;
        if (_runtime_filters != nullptr && _runtime_filters->size() > 0) return true;
        return false;
    };
    _has_any_predicate = f();
    return;
}

Status DataSource::parse_runtime_filters(RuntimeState* state) {
    if (_runtime_filters == nullptr || _runtime_filters->size() == 0) return Status::OK();
    for (const auto& item : _runtime_filters->descriptors()) {
        RuntimeFilterProbeDescriptor* probe = item.second;
        DCHECK(runtime_bloom_filter_eval_context.driver_sequence != -1);
        const RuntimeFilter* filter = probe->runtime_filter(runtime_bloom_filter_eval_context.driver_sequence);
        if (filter == nullptr) continue;
        SlotId slot_id;
        if (!probe->is_probe_slot_ref(&slot_id)) continue;
        LogicalType slot_type = probe->probe_expr_type();
        Expr* min_max_predicate = nullptr;
        RuntimeFilterHelper::create_min_max_value_predicate(state->obj_pool(), slot_id, slot_type, filter,
                                                            &min_max_predicate);
        if (min_max_predicate != nullptr) {
            ExprContext* ctx = state->obj_pool()->add(new ExprContext(min_max_predicate));
            RETURN_IF_ERROR(ctx->prepare(state));
            RETURN_IF_ERROR(ctx->open(state));
            _conjunct_ctxs.insert(_conjunct_ctxs.begin(), ctx);
        }
    }
    return Status::OK();
}

void DataSource::update_profile(const Profile& profile) {
    RuntimeProfile::Counter* mem_alloc_failed_counter = ADD_COUNTER(_runtime_profile, "MemAllocFailed", TUnit::UNIT);
    mem_alloc_failed_counter->update(profile.mem_alloc_failed_count);
}

StatusOr<pipeline::MorselQueuePtr> DataSourceProvider::convert_scan_range_to_morsel_queue(
        const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
        bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
        size_t num_total_scan_ranges, size_t scan_parallelism) {
    peek_scan_ranges(scan_ranges);

    pipeline::Morsels morsels;
    bool has_more_morsel = false;
    pipeline::ScanMorsel::build_scan_morsels(node_id, scan_ranges, accept_empty_scan_ranges(), &morsels,
                                             &has_more_morsel);

    if (partition_order_hint().has_value()) {
        bool asc = partition_order_hint().value();
        std::stable_sort(morsels.begin(), morsels.end(), [asc](auto& l, auto& r) {
            auto l_partition_id = down_cast<pipeline::ScanMorsel*>(l.get())->partition_id();
            auto r_partition_id = down_cast<pipeline::ScanMorsel*>(r.get())->partition_id();
            if (asc) {
                return std::less()(l_partition_id, r_partition_id);
            } else {
                return std::greater()(l_partition_id, r_partition_id);
            }
        });
    }

    if (output_chunk_by_bucket()) {
        std::stable_sort(morsels.begin(), morsels.end(), [](auto& l, auto& r) {
            return down_cast<pipeline::ScanMorsel*>(l.get())->owner_id() <
                   down_cast<pipeline::ScanMorsel*>(r.get())->owner_id();
        });
    }

    auto morsel_queue = std::make_unique<pipeline::DynamicMorselQueue>(std::move(morsels), has_more_morsel);
    if (scan_parallelism > 0) {
        morsel_queue->set_max_degree_of_parallelism(scan_parallelism);
    }
    return morsel_queue;
}

} // namespace starrocks::connector
