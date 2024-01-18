// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "connector/connector.h"

#include "connector/es_connector.h"
#include "connector/file_connector.h"
#include "connector/hive_connector.h"
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
    _connectors.emplace(std::make_pair(name, std::move(connector)));
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
    }
};

static ConnectorManagerInit _init;

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
        vectorized::RuntimeFilterProbeDescriptor* probe = item.second;
        const vectorized::JoinRuntimeFilter* filter = probe->runtime_filter();
        if (filter == nullptr) continue;
        SlotId slot_id;
        if (!probe->is_probe_slot_ref(&slot_id)) continue;
        PrimitiveType slot_type = probe->probe_expr_type();
        Expr* min_max_predicate = nullptr;
        vectorized::RuntimeFilterHelper::create_min_max_value_predicate(state->obj_pool(), slot_id, slot_type, filter,
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

} // namespace starrocks::connector