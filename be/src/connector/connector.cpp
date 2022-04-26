// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "connector/connector.h"

#include "connector/hive_connector.h"

namespace starrocks {
namespace connector {

static ConnectorManager global_default_instance;

const Connector* ConnectorManager::get(const std::string& name) {
    auto it = _connectors.find(name);
    if (it == _connectors.end()) return nullptr;
    return it->second.get();
}

void ConnectorManager::put(const std::string& name, std::unique_ptr<Connector> connector) {
    _connectors.emplace(std::make_pair(name, std::move(connector)));
}

ConnectorManager* ConnectorManager::default_instance() {
    return &global_default_instance;
}

void ConnectorManager::init() {
    ConnectorManager* cm = default_instance();
    cm->put("hive", std::make_unique<HiveConnector>());
}

Status DataSource::open(RuntimeState* state) {
    // TODO: common operations
    RETURN_IF_ERROR(do_open(state));
    return Status::OK();
}

void DataSource::close(RuntimeState* state) {
    // TODO: common operations.
    do_close(state);
}

Status DataSource::get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) {
    // TODO: common operations.
    RETURN_IF_ERROR(do_get_next(state, chunk));
    return Status::OK();
}

} // namespace connector
} // namespace starrocks