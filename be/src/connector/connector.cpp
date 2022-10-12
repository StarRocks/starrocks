// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "connector/connector.h"

#include "connector/es_connector.h"
#include "connector/file_connector.h"
#include "connector/hive_connector.h"
#include "connector/jdbc_connector.h"
#include "connector/lake_connector.h"
#include "connector/mysql_connector.h"

namespace starrocks {
namespace connector {

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

} // namespace connector
} // namespace starrocks
