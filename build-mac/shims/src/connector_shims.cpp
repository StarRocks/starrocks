// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "connector/connector.h"

namespace starrocks::connector {

// Define connector name constants to satisfy references
const std::string Connector::HIVE = "hive";
const std::string Connector::ES = "es";
const std::string Connector::JDBC = "jdbc";
const std::string Connector::MYSQL = "mysql";
const std::string Connector::FILE = "file";
const std::string Connector::LAKE = "lake";
const std::string Connector::BINLOG = "binlog";
const std::string Connector::ICEBERG = "iceberg";

// Minimal ConnectorManager that returns no connectors on macOS
ConnectorManager* ConnectorManager::default_instance() {
    static ConnectorManager instance;
    return &instance;
}

const Connector* ConnectorManager::get(const std::string& /*name*/) { return nullptr; }
void ConnectorManager::put(const std::string& /*name*/, std::unique_ptr<Connector> /*connector*/) {}

// DataSource implementations (from connector_core_shim.cpp)
const std::string DataSource::PROFILE_NAME = "DataSource";

void DataSource::update_has_any_predicate() {
    _has_any_predicate = (!_conjunct_ctxs.empty()) || (_runtime_filters != nullptr && _runtime_filters->size() > 0);
}

Status DataSource::parse_runtime_filters(RuntimeState*) { return Status::OK(); }

void DataSource::update_profile(const Profile&) {}

} // namespace starrocks::connector
