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

#include "connector/benchmark_connector.h"
#include "connector/cache_stats_connector.h"
#include "connector/connector_registry.h"
#include "connector/es_connector.h"
#include "connector/file_connector.h"
#include "connector/hive_connector.h"
#ifndef __APPLE__
#include "connector/iceberg_connector.h"
#endif
#include "connector/jdbc_connector.h"
#include "connector/lake_connector.h"
#include "connector/mysql_connector.h"

namespace starrocks::connector {

class ConnectorRegistryInit {
public:
    ConnectorRegistryInit() {
        ConnectorRegistry* registry = ConnectorRegistry::default_instance();
        registry->put(Connector::HIVE, std::make_unique<HiveConnector>());
        registry->put(Connector::ES, std::make_unique<ESConnector>());
        registry->put(Connector::JDBC, std::make_unique<JDBCConnector>());
        registry->put(Connector::MYSQL, std::make_unique<MySQLConnector>());
        registry->put(Connector::BENCHMARK, std::make_unique<BenchmarkConnector>());
        registry->put(Connector::CACHE_STATS, std::make_unique<CacheStatsConnector>());
        registry->put(Connector::FILE, std::make_unique<FileConnector>());
        registry->put(Connector::LAKE, std::make_unique<LakeConnector>());
#ifndef __APPLE__
        registry->put(Connector::ICEBERG, std::make_unique<IcebergConnector>());
#endif
    }
};

static ConnectorRegistryInit _init;

} // namespace starrocks::connector
