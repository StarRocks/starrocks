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
#include "connector/builtin_connector_registry.h"
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

namespace {

template <typename ConnectorT>
void install_if_absent(ConnectorRegistry* registry, const std::string& name) {
    if (registry->get(name) == nullptr) {
        registry->put(name, std::make_unique<ConnectorT>());
    }
}

} // namespace

Status install_builtin_connectors(ConnectorRegistry* registry) {
    DCHECK(registry != nullptr);
    install_if_absent<HiveConnector>(registry, Connector::HIVE);
    install_if_absent<ESConnector>(registry, Connector::ES);
    install_if_absent<JDBCConnector>(registry, Connector::JDBC);
    install_if_absent<MySQLConnector>(registry, Connector::MYSQL);
    install_if_absent<BenchmarkConnector>(registry, Connector::BENCHMARK);
    install_if_absent<CacheStatsConnector>(registry, Connector::CACHE_STATS);
    install_if_absent<FileConnector>(registry, Connector::FILE);
    install_if_absent<LakeConnector>(registry, Connector::LAKE);
#ifndef __APPLE__
    install_if_absent<IcebergConnector>(registry, Connector::ICEBERG);
#endif
    return Status::OK();
}

} // namespace starrocks::connector
