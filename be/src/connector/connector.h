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

#pragma once

#include <memory>
#include <string>

#include "connector/connector_registry.h"
#include "connector/connector_sink_provider.h"
#include "connector/connector_type.h"
#include "connector/data_source_provider.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class ConnectorScanNode;

namespace connector {

class Connector {
public:
    // supported connectors.
    inline static const std::string HIVE = "hive";
    inline static const std::string ES = "es";
    inline static const std::string JDBC = "jdbc";
    inline static const std::string MYSQL = "mysql";
    inline static const std::string FILE = "file";
    inline static const std::string LAKE = "lake";
    inline static const std::string BINLOG = "binlog";
    inline static const std::string ICEBERG = "iceberg";
    inline static const std::string BENCHMARK = "benchmark";
    inline static const std::string CACHE_STATS = "cache_stats";

    virtual ~Connector() = default;

    virtual DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                              const TPlanNode& plan_node) const;

    virtual std::unique_ptr<ConnectorChunkSinkProvider> create_data_sink_provider() const;

    virtual std::unique_ptr<ConnectorChunkSinkProvider> create_delete_sink_provider() const;

    virtual std::unique_ptr<ConnectorChunkSinkProvider> create_row_delta_sink_provider() const;

    virtual ConnectorType connector_type() const = 0;
};

} // namespace connector
} // namespace starrocks
