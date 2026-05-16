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

#include "connector/connector_chunk_sink.h"
#include "connector/data_source.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class ConnectorScanNode;

namespace connector {

enum ConnectorType {
    HIVE = 0,
    ES = 1,
    JDBC = 2,
    MYSQL = 3,
    FILE = 4,
    LAKE = 5,
    BINLOG = 6,
    ICEBERG = 7,
    BENCHMARK = 8,
    CACHE_STATS = 9,
};

class Connector {
public:
    // supported connectors.
    static const std::string HIVE;
    static const std::string ES;
    static const std::string JDBC;
    static const std::string MYSQL;
    static const std::string FILE;
    static const std::string LAKE;
    static const std::string BINLOG;
    static const std::string ICEBERG;
    static const std::string BENCHMARK;
    static const std::string CACHE_STATS;

    virtual ~Connector() = default;
    // First version we use TPlanNode to construct data source provider.
    // Later version we could use user-defined data.

    virtual DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                              const TPlanNode& plan_node) const {
        CHECK(false) << connector_type() << " connector does not implement chunk source yet";
        __builtin_unreachable();
    }

    // virtual DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
    //                                                         const std::string& table_handle) const;

    virtual std::unique_ptr<ConnectorChunkSinkProvider> create_data_sink_provider() const {
        CHECK(false) << connector_type() << " connector does not implement chunk sink yet";
        __builtin_unreachable();
    }

    virtual std::unique_ptr<ConnectorChunkSinkProvider> create_delete_sink_provider() const {
        CHECK(false) << connector_type() << " connector does not implement chunk sink yet";
        __builtin_unreachable();
    }

    virtual std::unique_ptr<ConnectorChunkSinkProvider> create_row_delta_sink_provider() const { return nullptr; }

    virtual ConnectorType connector_type() const = 0;
};

} // namespace connector
} // namespace starrocks
