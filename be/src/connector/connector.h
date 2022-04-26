// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
namespace starrocks {

namespace vectorized {
class ConnectorScanNode;
// class ConnectorSinkNode;
} // namespace vectorized

namespace connector {

class DataSource {
public:
    virtual Status open(RuntimeState* state);
    virtual void close(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, vectorized::ChunkPtr* chunk);

    virtual Status do_open(RuntimeState* state) = 0;
    virtual void do_close(RuntimeState* state) = 0;
    virtual Status do_get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) = 0;
};

using DataSourcePtr = std::unique_ptr<DataSource>;

class DataSourceProvider {
public:
    virtual Status init(RuntimeState* state) = 0;
    // First version we use TScanRange to define scan range
    // Later version we could use user-defined data.
    virtual DataSourcePtr create_data_source(const TScanRange& scan_range) = 0;
    // virtual DataSourcePtr create_data_source(const std::string& scan_range_spec) = 0;
};

// class DataSinkProvider;

class Connector {
public:
    // First version we use TPlanNode to construct data source provider.
    // Later version we could use user-defined data.

    virtual DataSourceProvider* create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                            const TPlanNode& plan_node) = 0;

    // virtual DataSourceProvider* create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
    //                                                         const std::string& table_handle);
    // virtual DataSinkProvider* create_data_sink_provider(vectorized::ConnectorSinkNode* sink_node,
    //                                                     const std::string& table_handle);
};

class ConnectorManager {
public:
    static void init();
    static ConnectorManager* default_instance();
    const Connector* get(const std::string& name);
    void put(const std::string& name, std::unique_ptr<Connector> connector);

private:
    std::unordered_map<std::string, std::unique_ptr<Connector>> _connectors;
};

} // namespace connector
} // namespace starrocks