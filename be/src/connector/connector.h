// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class ExprContext;
namespace vectorized {
class ConnectorScanNode;
class RuntimeFilterProbeCollector;
} // namespace vectorized

namespace connector {

class DataSource {
public:
    virtual ~DataSource() = default;
    virtual Status init() { return Status::OK(); }
    virtual Status open(RuntimeState* state) { return Status::OK(); }
    virtual void close(RuntimeState* state) {}
    virtual Status get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) { return Status::OK(); }

    virtual int64_t raw_rows_read() const { return 0; }
    virtual int64_t num_rows_read() const { return 0; }

    void set_runtime_profile(RuntimeProfile* runtime_profile) { _runtime_profile = runtime_profile; }
    void set_predicates(const std::vector<ExprContext*>& predicates) { _conjunct_ctxs = predicates; }
    void set_runtime_filters(const vectorized::RuntimeFilterProbeCollector* runtime_filters) {
        _runtime_filters = runtime_filters;
    }
    void set_read_limit(const uint64_t limit) { _read_limit = limit; }

protected:
    int64_t _read_limit = -1; // no limit
    std::vector<ExprContext*> _conjunct_ctxs;
    const vectorized::RuntimeFilterProbeCollector* _runtime_filters;
    RuntimeProfile* _runtime_profile;
};

using DataSourcePtr = std::unique_ptr<DataSource>;

class DataSourceProvider {
public:
    virtual ~DataSourceProvider() = default;

    // First version we use TScanRange to define scan range
    // Later version we could use user-defined data.
    virtual DataSourcePtr create_data_source(const TScanRange& scan_range) = 0;
    // virtual DataSourcePtr create_data_source(const std::string& scan_range_spec)  = 0;

    // non-pipeline APIs
    Status prepare(RuntimeState* state) { return Status::OK(); }
    Status open(RuntimeState* state) { return Status::OK(); }
    void close(RuntimeState* state) {}
};
using DataSourceProviderPtr = std::unique_ptr<DataSourceProvider>;

class Connector {
public:
    // supported connectors.
    static const std::string HIVE;

    virtual ~Connector() = default;
    // First version we use TPlanNode to construct data source provider.
    // Later version we could use user-defined data.

    virtual DataSourceProviderPtr create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                              const TPlanNode& plan_node) const = 0;

    // virtual DataSourceProviderPtr create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
    //                                                         const std::string& table_handle) const;
};

class ConnectorManager {
public:
    static ConnectorManager* default_instance();
    const Connector* get(const std::string& name);
    void put(const std::string& name, std::unique_ptr<Connector> connector);

private:
    std::unordered_map<std::string, std::unique_ptr<Connector>> _connectors;
};

} // namespace connector
} // namespace starrocks