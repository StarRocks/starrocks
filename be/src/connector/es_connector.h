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

#include "column/vectorized_fwd.h"
#include "connector/connector.h"

namespace starrocks {

class EsPredicate;
class ESScanReader;
class ScrollParser;

namespace connector {

class ESConnector final : public Connector {
public:
    ~ESConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::ES; }
};

class ESDataSource;
class ESDataSourceProvider;

class ESDataSourceProvider final : public DataSourceProvider {
public:
    ~ESDataSourceProvider() override = default;
    friend class ESDataSource;
    ESDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const TEsScanNode _es_scan_node;
};

class ESDataSource final : public DataSource {
public:
    ~ESDataSource() override = default;

    ESDataSource(const ESDataSourceProvider* provider, const TScanRange& scan_range);
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

private:
    const ESDataSourceProvider* _provider;
    const TEsScanRange _scan_range;

    // =========================
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _obj_pool;
    ObjectPool* _pool = &_obj_pool;

    std::map<std::string, std::string> _properties;
    std::map<std::string, std::string> _docvalue_context;
    std::map<std::string, std::string> _fields_context;
    std::vector<std::string> _column_names;
    std::string _timezone;

    // predicate index in the conjuncts
    std::vector<int> _predicate_idx;
    // Predicates will push down to ES
    std::vector<EsPredicate*> _predicates;
    bool _no_data = false;
    bool _line_eof = false;
    bool _batch_eof = false;
    int64_t _rows_read_number = 0;
    int64_t _rows_return_number = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;

    ESScanReader* _es_reader = nullptr;
    std::unique_ptr<ScrollParser> _es_scroll_parser;

    RuntimeProfile::Counter* _read_counter = nullptr;
    RuntimeProfile::Counter* _read_timer = nullptr;
    RuntimeProfile::Counter* _materialize_timer = nullptr;
    RuntimeProfile::Counter* _rows_read_counter = nullptr;
    // =========================

    Status _build_conjuncts();
    Status _normalize_conjuncts();
    Status _try_skip_constant_conjuncts();
    Status _create_scanner();
    void _init_counter();
};

} // namespace connector
} // namespace starrocks
