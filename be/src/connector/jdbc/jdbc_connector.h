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

#include <map>
#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "connector_primitive/connector.h"

namespace starrocks {

class JDBCScanner;
struct JDBCScanContext;
class ExprContext;
class TupleDescriptor;

namespace connector {

// What rendering this scan's join runtime filters into remote SQL produced.
struct JDBCRuntimeFilterPushdown {
    // One SQL fragment per pushed-down filter, values already rendered as literals.
    std::vector<std::string> predicates;

    int64_t filter_count = 0;
    int64_t value_count = 0;
    // What was pushed down, comma-separated as "<column>:<value count>", e.g. "c_custkey:37". The
    // totals above say how much travelled; this says which column carried how much of it, which is
    // the part that explains why a filter did or did not pay off. Empty when nothing was pushed.
    std::string pushed_columns;
    // Reason codes for filters that were not pushed down, comma-separated, e.g.
    // "c_name:unsupported_type". Empty when nothing was turned away.
    std::string skip_reasons;
};

// The text a FLOAT or DOUBLE value is rendered as, before it is quoted: fmt's shortest round-trip
// form, with the three non-finite values respelled the way PostgreSQL accepts them.
//
// Declared here so the non-finite spellings can be tested directly. They cannot be reached through
// a runtime filter in a unit test: the in-filter's value set is a phmap flat_hash_set, and
// inserting a NaN into one trips its "constructed value does not match the lookup key" assertion,
// because NaN compares unequal to itself. See the note in jdbc_connector_test.cpp.
std::string jdbc_ieee754_to_java_text(float value);
std::string jdbc_ieee754_to_java_text(double value);

// Renders the join runtime filters among `conjunct_ctxs` into remote predicates for the outermost
// WHERE of the remote SQL, and reports why any of them was left behind.
//
// Declared here rather than kept private to the .cpp so its correctness rules -- NULL handling, the
// superset law, the limit gate -- can be unit-tested without a JVM, a JDBC driver or a remote
// database. `max_values` is the caller's already-resolved ceiling on the size of one IN list.
void build_jdbc_runtime_filter_pushdown(const std::map<SlotId, std::string>& runtime_filter_columns,
                                        bool scan_has_limit, const std::vector<ExprContext*>& conjunct_ctxs,
                                        const TupleDescriptor& tuple_desc, size_t max_values,
                                        JDBCRuntimeFilterPushdown* out);

class JDBCConnector final : public Connector {
public:
    ~JDBCConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::JDBC; }
};

class JDBCDataSource;
class JDBCDataSourceProvider;

class JDBCDataSourceProvider final : public DataSourceProvider {
public:
    ~JDBCDataSourceProvider() override = default;
    friend class JDBCDataSource;
    JDBCDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    // A JDBC scan has no scan ranges, so it runs at dop 1 and would cap the operators above it.
    // Fanning its output out lifts that cap, but PassthroughExchanger round-robins whole chunks
    // across the receiving drivers, which reorders them. When the FE pushed an ORDER BY down and
    // kept no TopN to put the rows back in order, that trade is not available.
    bool insert_local_exchange_operator() const override { return !_jdbc_scan_node.preserve_remote_order; }
    bool accept_empty_scan_ranges() const override { return false; }
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const TJDBCScanNode _jdbc_scan_node;
};

class JDBCDataSource final : public DataSource {
public:
    ~JDBCDataSource() override = default;

    JDBCDataSource(const JDBCDataSourceProvider* provider, const TScanRange& scan_range);
    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

private:
    Status _create_scanner(RuntimeState* state);

    // Renders the join runtime filters this scan carries into remote predicates, values already
    // written into them as literals, and appends them to `filters`. Also records in `scan_ctx`
    // what was pushed and why any filter was left out, so the scan profile can explain a missing
    // pushdown.
    void _append_runtime_filters(RuntimeState* state, JDBCScanContext* scan_ctx, std::vector<std::string>* filters);

    // ====================================
    const JDBCDataSourceProvider* _provider;
    ObjectPool _obj_pool;
    ObjectPool* _pool = &_obj_pool;
    RuntimeState* _runtime_state = nullptr;
    JDBCScanner* _scanner = nullptr;
    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
};

} // namespace connector
} // namespace starrocks
