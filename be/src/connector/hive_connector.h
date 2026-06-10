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

#include <unordered_map>

#include "column/column_access_path.h"
#include "column/vectorized_fwd.h"
#include "connector/connector.h"
#include "connector/hive_chunk_sink.h"
#include "exec/connector_scan_node.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"

namespace starrocks {
class HiveTableDescriptor;
}

namespace starrocks::connector {

class HiveConnector final : public Connector {
public:
    ~HiveConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    std::unique_ptr<ConnectorChunkSinkProvider> create_data_sink_provider() const override;

    ConnectorType connector_type() const override { return ConnectorType::HIVE; }
};

class HiveDataSource;
class HiveDataSourceProvider;

class HiveDataSourceProvider final : public DataSourceProvider {
public:
    ~HiveDataSourceProvider() override = default;
    friend class HiveDataSource;
    HiveDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    HiveDataSourceProvider(ConnectorScanNode* scan_node, int32_t plan_node_id, const THdfsScanNode& hdfs_scan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

    void prepare_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;
    void default_data_source_mem_bytes(int64_t* min_value, int64_t* max_value) override;

    friend class HiveDataSource;

protected:
    int32_t _plan_node_id;
    ConnectorScanNode* _scan_node;
    const THdfsScanNode _hdfs_scan_node;
    int64_t _max_file_length = 0;
    mutable std::atomic<int32_t> _lazy_column_coalesce_counter = 0;
};

class HiveDataSource final : public DataSource {
public:
    ~HiveDataSource() override = default;

    HiveDataSource(const HiveDataSourceProvider* provider, const TScanRange& scan_range);
    HiveDataSource(const HiveDataSourceProvider* provider, const THdfsScanRange& hdfs_scan_range);
    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;
    const std::string get_custom_coredump_msg() const override;
    int32_t scan_range_indicate_const_column_index(SlotId id) const;
    int32_t extended_column_index(SlotId id) const;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;
    int64_t io_time_spent() const override;
    int64_t estimated_mem_usage() const override;
    bool can_estimate_mem_usage() const override { return true; }

    void get_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) override;
    Status _init_chunk_if_needed(ChunkPtr* chunk, size_t n) override;

private:
    const HiveDataSourceProvider* _provider;
    THdfsScanRange _scan_range;

    // ============= init func =============
    Status _init_conjunct_ctxs(RuntimeState* state);
    void _update_has_any_predicate();
    Status _decompose_conjunct_ctxs(RuntimeState* state);
    void _init_tuples_and_slots(RuntimeState* state);
    void _init_counter(RuntimeState* state);
    void _init_runtime_filter_counters();
    void _init_global_late_materialization_context(RuntimeState* state);

    Status _init_partition_values();
    Status _init_extended_values();
    Status _init_global_dicts(HdfsScannerContext* ctx);
    Status _init_scanner(RuntimeState* state);
    Status _check_all_slots_nullable();

    // =====================================
    ObjectPool _pool;
    RuntimeState* _runtime_state = nullptr;

    HdfsScanner* _scanner = nullptr;
    HdfsScannerContext _scanner_ctx;

    // Partition-level predicate evaluation — not passed to scanners.
    struct PartitionFilter {
        std::vector<ExprContext*> conjunct_ctxs;
        std::vector<ExprContext*> values;
        bool has_conjuncts = false;
        bool filter_by_eval = false;
    };
    PartitionFilter _partition_filter;

    // ExprContexts for extended column values (iceberg data_seq_num, etc.).
    // Lifetime managed by _pool; passed to HdfsScannerContext before scanner init.
    std::vector<ExprContext*> _extended_column_expr_ctxs;

    // Per-range scan_range_id assigned by the global late materialization context.
    int32_t _scan_range_id = -1;

    bool _no_more_chunks = false;
    const TupleDescriptor* _min_max_tuple_desc = nullptr;
    std::vector<std::string> _hive_column_names;
    const HiveTableDescriptor* _hive_table = nullptr;
    std::vector<ColumnAccessPathPtr> _column_access_paths;
};

} // namespace starrocks::connector
