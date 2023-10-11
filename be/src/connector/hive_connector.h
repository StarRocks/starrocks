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
#include "exec/connector_scan_node.h"
#include "exec/hdfs_scanner.h"

namespace starrocks::connector {

class HiveConnector final : public Connector {
public:
    ~HiveConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::HIVE; }
};

class HiveDataSource;
class HiveDataSourceProvider;

class HiveDataSourceProvider final : public DataSourceProvider {
public:
    ~HiveDataSourceProvider() override = default;
    friend class HiveDataSource;
    HiveDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const THdfsScanNode _hdfs_scan_node;
};

class HiveDataSource final : public DataSource {
public:
    ~HiveDataSource() override = default;

    HiveDataSource(const HiveDataSourceProvider* provider, const TScanRange& scan_range);
    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;
    const std::string get_custom_coredump_msg() const override;
    std::atomic<int32_t>* get_lazy_column_coalesce_counter() {
        return _provider->_scan_node->get_lazy_column_coalesce_counter();
    }

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;
    int64_t io_time_spent() const override;
    int64_t estimated_mem_usage() const override;

private:
    const HiveDataSourceProvider* _provider;
    const THdfsScanRange _scan_range;

    // ============= init func =============
    Status _init_conjunct_ctxs(RuntimeState* state);
    void _update_has_any_predicate();
    Status _decompose_conjunct_ctxs(RuntimeState* state);
    void _init_tuples_and_slots(RuntimeState* state);
    void _init_counter(RuntimeState* state);

    Status _init_partition_values();
    Status _init_scanner(RuntimeState* state);
    HdfsScanner* _create_hudi_jni_scanner();
    HdfsScanner* _create_paimon_jni_scanner(FSOptions& options);
    Status _check_all_slots_nullable();

    // =====================================
    ObjectPool _pool;
    RuntimeState* _runtime_state = nullptr;
    HdfsScanner* _scanner = nullptr;
    bool _use_datacache = false;
    bool _enable_populate_datacache = false;

    // ============ conjuncts =================
    std::vector<ExprContext*> _min_max_conjunct_ctxs;

    // complex conjuncts, such as contains multi slot, are evaled in scanner.
    std::vector<ExprContext*> _scanner_conjunct_ctxs;
    // conjuncts that contains only one slot.
    // 1. conjuncts that column is not exist in file, are used to filter file in file reader.
    // 2. conjuncts that column is materialized, are evaled in group reader.
    std::unordered_map<SlotId, std::vector<ExprContext*>> _conjunct_ctxs_by_slot;
    std::unordered_set<SlotId> _slots_in_conjunct;

    // used for reader to decide decode or not
    // if only used by filter(not output) and only used in conjunct_ctx_by_slot
    // there is no need to decode.
    std::unordered_set<SlotId> _slots_of_mutli_slot_conjunct;

    // partition conjuncts of each partition slot.
    std::vector<ExprContext*> _partition_conjunct_ctxs;
    std::vector<ExprContext*> _partition_values;
    bool _has_partition_conjuncts = false;
    bool _filter_by_eval_partition_conjuncts = false;
    bool _no_data = false;

    int _min_max_tuple_id = 0;
    TupleDescriptor* _min_max_tuple_desc = nullptr;

    // materialized columns.
    std::vector<SlotDescriptor*> _materialize_slots;
    std::vector<int> _materialize_index_in_chunk;

    // partition columns.
    std::vector<SlotDescriptor*> _partition_slots;

    // partition column index in `tuple_desc`
    std::vector<int> _partition_index_in_chunk;
    // partition index in hdfs partition columns
    std::vector<int> _partition_index_in_hdfs_partition_columns;
    bool _has_partition_columns = false;

    std::vector<std::string> _hive_column_names;
    bool _case_sensitive = false;
    bool _can_use_any_column = false;
    bool _can_use_min_max_count_opt = false;
    const HiveTableDescriptor* _hive_table = nullptr;

    // ======================================
    // The following are profile metrics
    HdfsScanProfile _profile;
};

} // namespace starrocks::connector
