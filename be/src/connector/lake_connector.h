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

#include "connector/connector.h"
#include "exec/olap_scan_prepare.h"
#include "storage/conjunctive_predicates.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/predicate_tree/predicate_tree.hpp"

namespace starrocks::connector {

class LakeConnector final : public Connector {
public:
    ~LakeConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::LAKE; }
};

class LakeDataSourceProvider;

class LakeDataSource final : public DataSource {
public:
    explicit LakeDataSource(const LakeDataSourceProvider* provider, const TScanRange& scan_range);
    ~LakeDataSource() override;

    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override { return _raw_rows_read; }
    int64_t num_rows_read() const override { return _num_rows_read; }
    int64_t num_bytes_read() const override { return _bytes_read; }
    int64_t cpu_time_spent() const override { return _cpu_time_spent_ns; }

    void get_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) override {
        _reader->get_split_tasks(split_tasks);
    }

private:
    Status get_tablet(const TInternalScanRange& scan_range);
    Status init_global_dicts(TabletReaderParams* params);
    Status init_unused_output_columns(const std::vector<std::string>& unused_output_columns);
    Status init_scanner_columns(std::vector<uint32_t>& scanner_columns);
    void decide_chunk_size(bool has_predicate);
    Status init_reader_params(const std::vector<OlapScanRange*>& key_ranges,
                              const std::vector<uint32_t>& scanner_columns, std::vector<uint32_t>& reader_columns);
    Status init_tablet_reader(RuntimeState* state);
    Status build_scan_range(RuntimeState* state);
    void init_counter(RuntimeState* state);
    void update_realtime_counter(Chunk* chunk);
    void update_counter();

    Status init_column_access_paths(Schema* schema);
    Status prune_schema_by_access_paths(Schema* schema);

private:
    const LakeDataSourceProvider* _provider;
    const TInternalScanRange _scan_range;

    Status _status = Status::OK();
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _not_push_down_conjuncts;
    PredicateTree _non_pushdown_pred_tree;
    ConjunctivePredicates _not_push_down_predicates;
    std::vector<uint8_t> _selection;

    ObjectPool _obj_pool;

    RuntimeState* _runtime_state = nullptr;
    const std::vector<SlotDescriptor*>* _slots = nullptr;
    std::vector<std::unique_ptr<OlapScanRange>> _key_ranges;
    std::vector<OlapScanRange*> _scanner_ranges;
    OlapScanConjunctsManager _conjuncts_manager;

    lake::VersionedTablet _tablet;
    TabletSchemaCSPtr _tablet_schema;
    TabletReaderParams _params{};
    std::shared_ptr<lake::TabletReader> _reader;
    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<ChunkIterator> _prj_iter;

    std::unordered_set<uint32_t> _unused_output_column_ids;
    // For release memory.
    using PredicatePtr = std::unique_ptr<ColumnPredicate>;
    std::vector<PredicatePtr> _predicate_free_pool;

    // slot descriptors for each one of |output_columns|.
    std::vector<SlotDescriptor*> _query_slots;

    std::vector<ColumnAccessPathPtr> _column_access_paths;

    // The following are profile meatures
    int64_t _num_rows_read = 0;
    int64_t _raw_rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_spent_ns = 0;

    RuntimeProfile::Counter* _bytes_read_counter = nullptr;
    RuntimeProfile::Counter* _rows_read_counter = nullptr;

    RuntimeProfile::Counter* _expr_filter_timer = nullptr;
    RuntimeProfile::Counter* _create_seg_iter_timer = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompress_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_counter = nullptr;
    RuntimeProfile::Counter* _del_vec_filter_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_timer = nullptr;
    RuntimeProfile::Counter* _chunk_copy_timer = nullptr;
    RuntimeProfile::Counter* _get_delvec_timer = nullptr;
    RuntimeProfile::Counter* _get_delta_column_group_timer = nullptr;
    RuntimeProfile::Counter* _seg_init_timer = nullptr;
    RuntimeProfile::Counter* _column_iterator_init_timer = nullptr;
    RuntimeProfile::Counter* _bitmap_index_iterator_init_timer = nullptr;
    RuntimeProfile::Counter* _zone_map_filter_timer = nullptr;
    RuntimeProfile::Counter* _rows_key_range_filter_timer = nullptr;
    RuntimeProfile::Counter* _rows_key_range_counter = nullptr;
    RuntimeProfile::Counter* _bf_filter_timer = nullptr;
    RuntimeProfile::Counter* _zm_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _seg_zm_filtered_counter = nullptr;
    RuntimeProfile::Counter* _seg_rt_filtered_counter = nullptr;
    RuntimeProfile::Counter* _sk_filtered_counter = nullptr;
    RuntimeProfile::Counter* _rows_after_sk_filtered_counter = nullptr;
    RuntimeProfile::Counter* _block_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_seek_counter = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    RuntimeProfile::Counter* _block_fetch_timer = nullptr;
    RuntimeProfile::Counter* _bi_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bi_filter_timer = nullptr;
    RuntimeProfile::Counter* _pushdown_predicates_counter = nullptr;
    RuntimeProfile::Counter* _rowsets_read_count = nullptr;
    RuntimeProfile::Counter* _segments_read_count = nullptr;
    RuntimeProfile::Counter* _total_columns_data_page_count = nullptr;
    RuntimeProfile::Counter* _read_pk_index_timer = nullptr;

    // Page count
    RuntimeProfile::Counter* _pages_count_memory_counter = nullptr;
    RuntimeProfile::Counter* _pages_count_local_disk_counter = nullptr;
    RuntimeProfile::Counter* _pages_count_remote_counter = nullptr;
    RuntimeProfile::Counter* _pages_count_total_counter = nullptr;
    // Compressed bytes read
    RuntimeProfile::Counter* _compressed_bytes_read_local_disk_counter = nullptr;
    RuntimeProfile::Counter* _compressed_bytes_read_remote_counter = nullptr;
    RuntimeProfile::Counter* _compressed_bytes_read_total_counter = nullptr;
    RuntimeProfile::Counter* _compressed_bytes_read_request_counter = nullptr;
    // IO count
    RuntimeProfile::Counter* _io_count_local_disk_counter = nullptr;
    RuntimeProfile::Counter* _io_count_remote_counter = nullptr;
    RuntimeProfile::Counter* _io_count_total_counter = nullptr;
    RuntimeProfile::Counter* _io_count_request_counter = nullptr;
    // IO time
    RuntimeProfile::Counter* _io_ns_local_disk_timer = nullptr;
    RuntimeProfile::Counter* _io_ns_remote_timer = nullptr;
    RuntimeProfile::Counter* _io_ns_total_timer = nullptr;
    // Prefetch
    RuntimeProfile::Counter* _prefetch_hit_counter = nullptr;
    RuntimeProfile::Counter* _prefetch_wait_finish_timer = nullptr;
    RuntimeProfile::Counter* _prefetch_pending_timer = nullptr;

    RuntimeProfile::Counter* _pushdown_access_paths_counter = nullptr;
    RuntimeProfile::Counter* _access_path_hits_counter = nullptr;
    RuntimeProfile::Counter* _access_path_unhits_counter = nullptr;
};

// ================================

class LakeDataSourceProvider final : public DataSourceProvider {
public:
    friend class LakeDataSource;
    LakeDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    ~LakeDataSourceProvider() override = default;

    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    Status init(ObjectPool* pool, RuntimeState* state) override;
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

    // always enable shared scan for cloud native table
    bool always_shared_scan() const override { return true; }

    StatusOr<pipeline::MorselQueuePtr> convert_scan_range_to_morsel_queue(
            const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
            bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
            size_t num_total_scan_ranges, size_t scan_dop = 0) override;

    // for ut
    void set_lake_tablet_manager(lake::TabletManager* tablet_manager) { _tablet_manager = tablet_manager; }

    // possiable physical distribution optimize of data source
    bool sorted_by_keys_per_tablet() const override {
        return _t_lake_scan_node.__isset.sorted_by_keys_per_tablet && _t_lake_scan_node.sorted_by_keys_per_tablet;
    }
    bool output_chunk_by_bucket() const override {
        return _t_lake_scan_node.__isset.output_chunk_by_bucket && _t_lake_scan_node.output_chunk_by_bucket;
    }
    bool is_asc_hint() const override {
        if (!sorted_by_keys_per_tablet() && _t_lake_scan_node.__isset.output_asc_hint) {
            return _t_lake_scan_node.output_asc_hint;
        }
        return true;
    }
    std::optional<bool> partition_order_hint() const override {
        if (!sorted_by_keys_per_tablet() && _t_lake_scan_node.__isset.partition_order_hint) {
            return _t_lake_scan_node.partition_order_hint;
        }
        return std::nullopt;
    }

protected:
    ConnectorScanNode* _scan_node;
    const TLakeScanNode _t_lake_scan_node;

    // for ut
    lake::TabletManager* _tablet_manager;

private:
    StatusOr<bool> _could_tablet_internal_parallel(const std::vector<TScanRangeParams>& scan_ranges,
                                                   int32_t pipeline_dop, size_t num_total_scan_ranges,
                                                   TTabletInternalParallelMode::type tablet_internal_parallel_mode,
                                                   int64_t* scan_dop, int64_t* splitted_scan_rows) const;
    StatusOr<bool> _could_split_tablet_physically(const std::vector<TScanRangeParams>& scan_ranges) const;
};

} // namespace starrocks::connector
