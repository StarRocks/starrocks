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
#include <vector>

#include "connector/connector.h"
#include "exec/olap_scan_prepare.h"
#include "storage/conjunctive_predicates.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/predicate_tree/predicate_tree.hpp"

namespace starrocks {
class TabletSchema;
class TabletMetadataPB;
class SlotDescriptor;
namespace lake {
class TabletManager;
}
} // namespace starrocks

namespace starrocks::connector {

// Check if all PK columns are in the selected slots. Used by CACHE SELECT to
// decide whether to warm up persistent index SST files.
bool has_all_pk_columns_selected(const TabletSchema* tablet_schema, const std::vector<SlotDescriptor*>& slots);

// Warm up persistent index SST files for cloud-native PK tables.
Status warmup_pk_index_sst_files(const TabletMetadataPB* metadata, lake::TabletManager* tablet_mgr);

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
    bool can_reuse_with(const pipeline::ScanMorsel& morsel) const override;
    Status reuse(RuntimeState* state, pipeline::ScanMorsel* morsel) override;
    void release_for_reuse(RuntimeState* state) override;

    int64_t raw_rows_read() const override { return _raw_rows_read; }
    int64_t num_rows_read() const override { return _num_rows_read; }
    int64_t num_bytes_read() const override { return _bytes_read; }
    int64_t cpu_time_spent() const override { return _cpu_time_spent_ns; }

    void get_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) override;

    // parse_runtime_filters is used to generate min-max predicates from runtime filters, while LakeDataSource already
    // generates predicates by ScanConjunctsManager, so skip parse_runtime_filters to make the parse logic is consistent
    // to the share-nothing mode.
    Status parse_runtime_filters(RuntimeState* state) override { return Status::OK(); }

    TabletSchemaCSPtr TEST_tablet_schema() const { return _tablet_schema; }

private:
    struct LateRuntimeFilterReinitDecision {
        enum class Action {
            NONE = 0,
            FULL_REINIT = 1,
            REFRESH_RUNTIME_RANGE_PRUNER = 2,
        };

        enum class Reason {
            NONE = 0,
            FILTER_SET_CHANGED = 1,
            NEWLY_ARRIVED = 2,
            VERSION_CHANGED = 3,
        };

        bool triggered = false;
        Action action = Action::NONE;
        Reason reason = Reason::NONE;
        int32_t filter_id = -1;
    };

    Status get_tablet(const TInternalScanRange& scan_range);
    Status init_global_dicts(TabletReaderParams* params);
    Status init_unused_output_columns(const std::vector<std::string>& unused_output_columns);
    Status init_scanner_columns(std::vector<uint32_t>& scanner_columns, std::vector<uint32_t>& reader_columns);
    void decide_chunk_size(bool has_predicate);
    Status init_reader_params(const std::vector<OlapScanRange*>& key_ranges);
    Status init_tablet_reader(RuntimeState* state);
    Status build_scan_range(RuntimeState* state);
    void init_counter(RuntimeState* state);
    void update_realtime_counter(Chunk* chunk);
    void update_counter(RuntimeState* state);
    void update_counter(RuntimeState* state, const OlapReaderStatistics& stats);
    bool enable_global_late_materialization() const;
    void refresh_glm_context() const;
    bool enable_local_child_morsel_reuse() const;
    bool enable_local_child_prepared_state_reuse() const;
    bool should_attach_prepared_read_state() const;
    bool can_reuse_current_morsel(const pipeline::ScanMorsel& morsel) const;
    bool can_reuse_with_signature(const pipeline::ScanMorsel& morsel) const;
    bool has_reuse_blocker() const;
    bool can_fast_reopen_current_morsel() const;
    Status open_reader_for_current_morsel();
    Status fast_reopen_reader_for_current_morsel();
    Status reinit_reader_for_current_morsel_with_runtime_filters();
    void observe_adaptive_split_task(const pipeline::LakeSplitContext* split_context,
                                     const RowidRangeOptionPtr& rowid_range);
    void release_reader(RuntimeState* state);
    void refresh_reuse_signature();
    void refresh_runtime_filter_versions();
    Status refresh_runtime_range_pruner_for_stream_build_filters();
    LateRuntimeFilterReinitDecision detect_late_runtime_filter_reinit() const;
    void record_late_runtime_filter_reinit(const LateRuntimeFilterReinitDecision& decision);
    Status reset_reader_state_for_reinit();
    Status rebuild_scan_conjuncts();

    Status _extend_schema_by_access_paths();
    void _inherit_default_value_from_json(TabletColumn* column, const TabletColumn& root_column,
                                          const ColumnAccessPath* path);
    Status init_column_access_paths(Schema* schema);
    Status prune_schema_by_access_paths(Schema* schema);

private:
    const LakeDataSourceProvider* _provider;
    const TInternalScanRange _scan_range;

    Status _status = Status::OK();
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _not_push_down_conjuncts;
    PredicateTree _non_pushdown_pred_tree;
    Filter _selection;

    ObjectPool _obj_pool;

    RuntimeState* _runtime_state = nullptr;
    const std::vector<SlotDescriptor*>* _slots = nullptr;
    std::vector<std::unique_ptr<OlapScanRange>> _key_ranges;
    std::vector<OlapScanRange*> _scanner_ranges;
    std::unique_ptr<ScanConjunctsManager> _conjuncts_manager = nullptr;

    lake::VersionedTablet _tablet;
    TabletSchemaCSPtr _tablet_schema;
    TabletReaderParams _params{};
    std::shared_ptr<lake::TabletReader> _reader;
    lake::TabletReader::PreparedReadStatePtr _prepared_read_state;
    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<ChunkIterator> _prj_iter;
    Schema _reader_schema;
    Schema _output_schema;
    bool _reader_schema_inited = false;
    bool _use_projection_iterator = false;
    bool _prepare_only_mode = false;
    bool _reuse_pending = false;
    bool _ignore_split_context_prepared_state_once = false;
    struct ReuseSignature {
        bool valid = false;
        int64_t tablet_id = 0;
        std::string version;
        int64_t from_version = 0;
        const void* rowsets_identity = nullptr;
    };
    ReuseSignature _reuse_signature;
    std::unordered_map<int32_t, size_t> _runtime_filter_versions;

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
    int64_t _released_raw_rows_read = 0;
    int64_t _released_bytes_read = 0;
    int64_t _released_cpu_time_spent_ns = 0;
    int64_t _reported_num_rows_read = 0;

    RuntimeProfile::Counter* _bytes_read_counter = nullptr;
    RuntimeProfile::Counter* _rows_read_counter = nullptr;

    RuntimeProfile::Counter* _expr_filter_timer = nullptr;
    RuntimeProfile::Counter* _expr_filter_counter = nullptr;
    RuntimeProfile::Counter* _create_seg_iter_timer = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompress_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_counter = nullptr;
    RuntimeProfile::Counter* _del_vec_filter_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_timer = nullptr;
    RuntimeProfile::Counter* _rf_pred_filter_timer = nullptr;
    RuntimeProfile::Counter* _rf_pred_input_rows = nullptr;
    RuntimeProfile::Counter* _rf_pred_output_rows = nullptr;
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
    RuntimeProfile::Counter* _seg_metadata_filtered_counter = nullptr;
    RuntimeProfile::Counter* _segs_metadata_filtered_counter = nullptr;
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

    // Gin filter Statistics
    RuntimeProfile::Counter* _gin_filtered_timer = nullptr;
    RuntimeProfile::Counter* _gin_filtered_counter = nullptr;
    RuntimeProfile::Counter* _gin_prefix_filter_timer = nullptr;
    RuntimeProfile::Counter* _gin_ngram_dict_filter_timer = nullptr;
    RuntimeProfile::Counter* _gin_predicate_dict_filter_timer = nullptr;
    RuntimeProfile::Counter* _gin_dict_counter = nullptr;
    RuntimeProfile::Counter* _gin_ngram_dict_counter = nullptr;
    RuntimeProfile::Counter* _gin_ngram_dict_filtered_counter = nullptr;
    RuntimeProfile::Counter* _gin_predicate_dict_filtered_counter = nullptr;

    RuntimeProfile::Counter* _pushdown_predicates_counter = nullptr;
    RuntimeProfile::Counter* _non_pushdown_predicates_counter = nullptr;
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
    RuntimeProfile::Counter* _late_rf_reinit_counter = nullptr;
    RuntimeProfile::Counter* _late_rf_reinit_new_arrival_counter = nullptr;
    RuntimeProfile::Counter* _late_rf_reinit_version_changed_counter = nullptr;
    RuntimeProfile::Counter* _late_rf_reinit_filter_set_changed_counter = nullptr;
    RuntimeProfile::Counter* _lake_prepared_state_candidate_counter = nullptr;
    RuntimeProfile::Counter* _lake_prepared_state_reuse_hit_counter = nullptr;
    RuntimeProfile::Counter* _lake_prepared_state_reject_disabled_counter = nullptr;
    RuntimeProfile::Counter* _lake_prepared_state_reject_adaptive_pending_counter = nullptr;
    RuntimeProfile::Counter* _lake_prepared_state_targeted_read_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_seed_local_tasks_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_seed_local_rows_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_pending_tasks_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_pending_rows_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_pending_opened_prepared_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_pending_opened_unprepared_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_pending_opened_prepared_rows_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_pending_opened_unprepared_rows_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_refined_tasks_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_refined_rows_counter = nullptr;
    RuntimeProfile::Counter* _lake_adaptive_segment_switch_counter = nullptr;
    bool _lake_adaptive_last_segment_valid = false;
    size_t _lake_adaptive_last_rowset_index = 0;
    size_t _lake_adaptive_last_segment_index = 0;
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

    bool always_shared_scan() const override { return false; }

    StatusOr<pipeline::MorselQueuePtr> convert_scan_range_to_morsel_queue(
            const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
            bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
            size_t num_total_scan_ranges, size_t scan_parallelism = 0) override;

    // for ut
    void set_lake_tablet_manager(lake::TabletManager* tablet_manager) { _tablet_manager = tablet_manager; }

    // possiable physical distribution optimize of data source
    bool sorted_by_keys_per_tablet() const override {
        return _t_lake_scan_node.__isset.sorted_by_keys_per_tablet && _t_lake_scan_node.sorted_by_keys_per_tablet;
    }
    bool output_chunk_by_bucket() const override {
        return _t_lake_scan_node.__isset.output_chunk_by_bucket && _t_lake_scan_node.output_chunk_by_bucket;
    }

    size_t next_uniq_id() const { return starrocks::next_uniq_id(_t_lake_scan_node); }

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

    bool could_split() const { return _could_split; }

    bool could_split_physically() const { return _could_split_physically; }

    int64_t get_splitted_scan_rows() const { return splitted_scan_rows; }

    bool use_lake_adaptive_split_morsel_queue() const { return _use_lake_adaptive_split_morsel_queue; }

protected:
    ConnectorScanNode* _scan_node;
    const TLakeScanNode _t_lake_scan_node;

    // for ut
    lake::TabletManager* _tablet_manager;

    bool _could_split = false;
    bool _could_split_physically = false;
    bool _use_lake_adaptive_split_morsel_queue = false;
    int64_t splitted_scan_rows = 0;

private:
    StatusOr<bool> _could_tablet_internal_parallel(const std::vector<TScanRangeParams>& scan_ranges,
                                                   int32_t pipeline_dop, size_t num_total_scan_ranges,
                                                   TTabletInternalParallelMode::type tablet_internal_parallel_mode,
                                                   int64_t* scan_dop, int64_t* splitted_scan_rows) const;
    StatusOr<bool> _could_split_tablet_physically(const std::vector<TScanRangeParams>& scan_ranges) const;
};

} // namespace starrocks::connector
