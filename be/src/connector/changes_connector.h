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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "column/chunk.h"
#include "column/column.h"
#include "connector/connector.h"
#include "exec/olap_scan_prepare.h"
#include "exec/pipeline/scan/morsel.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/runtime_filter_predicate.h"
#include "storage/runtime_range_pruner.hpp"
#include "fs/fs.h"
#include "storage/chunk_iterator.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class PredicateParser;
class Segment;
class TabletMetadataPB;
class RowsetMetadataPB;
namespace lake {
class TabletManager;
}
} // namespace starrocks

namespace starrocks::connector {

// Spec §3.6 E11: caller-actionable error when the metadata ancestor chain is too
// short to cover the (V_base, V_head] request range. Surfaced verbatim to the FE so
// users can correlate with `cloud_native_tablet_metadata_ancestors_recorded`.
// Exposed for unit testing the exact wording without staging a full traversal.
std::string format_ancestor_chain_exhausted_error(int64_t tablet_id);

class ChangesConnector final : public Connector {
public:
    ~ChangesConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::CHANGES; }
};

class ChangesDataSource;

class ChangesDataSourceProvider final : public DataSourceProvider {
public:
    ~ChangesDataSourceProvider() override = default;
    friend class ChangesDataSource;
    ChangesDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return false; }
    // CHANGES semantics: each tablet corresponds to one scan range. An empty delta
    // (e.g. `CHANGES FROM V TO V`) produces zero scan ranges and should yield 0 rows.
    // Returning false here would cause the pipeline layer to inject a default-constructed
    // TScanRangeParams() placeholder (see morsel.cpp build_scan_morsels), which makes
    // ChangesDataSource call get_tablet_metadata(tablet_id=0, ...) and fail with
    // "starlet err grpc.GetShard(shardId=0)". Returning true matches OlapScanNode/Lake.
    // TODO: the root-cause fix is to fold empty-delta CHANGES into a ValuesScan in the
    // FE optimizer so no scan operator is scheduled at all.
    bool accept_empty_scan_ranges() const override { return true; }

    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const TChangesScanNode _changes_scan_node;
};

/// Per-tablet statistics collected during metadata traversal (Phase 1).
struct ChangesStats {
    int64_t insertion_rows = 0;
    int64_t insertion_data_size = 0;
    int64_t insertion_segment_count = 0;
    int64_t insertion_segment_total_rows = 0;
    int64_t deletion_rows = 0;
    int64_t deletion_data_size = 0;
    int64_t deletion_segment_count = 0;
    int64_t deletion_segment_total_rows = 0;
    int64_t load_count = 0;
    int64_t compaction_count = 0;
    int64_t metadata_count = 0;
    bool has_delete_predicate = false;
    int64_t base_version_rows = 0;
    int64_t base_version_data_size = 0;
    int64_t base_version_segment_count = 0;
    int64_t head_version_rows = 0;
    int64_t head_version_data_size = 0;
    int64_t head_version_segment_count = 0;
};

struct RowVersionFilter {
    TExprOpcode::type op;
    int64_t value;

    RowVersionFilter(TExprOpcode::type op, int64_t value) : op(op), value(value) {}

    bool evaluate(int64_t commit_version) const {
        switch (op) {
        case TExprOpcode::EQ: return commit_version == value;
        case TExprOpcode::NE: return commit_version != value;
        case TExprOpcode::LT: return commit_version < value;
        case TExprOpcode::LE: return commit_version <= value;
        case TExprOpcode::GT: return commit_version > value;
        case TExprOpcode::GE: return commit_version >= value;
        default: return true;
        }
    }
};

struct ChangesRowset {
    uint32_t id;
    int64_t commit_version;
    int64_t num_rows;       // total rowset rows
    int64_t data_size;
    std::vector<std::string> segments;  // segment filenames
    std::vector<int64_t> segment_num_rows;  // per-segment row counts (from SegmentMetadataPB)

    // Returns per-segment row count. Falls back to proportional estimate
    // when segment_metas is absent or lacks num_rows.
    int64_t get_segment_num_rows(size_t segment_index) const {
        if (segment_index < segment_num_rows.size()) {
            return segment_num_rows[segment_index];
        }
        // Fallback: proportional estimate
        return segments.empty() ? 0 : num_rows / static_cast<int64_t>(segments.size());
    }
};

struct ChangesPhase1Result {
    std::vector<ChangesRowset> changes_rowsets;      // Phase 1 discovered delta rowsets (with segment_metas)
    std::shared_ptr<const TabletMetadataPB> head_metadata;  // V_head metadata
};

/// CDC data source implementing the Delta Replay algorithm for
/// duplicate-key and aggregate-key tables.
///
/// Two-phase execution:
///   Phase 1 (_do_metadata_traversal): Walk tablet metadata backwards via
///     metadata_ancestors, discovering LOAD rowsets in (base_version, head_version].
///   Phase 2 (_read_next_chunk): Read segment data from discovered rowsets,
///     attach __CHANGE_TYPE__(INSERT=0) and __ROW_VERSION__ metadata columns.
class ChangesDataSource final : public DataSource {
public:
    ~ChangesDataSource() override = default;

    ChangesDataSource(const ChangesDataSourceProvider* provider, const TScanRange& scan_range);
    std::string name() const override { return "ChangesDataSource"; }
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status parse_runtime_filters(RuntimeState* state) override { return Status::OK(); }
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override { return _rows_read; }
    int64_t num_rows_read() const override { return _rows_read; }
    int64_t num_bytes_read() const override { return _bytes_read; }
    int64_t cpu_time_spent() const override { return _cpu_time_ns; }

private:
    // spec §2 enum: 0 = INSERT, 1 = DELETE (reserved for PK / delete-predicate phase)
    static constexpr int8_t kChangeTypeInsert = 0;

    // Phase 1: metadata traversal
    Status _do_metadata_traversal();
    void _scan_metadata_for_changes_rowsets(const TabletMetadataPB& meta);

    // Phase 2: segment reading
    Status _read_next_chunk(ChunkPtr* chunk);
    Status _open_next_segment();
    Schema _build_read_schema();
    void _append_metadata_columns(Chunk* chunk, int64_t commit_version);

    void _classify_predicates();
    void _try_extract_row_version_predicate(ExprContext* ctx);
    Status _read_head_metadata();
    Status _init_predicate_tree();

    // Input parameters (from TChangesScanRange)
    const ChangesDataSourceProvider* _provider;
    int64_t _tablet_id = 0;
    int64_t _base_version = 0; // V_base (left-open)
    int64_t _head_version = 0; // V_head (right-closed)

    // Phase 1 state
    std::unordered_set<uint32_t> _found_ids;
    std::unordered_set<int64_t> _found_cv;
    std::vector<ChangesRowset> _changes_rowsets;
    std::shared_ptr<const TabletMetadataPB> _head_metadata;

    // Stats
    ChangesStats _stats;
    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;

    // --- Profile 计数器（谓词过滤） ---
    RuntimeProfile::Counter* _expr_filter_timer = nullptr;
    RuntimeProfile::Counter* _expr_filter_counter = nullptr;

    // --- 谓词分类 ---
    std::vector<ExprContext*> _data_column_conjuncts;  // 可下推到存储层的数据列谓词
    std::vector<RowVersionFilter> _row_version_filters;
    RuntimeProfile::Counter* _rowset_skipped_counter = nullptr;

    ObjectPool _obj_pool;                              // 声明最先 → 析构最后（_parser 等由其管理）
    std::unique_ptr<ScanConjunctsManager> _conjuncts_manager;
    PredicateParser* _parser = nullptr;                 // _obj_pool 管理生命周期
    PredicateTree _cached_pred_tree;                   // 可推到存储层的谓词
    RuntimeScanRangePruner _runtime_range_pruner;
    ColumnPredicatePtrs _col_preds_owner;

    // --- Profile 计数器（存储层下推） ---
    RuntimeProfile::Counter* _pred_pushdown_counter = nullptr;

    const TupleDescriptor* _tuple_desc = nullptr;
    std::vector<std::string> _key_column_names;

    RuntimeState* _runtime_state = nullptr;

    // Phase 2 state: segment reading
    size_t _current_rowset_index = 0;
    size_t _current_segment_index = 0;
    ChunkIteratorPtr _segment_iter;
    TabletSchemaCSPtr _tablet_schema;
    OlapReaderStatistics _seg_stats;
    int64_t _prev_block_fetch_ns = 0;
    std::shared_ptr<FileSystem> _fs;
    std::optional<Schema> _cached_read_schema;

    // Slot ID tracking for metadata columns
    std::optional<int> _change_type_slot_id;
    std::optional<int> _row_version_slot_id;

    // Pre-computed nullable flags for metadata slots
    bool _change_type_slot_is_nullable = false;
    bool _row_version_slot_is_nullable = false;

    // Data column slots (exclude metadata columns)
    std::vector<SlotDescriptor*> _data_slots;
};

} // namespace starrocks::connector
