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
#include "exec/pipeline/scan/morsel.h"
#include "fs/fs.h"
#include "storage/chunk_iterator.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class Segment;
class TabletMetadataPB;
class RowsetMetadataPB;
namespace lake {
class TabletManager;
}
} // namespace starrocks

namespace starrocks::connector {

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

struct ChangesRowset {
    int64_t version;
    std::vector<std::string> segments;
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
    void _scan_metadata_for_changes_rowsets(const TabletMetadataPB& meta,
                                            std::unordered_set<uint32_t>& seen_rowset_ids,
                                            std::unordered_set<int64_t>& discovered_versions);

    // Phase 2: segment reading
    Status _read_next_chunk(ChunkPtr* chunk);
    Status _open_next_segment();
    Schema _build_read_schema();
    void _append_metadata_columns(Chunk* chunk, int64_t version);

    Status _read_head_metadata();

    // --- Inputs (immutable after open) ---
    const ChangesDataSourceProvider* _provider;
    int64_t _tablet_id = 0;
    int64_t _base_version = 0;   // V_base (left-open)
    int64_t _head_version = 0;   // V_head (right-closed)

    // --- Runtime context ---
    RuntimeState* _runtime_state = nullptr;
    const TupleDescriptor* _tuple_desc = nullptr;

    // --- Tuple slots resolved from _tuple_desc ---
    std::vector<SlotDescriptor*> _data_slots;
    std::optional<int> _change_type_slot_id;
    std::optional<int> _row_version_slot_id;
    bool _change_type_slot_is_nullable = false;
    bool _row_version_slot_is_nullable = false;

    // --- Phase 1: metadata traversal output ---
    std::shared_ptr<const TabletMetadataPB> _head_metadata;
    TabletSchemaCSPtr _tablet_schema;
    std::vector<ChangesRowset> _changes_rowsets;
    // Fail-fast gate: any in-range rowset with a DELETE predicate aborts
    // Phase 2. Spec §2 reserves DELETE for the PK stage.
    bool _has_delete_predicate = false;

    // --- Phase 2: segment-read cursor and IO state ---
    size_t _current_rowset_index = 0;
    size_t _current_segment_index = 0;
    ChunkIteratorPtr _segment_iter;
    std::shared_ptr<FileSystem> _fs;
    std::optional<Schema> _cached_read_schema;
    OlapReaderStatistics _seg_stats;
    int64_t _prev_block_fetch_ns = 0;

    // --- Counters exposed via the DataSource interface ---
    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;
};

} // namespace starrocks::connector
