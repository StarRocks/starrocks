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
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column.h"
#include "connector/connector.h"
#include "exec/pipeline/scan/morsel.h"
#include "gen_cpp/Descriptors_types.h"
#include "storage/chunk_iterator.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class TabletMetadataPB;
class RowsetMetadataPB;
namespace lake {
class TabletManager;
}
} // namespace starrocks

namespace starrocks::connector {

// Pairs a tuple slot with the CHANGES metadata kind that fills it.
struct ChangesMetaSlot {
    TChangesMetaKind::type kind;
    const SlotDescriptor* slot;
};

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
    // CHANGES emits one scan range per tablet; an empty delta yields zero
    // ranges and 0 rows. Returning false here would let the pipeline layer
    // inject a default-constructed TScanRangeParams() placeholder, which
    // drives a get_tablet_metadata(tablet_id=0, ...) call that fails with
    // "starlet err grpc.GetShard(shardId=0)". Matches OlapScanNode/Lake.
    // TODO: fold empty-delta CHANGES into a ValuesScan in the FE optimizer
    // so no scan operator is scheduled at all.
    bool accept_empty_scan_ranges() const override { return true; }

    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const TChangesScanNode _changes_scan_node;
};

/// Per-tablet CHANGES scan over a (base, head] range for duplicate-key and
/// aggregate-key tables. Walks tablet metadata backwards via
/// metadata_ancestors to discover LOAD rowsets in range, reads them, and
/// appends the metadata columns named in the plan node to every surfaced row.
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
    Status _do_metadata_traversal();
    void _scan_metadata_for_changes_rowsets(const TabletMetadataPtr& meta,
                                            std::unordered_set<uint32_t>& seen_rowset_ids);

    Status _read_next_chunk(ChunkPtr* chunk);

    Status _init_read_schema();

    // --- Inputs (immutable after open) ---
    const ChangesDataSourceProvider* _provider;
    int64_t _tablet_id = 0;
    int64_t _base_version = 0; // left-open
    int64_t _head_version = 0; // right-closed

    // --- Runtime context ---
    RuntimeState* _runtime_state = nullptr;
    const TupleDescriptor* _tuple_desc = nullptr;

    // --- Tuple slots resolved from _tuple_desc ---
    std::vector<SlotDescriptor*> _data_slots;
    // Tuple slots that receive CHANGES metadata values, built once in open().
    std::vector<ChangesMetaSlot> _changes_meta_slots;

    // Stable slot-id -> chunk column index mapping for every materialized
    // data slot. Built once in _init_read_schema() so the per-chunk hot path
    // does not re-scan the chunk schema.
    std::vector<std::pair<SlotId, size_t>> _data_slot_chunk_indices;

    // --- Metadata traversal output ---
    std::shared_ptr<const TabletMetadataPB> _head_metadata;
    TabletSchemaCSPtr _tablet_schema;
    std::vector<lake::RowsetPtr> _changes_rowsets;
    // True if any in-range rowset carries a DELETE predicate. DUP/AGG
    // CHANGES does not surface deletions, so open() aborts with NotSupported.
    bool _has_delete_predicate = false;

    // --- Read cursor and per-rowset iterator buffer ---
    // Union over per-segment iterators; each segment iterator is wrapped
    // by ChangesMetaAppendingIterator so chunks surface with the metadata columns
    // already populated from the source rowset's version.
    ChunkIteratorPtr _chunk_iter;
    Schema _read_schema;
    OlapReaderStatistics _read_stats;

    // --- Counters exposed via the DataSource interface ---
    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;
};

} // namespace starrocks::connector
