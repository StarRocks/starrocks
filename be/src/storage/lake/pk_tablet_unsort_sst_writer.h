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

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "base/phmap/btree.h"
#include "storage/lake/pk_tablet_sst_writer.h"
#include "storage/sstable/table_builder.h"

namespace starrocks::lake {

// SST writer for separate-sort-key PK tables: primary keys arrive in sort-key order (i.e. NOT in
// primary-key order), so they cannot be streamed into a sorted SST directly. Instead they are
// buffered in an in-memory map keyed by encoded primary key, which both sorts them and collapses
// duplicate primary keys within the segment. The winner of a duplicate is the row with the largest
// per-row ordering key (rssid_rowid high bits = slot_idx = last memtable flush); the loser's rowid
// is recorded so publish can mask it with a delete vector. At flush the map is written as one sorted
// SST for the current segment (still 1 segment : 1 SST). Only the rowid is stored per entry; rssid
// and version are supplied via shared_rssid/shared_version at ingest time, exactly like the streaming
// builder.
// When the map grows past the memtable memory threshold (is_map_full, mirroring
// LakePersistentIndex::is_memtable_full) it is flushed to an intermediate SST and cleared, bounding
// memory. At segment finalize the intermediate SSTs (plus the residual map) are k-way merged into the
// single final SST; duplicate PKs across intermediates are resolved there (last-flushed wins) and the
// losers appended to the delete vector.
// Memory: the map is bounded by l0_max_mem_usage (100 MB default) before it spills, and each parallel
// merge task owns its own writer (and map), so peak usage is up to l0_max_mem_usage x the number of
// concurrent merge tasks before spilling kicks in. It is charged to the load-spill merge mem tracker.
class PkTabletUnsortSSTWriter : public PkTabletSSTWriter {
public:
    PkTabletUnsortSSTWriter(TabletSchemaCSPtr tablet_schema_ptr, TabletManager* tablet_mgr, int64_t tablet_id)
            : PkTabletSSTWriter(std::move(tablet_schema_ptr), tablet_mgr, tablet_id) {}
    ~PkTabletUnsortSSTWriter() override = default;
    Status append_sst_record(const Chunk& data, const std::vector<uint64_t>* rssid_rowids = nullptr,
                             const std::vector<uint32_t>* column_indexes = nullptr) override;
    // Feed the batch's DELETE rows (op-aware separate-sort-key path). They carry a per-row ordering key
    // (rssid_rowids) but no segment row, so they participate in the per-PK last-op reconciliation
    // without occupying a rowid. A DELETE that is the latest op for its PK is collected into a del file
    // (take_delete_keys) so publish erases the (possibly pre-existing) row; a DELETE superseded by a
    // later UPSERT is dropped.
    Status append_delete_records(const Chunk& data, const std::vector<uint64_t>* rssid_rowids,
                                 const std::vector<uint32_t>* column_indexes = nullptr) override;
    Status reset_sst_writer(const std::shared_ptr<LocationProvider>& location_provider,
                            const std::shared_ptr<FileSystem>& fs) override;
    StatusOr<std::pair<FileInfo, PersistentIndexSstableRangePB>> flush_sst_writer() override;
    bool has_file_info() const override { return _wf != nullptr; }
    std::vector<uint32_t> take_deleted_rowids() override { return std::move(_deleted_rowids); }
    MutableColumnPtr take_delete_keys() override { return std::move(_delete_keys); }

private:
    // A row that has no segment position (a DELETE) stores this reserved rowid, matching the
    // UINT32_MAX delete-rowid convention used elsewhere in the PK write path. At flush a winning
    // entry with this rowid is collected into `_delete_keys` (a del file) instead of a PK -> rowid
    // SST mapping, so publish erases the key -- including a row written by an earlier transaction.
    static constexpr uint32_t kDeleteRowid = std::numeric_limits<uint32_t>::max();

    // encoded primary key -> {order, rowid}. `order` is the per-row rssid_rowid used to pick the
    // last-op winner (kept the largest); `rowid` is the position within the current segment, or
    // kDeleteRowid when the winning op is a DELETE.
    struct Entry {
        uint64_t order;
        uint32_t rowid;
    };
    // An intermediate SST spilled from `_map` when it got full. Its entries store both rowid and order
    // (order in the value's version field) so the final merge can pick the dedup winner across SSTs.
    struct IntermediateSst {
        std::string location; // full path, used to reopen for merge and to delete afterwards
        uint64_t size;
        std::string encryption_meta;
    };

    // Project the primary-key columns out of a vertical-compaction key-group chunk into `out` in
    // primary-key order. `column_indexes[i]` is the tablet column id of `data`'s column i; the PK
    // columns are those with a tablet column id < num_key_columns. `out` then has the PK columns at
    // positions [0, num_key), which is what encode_pk_keys expects.
    Status project_pk_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, Chunk* out) const;
    // Shared body of append_sst_record (is_delete=false) and append_delete_records (is_delete=true):
    // encode each row's PK and reconcile it into `_map` by rssid_rowid order (last op wins). UPSERTs
    // take a running segment rowid; DELETEs use kDeleteRowid and do not advance the rowid counter.
    Status append_records(const Chunk& data, const std::vector<uint64_t>* rssid_rowids,
                          const std::vector<uint32_t>* column_indexes, bool is_delete);
    // Reconcile one row (already encoded) into `_map`, masking the loser's segment rowid (if any). Takes
    // a non-owning view: the map's transparent comparator finds without allocating, and a std::string is
    // materialized only on the insert (miss) branch, so duplicate primary keys cost no heap churn.
    void reconcile_entry(std::string_view key, uint64_t order, uint32_t rowid);
    // Append an encoded-PK slice (a winning DELETE's key) as one cell of the del-file column.
    void collect_delete_key(const Slice& key);
    // Whether `_map` has reached the memtable memory threshold and should be spilled to an SST.
    bool is_map_full() const;
    // Spill the current `_map` to a new intermediate SST (storing order+rowid), then clear the map.
    Status flush_map_to_intermediate_sst();
    // K-way merge the intermediate SSTs into `builder` (the final SST), keeping the max-order entry
    // per key and appending the losing rowids to `_deleted_rowids`.
    Status merge_intermediates_into(sstable::TableBuilder* builder);

    phmap::btree_map<std::string, Entry, std::less<>> _map;
    std::vector<uint32_t> _deleted_rowids;
    // Encoded primary keys (del-file binary format) whose latest op is a DELETE, collected at flush and
    // moved out via take_delete_keys(). A map/merge key IS an encoded-PK slice, so it is appended
    // straight into this column (built from clone_empty_pk_column). nullptr until the first winner.
    MutableColumnPtr _delete_keys;
    // Rough running estimate of `_map`'s memory footprint, compared against l0_*_mem_usage.
    size_t _map_mem_usage = 0;
    std::vector<IntermediateSst> _intermediate_ssts;
    // Running rowid within the current segment (== rows appended so far); reset per segment.
    uint32_t _next_rowid = 0;
    std::unique_ptr<WritableFile> _wf;
    std::string _encryption_meta;
    // Saved from reset_sst_writer so intermediate SSTs can be created during append.
    std::shared_ptr<LocationProvider> _location_provider;
    std::shared_ptr<FileSystem> _fs;
};

} // namespace starrocks::lake
