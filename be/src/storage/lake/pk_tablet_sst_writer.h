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

#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "column/global_dict/types_fwd_decl.h"
#include "gutil/macros.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_writer.h"
#include "storage/sstable/table_builder.h"

namespace starrocks::lake {

class PersistentIndexSstableStreamBuilder;

class DefaultSSTWriter {
public:
    virtual ~DefaultSSTWriter() = default;
    // `rssid_rowids`, when non-null, carries one per-row ordering key aligned with `data`'s rows.
    // Its high 32 bits encode the source memtable flush order (slot_idx); writers that must resolve
    // duplicate primary keys by "last-flushed wins" (see PkTabletUnsortSSTWriter) use it. Writers
    // whose input is already in primary-key order (PkTabletSSTWriter) ignore it.
    // `column_indexes`, when non-null, gives the tablet column id of each column in `data` (used by
    // the vertical-compaction key group, whose chunk carries [sort-key columns, then PK columns]).
    // The unsort writer uses it to locate the PK columns; other writers ignore it because `data`
    // already has the PK columns at positions [0, num_key_columns).
    virtual Status append_sst_record(const Chunk& data, const std::vector<uint64_t>* rssid_rowids = nullptr,
                                     const std::vector<uint32_t>* column_indexes = nullptr) {
        return Status::OK();
    }
    // Feed DELETE rows to writers that resolve op order themselves (the separate-sort-key unsort
    // writer). Default is a no-op: other writers receive deletes via the caller's del-file path.
    virtual Status append_delete_records(const Chunk& data, const std::vector<uint64_t>* rssid_rowids,
                                         const std::vector<uint32_t>* column_indexes = nullptr) {
        return Status::OK();
    }
    virtual Status reset_sst_writer(const std::shared_ptr<LocationProvider>& location_provider,
                                    const std::shared_ptr<FileSystem>& fs) {
        return Status::OK();
    }
    virtual StatusOr<std::pair<FileInfo, PersistentIndexSstableRangePB>> flush_sst_writer() { return Status::OK(); }
    virtual bool has_file_info() const { return false; }
    // Rowids (within the just-flushed segment) that lost primary-key dedup and must be masked by a
    // delete vector at publish. Non-empty only for the unsort writer; the caller moves them out once
    // per segment right after flush_sst_writer().
    virtual std::vector<uint32_t> take_deleted_rowids() { return {}; }
    // Encoded primary keys (del-file binary format) of the rows whose latest op is a DELETE, moved out
    // once per segment right after flush_sst_writer(). Non-empty only for the unsort writer, whose
    // sort-key-ordered merge cannot route deletes through the caller's del file; the caller writes them
    // to a del file so publish erases the (possibly pre-existing) rows. nullptr means "no deletes".
    virtual MutableColumnPtr take_delete_keys() { return nullptr; }
};

class PkTabletSSTWriter : public DefaultSSTWriter {
public:
    PkTabletSSTWriter(TabletSchemaCSPtr tablet_schema_ptr, TabletManager* tablet_mgr, int64_t tablet_id)
            : _tablet_schema_ptr(std::move(tablet_schema_ptr)), _tablet_mgr(tablet_mgr), _tablet_id(tablet_id) {}
    ~PkTabletSSTWriter() override = default;
    Status append_sst_record(const Chunk& data, const std::vector<uint64_t>* rssid_rowids = nullptr,
                             const std::vector<uint32_t>* column_indexes = nullptr) override;
    Status reset_sst_writer(const std::shared_ptr<LocationProvider>& location_provider,
                            const std::shared_ptr<FileSystem>& fs) override;
    StatusOr<std::pair<FileInfo, PersistentIndexSstableRangePB>> flush_sst_writer() override;
    bool has_file_info() const override { return _pk_sst_builder != nullptr; }

protected:
    // Encodes the primary-key columns of `data` into the encoded-key form used by the persistent
    // index SST. On success returns a pointer to `data.num_rows()` contiguous key slices; the slices
    // are backed by `keys` and `owned_column`, both of which the caller must keep alive until the
    // slices are consumed.
    StatusOr<const Slice*> encode_pk_keys(const Chunk& data, Buffer<Slice>* keys, MutableColumnPtr* owned_column);

    // An empty column in the encoded-PK (del-file binary) format, i.e. the same column type
    // encode_pk_keys fills, so an encoded key slice can be appended straight back as one del-file cell.
    // Returns nullptr before the first encode_pk_keys call (when _pk_column is not yet built).
    MutableColumnPtr clone_empty_pk_column() const { return _pk_column ? _pk_column->clone_empty() : nullptr; }

    TabletSchemaCSPtr _tablet_schema_ptr;
    TabletManager* _tablet_mgr;
    int64_t _tablet_id;

private:
    std::unique_ptr<PersistentIndexSstableStreamBuilder> _pk_sst_builder;
    MutableColumnPtr _pk_column;
    Schema _pkey_schema;
    size_t _key_size = 0;
};

} // namespace starrocks::lake
