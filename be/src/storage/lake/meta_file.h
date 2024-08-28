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

#include <string>
#include <unordered_map>

#include "column/vectorized_fwd.h"
#include "fs/fs.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"

namespace starrocks {

class DelVector;

namespace lake {

class UpdateManager;

enum RecoverFlag { OK = 0, RECOVER_WITHOUT_PUBLISH, RECOVER_WITH_PUBLISH };

class MetaFileBuilder {
public:
    explicit MetaFileBuilder(const Tablet& tablet, std::shared_ptr<TabletMetadata> metadata_ptr);
    // append delvec to builder's buffer
    void append_delvec(const DelVectorPtr& delvec, uint32_t segment_id);
    // append delta column group to builder
    void append_dcg(uint32_t rssid, const std::vector<std::string>& filenames,
                    const std::vector<std::vector<ColumnUID>>& unique_column_id_list);
    // handle txn log
    void apply_opwrite(const TxnLogPB_OpWrite& op_write, const std::map<int, FileInfo>& replace_segments,
                       const std::vector<std::string>& orphan_files);
    void apply_column_mode_partial_update(const TxnLogPB_OpWrite& op_write);
    void apply_opcompaction(const TxnLogPB_OpCompaction& op_compaction, uint32_t max_compact_input_rowset_id,
                            int64_t output_rowset_schema_id);
    void apply_opcompaction_with_conflict(const TxnLogPB_OpCompaction& op_compaction);
    // finalize will generate and sync final meta state to storage.
    // |txn_id| the maximum applied transaction ID, used to construct the delvec file name, and
    // the garbage collection module relies on this value to check if a delvec file can be safely
    // deleted.
    Status finalize(int64_t txn_id);
    // find delvec in builder's buffer, used for batch txn log precess.
    StatusOr<bool> find_delvec(const TabletSegmentId& tsid, DelVectorPtr* pdelvec) const;

    // update num dels in rowset meta, `segment_id_to_add_dels` record each segment's incremental del count
    Status update_num_del_stat(const std::map<uint32_t, size_t>& segment_id_to_add_dels);

    void set_recover_flag(RecoverFlag flag) { _recover_flag = flag; }
    RecoverFlag recover_flag() const { return _recover_flag; }

    void finalize_sstable_meta(const PersistentIndexSstableMetaPB& sstable_meta);

private:
    // update delvec in tablet meta
    Status _finalize_delvec(int64_t version, int64_t txn_id);
    // fill delvec cache, for better reading latency
    void _fill_delvec_cache();
    // collect del files which are above cloud native index's rebuild point
    void _collect_del_files_above_rebuild_point(RowsetMetadataPB* rowset,
                                                std::vector<DelfileWithRowsetId>* collect_del_files);

private:
    Tablet _tablet;
    std::shared_ptr<TabletMetadata> _tablet_meta;
    UpdateManager* _update_mgr;
    Buffer<uint8_t> _buf;
    std::unordered_map<uint32_t, DelvecPagePB> _delvecs;
    // from segment id to delvec, used for fill cache in finalize stage.
    std::unordered_map<uint32_t, DelVectorPtr> _segmentid_to_delvec;
    // from cache key to segment id
    std::unordered_map<std::string, uint32_t> _cache_key_to_segment_id;
    // When recover flag isn't ok, need recover later
    RecoverFlag _recover_flag = RecoverFlag::OK;
};

Status get_del_vec(TabletManager* tablet_mgr, const TabletMetadata& metadata, uint32_t segment_id, bool fill_cache,
                   DelVector* delvec);
bool is_primary_key(TabletMetadata* metadata);
bool is_primary_key(const TabletMetadata& metadata);

void trim_partial_compaction_last_input_rowset(const MutableTabletMetadataPtr& metadata,
                                               const TxnLogPB_OpCompaction& op_compaction,
                                               RowsetMetadataPB& last_input_rowset);

} // namespace lake
} // namespace starrocks
