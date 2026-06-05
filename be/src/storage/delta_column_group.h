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

#include "common/status.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/olap_common.pb.h"
#include "storage/olap_common.h"

namespace starrocks {

// Physical layout of one delta-column-group file. Mirrors DeltaColumnFileKindPB
// in lake_types.proto. DENSE_COLS is the historical `.cols` file (one rewritten,
// row-complete column per uid). SPARSE_PERCOL is a `.spcols` Sparse Delta Column
// Group file covering only K updated rows, applied as an overlay layer.
enum class DeltaColumnFileKind : uint8_t {
    DENSE_COLS = 0,
    SPARSE_PERCOL = 1,
};

// Reserved column unique id for the leading `source_rowid` column inside every
// `.spcols` file. ColumnUID is the FE-assigned int32 (be/src/common/column_id.h):
// real columns count up from small positive ints toward next_column_unique_id,
// and there is no hardcoded sentinel uid in BE today (FULL_ROW_COLUMN/`__row`
// is matched by name, not by a reserved uid — see tablet_schema.cpp:633). We pick
// a value near INT32_MAX, far above any plausible next_column_unique_id, so it can
// never collide with a real column uid. This uid is internal to the .spcols
// container: it is the value-bearing column for base rowid <-> local ordinal
// translation and MUST NEVER appear in DeltaColumnGroup::column_ids() (the
// uid->file mapping); SegmentWriter::_verify_footer uid-uniqueness check backstops
// any accidental collision.
constexpr uint32_t kSDCGSourceRowidUid = 0x7FFFFFF0u; // 2147483632

// Sentinel for an unknown presence bound (no SparsePresencePB recorded for this file,
// e.g. a DENSE entry, legacy metadata, or a writer that predates the presences field).
// Readers must treat an unknown bound as "no skip": fall back to opening the .spcols.
constexpr int64_t kSDCGPresenceUnknown = -1;

// Per-file presence summary for a SPARSE_PERCOL `.spcols` overlay, mirroring
// SparsePresencePB in lake_types.proto. [min, max] is the closed range of base-segment
// ordinals (source_rowid values) the sparse file touches; row_count is K. All three
// default to kSDCGPresenceUnknown == "unknown" so DENSE / legacy slots normalize to a
// no-skip presence and the parallel array stays 1:1 with column_files.
struct SparsePresence {
    int64_t min_source_rowid = kSDCGPresenceUnknown;
    int64_t max_source_rowid = kSDCGPresenceUnknown;
    int64_t row_count = kSDCGPresenceUnknown;
    bool known() const { return min_source_rowid != kSDCGPresenceUnknown && max_source_rowid != kSDCGPresenceUnknown; }
};

// `DeltaColumnGroup` is used for record the new update columns data
// It is generated when column mode partial update happen.
// `_column_ids` record the columns which have been updated, and `_column_file` points to the data file
// which contains update columns.
// Column data file end as `.cols` (dense) or `.spcols` (sparse, SDCG).
class DeltaColumnGroup;
using DeltaColumnGroupPtr = std::shared_ptr<DeltaColumnGroup>;
using DeltaColumnGroupList = std::vector<DeltaColumnGroupPtr>;

class DeltaColumnGroup {
public:
    DeltaColumnGroup() = default;
    ~DeltaColumnGroup() = default;
    void init(int64_t version, const std::vector<std::vector<ColumnUID>>& column_ids,
              const std::vector<std::string>& column_files, const std::vector<std::string>& encryption_metas = {},
              int64_t file_size = 0);
    Status load(int64_t version, const char* data, size_t length);
    Status load(int64_t version, const DeltaColumnGroupVerPB& dcg_ver_pb);
    std::string save() const;
    // merge this dcg into dst dcgs by version, returns the number of successful merges
    int merge_into_by_version(DeltaColumnGroupList& dcgs, const std::string& dir, const RowsetId& rowset_id,
                              int segment_id);
    // merge src dcg into this dcg by version and change the src dcg's file name suffix
    bool merge_by_version(DeltaColumnGroup& dcg, const std::string& dir, const RowsetId& rowset_id, int segment_id);

    std::pair<int32_t, int32_t> get_column_idx(ColumnUID uid) const {
        for (int idx = 0; idx < _column_uids.size(); ++idx) {
            for (int cidx = 0; cidx < _column_uids[idx].size(); cidx++) {
                // it is impossible that multiple _column_ids[idx][cidx]
                // will hit cid in a single dcg.
                if (_column_uids[idx][cidx] == uid) {
                    return std::pair<int32_t, int32_t>{std::pair{idx, cidx}};
                }
            }
        }
        return std::pair<int32_t, int32_t>{std::pair{-1, -1}};
    }

    // `_column_file` contains `$1_$2_$3_$4.cols`, $1 is rowsetid, $2 is segment id. $3 is version . $4 is seq suffix
    std::vector<std::string> column_files(const std::string& dir_path) const {
        std::vector<std::string> column_files = _column_files;
        for (auto& column_file : column_files) {
            column_file = dir_path + "/" + column_file;
        }
        return column_files;
    }

    StatusOr<std::string> column_file_by_idx(const std::string& dir_path, uint32_t idx) const {
        if (idx >= _column_files.size()) {
            return Status::InvalidArgument(fmt::format("column_file_by_idx fail, path: {} column file cnt: {} idx: {}",
                                                       dir_path, _column_files.size(), idx));
        }
        return dir_path + "/" + _column_files[idx];
    }

    // TODO: rename
    std::vector<std::vector<ColumnUID>>& column_ids() { return _column_uids; }
    // TODO: rename
    const std::vector<std::vector<ColumnUID>>& column_ids() const { return _column_uids; }
    int64_t version() const { return _version; }

    std::string debug_string() {
        std::stringstream ss;
        ss << "ver:" << _version << ", ";
        for (int i = 0; i < _column_files.size(); ++i) {
            ss << "file:" << _column_files[i] << ", ";
            ss << "uids:";
            for (auto uid : _column_uids[i]) {
                ss << uid << "|";
            }

            ss << "\n";
        }
        return ss.str();
    }

    size_t memory_usage() const { return _memory_usage; }

    const std::vector<std::string>& relative_column_files() const { return _column_files; }

    const std::vector<std::string>& encryption_metas() const { return _encryption_metas; }

    // Per-file size of each `.cols` file, 1:1 with relative_column_files(). May be empty (and
    // individual entries may be 0) for data written before the size field existed.
    const std::vector<int64_t>& column_file_sizes() const { return _column_file_sizes; }

    int64_t file_size() const { return _file_size; }

    // === SDCG (Sparse Delta Column Group) ===
    // Per-file kind. `idx` indexes into relative_column_files(). When `idx` is
    // out of range (legacy metadata that never carried _file_kinds), the file is
    // DENSE_COLS — this is the zero-regression hinge: absent kinds == all dense.
    DeltaColumnFileKind file_kind(size_t idx) const {
        return idx < _file_kinds.size() ? _file_kinds[idx] : DeltaColumnFileKind::DENSE_COLS;
    }
    bool is_file_dense(size_t idx) const { return file_kind(idx) == DeltaColumnFileKind::DENSE_COLS; }
    // Row count (K) stored in a SPARSE_PERCOL file; 0 for dense entries / unknown.
    int64_t sparse_row_count(size_t idx) const { return idx < _sparse_row_counts.size() ? _sparse_row_counts[idx] : 0; }
    // Base segment row count (M) that this entry's sparse files were derived from.
    // 0 == unknown (legacy / dense-only metadata).
    int64_t source_segment_num_rows() const { return _source_segment_num_rows; }
    const std::vector<DeltaColumnFileKind>& file_kinds() const { return _file_kinds; }
    const std::vector<int64_t>& sparse_row_counts() const { return _sparse_row_counts; }
    // Per-file presence bounds for SPARSE_PERCOL overlays. `idx` indexes into
    // relative_column_files(). Out-of-range / unrecorded => kSDCGPresenceUnknown
    // (== "unknown", reader must not skip). DENSE entries are unknown by construction.
    int64_t presence_min(size_t idx) const {
        return idx < _presences.size() ? _presences[idx].min_source_rowid : kSDCGPresenceUnknown;
    }
    int64_t presence_max(size_t idx) const {
        return idx < _presences.size() ? _presences[idx].max_source_rowid : kSDCGPresenceUnknown;
    }
    int64_t presence_row_count(size_t idx) const {
        return idx < _presences.size() ? _presences[idx].row_count : kSDCGPresenceUnknown;
    }
    // True iff this file has a known [min,max] range. A reader may then skip the file
    // when its requested base rowid falls outside [presence_min, presence_max].
    bool presence_known(size_t idx) const { return idx < _presences.size() && _presences[idx].known(); }
    const std::vector<SparsePresence>& presences() const { return _presences; }
    // Install SDCG metadata. `kinds`/`counts`/`presences` are parallel to column_files
    // (either empty == legacy all-dense, or strictly 1:1 with column_files). `source_rows`
    // is M (0 == unknown). Lake-only path: local-engine serializers never call this,
    // so local behavior stays byte-identical. The 3-arg overload keeps presences absent
    // (every slot unknown) for callers that do not carry presence summaries.
    void set_sdcg_meta(std::vector<DeltaColumnFileKind> kinds, std::vector<int64_t> counts, int64_t source_rows) {
        _file_kinds = std::move(kinds);
        _sparse_row_counts = std::move(counts);
        _source_segment_num_rows = source_rows;
    }
    void set_sdcg_meta(std::vector<DeltaColumnFileKind> kinds, std::vector<int64_t> counts,
                       std::vector<SparsePresence> presences, int64_t source_rows) {
        _file_kinds = std::move(kinds);
        _sparse_row_counts = std::move(counts);
        _presences = std::move(presences);
        _source_segment_num_rows = source_rows;
    }

private:
    void _calc_memory_usage();

private:
    int64_t _version = 0;
    std::vector<std::vector<ColumnUID>> _column_uids;
    std::vector<std::string> _column_files;
    std::vector<std::string> _encryption_metas;
    std::vector<int64_t> _column_file_sizes; // per-file size, 1:1 with _column_files; 0/empty = unknown
    // === SDCG === parallel to _column_files; empty == legacy all-dense (see file_kind()).
    std::vector<DeltaColumnFileKind> _file_kinds;
    std::vector<int64_t> _sparse_row_counts; // K per sparse file; 0 for dense / unknown
    // Per-file presence bounds; 1:1 with _column_files when present, else empty (all unknown).
    std::vector<SparsePresence> _presences;
    int64_t _source_segment_num_rows = 0; // M of the source segment; 0 == unknown
    size_t _memory_usage = 0;
    int64_t _file_size = 0; // file size of all column files
};

class DeltaColumnGroupLoader {
public:
    DeltaColumnGroupLoader() = default;
    virtual ~DeltaColumnGroupLoader() = default;
    // Used for PK table
    virtual Status load(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* pdcgs) = 0;
    // Used for non-PK table
    virtual Status load(int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id, int64_t version,
                        DeltaColumnGroupList* pdcgs) = 0;
};

class DeltaColumnGroupListSerializer {
public:
    static std::string serialize_delta_column_group_list(const DeltaColumnGroupList& dcgs,
                                                         DeltaColumnGroupListPB* dst = nullptr);
    static Status deserialize_delta_column_group_list(const char* data, size_t length, DeltaColumnGroupList* dcgs);
    static Status deserialize_delta_column_group_list(const DeltaColumnGroupListPB& dcgs_pb,
                                                      DeltaColumnGroupList* dcgs);
    static Status _deserialize_delta_column_group_list(const DeltaColumnGroupListPB& dcgs_pb,
                                                       DeltaColumnGroupList* dcgs);
};

class DeltaColumnGroupListHelper {
public:
    static void garbage_collection(DeltaColumnGroupList& dcg_list, const TabletSegmentId& tsid,
                                   int64_t min_readable_version, const std::string& tablet_path,
                                   std::vector<std::pair<TabletSegmentId, int64_t>>* garbage_dcgs,
                                   std::vector<std::string>* garbage_files);

    // used for non-Primary Key tablet only
    static Status save_snapshot(const std::string& file_path, DeltaColumnGroupSnapshotPB& dcg_snapshot_pb);
    static Status parse_snapshot(const std::string& file_path, DeltaColumnGroupSnapshotPB& dcg_snapshot_pb);
};

} // namespace starrocks
