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

// Per-COLUMN presence inside a PACKED SPARSE_PERCOL `.spcols` file, mirroring
// ColumnSparsePresencePB in lake_types.proto. A packed file is a UNION over several rowid
// equivalence classes; every value column physically has K_union rows (real values where the
// column covers the union, append_default placeholders elsewhere). The placeholder rows are
// physical NULLs that MUST NEVER be applied. `roaring` is the AUTHORITATIVE apply gate: the
// exact base-segment rowid set this column covers. [min,max]+count are the cheap zero-IO
// pre-filter (range skip) only -- they are INSUFFICIENT to gate apply because columns
// interleave within the union. All bounds default to kSDCGPresenceUnknown; `roaring` empty
// (the default) means "no per-column override recorded -- fall back to the file-level
// presence" (a homogeneous / single-class file has no placeholders, so its source_rowid
// column already IS every column's covered set).
struct ColumnSparsePresence {
    ColumnUID column_uid = 0;
    int64_t min_source_rowid = kSDCGPresenceUnknown;
    int64_t max_source_rowid = kSDCGPresenceUnknown;
    int64_t count = kSDCGPresenceUnknown;
    // Serialized 32-bit CRoaring portable bitmap (read via roaring::Roaring::readSafe). Empty
    // => no exact set recorded; the reader must fall back to the file-level presence.
    std::string roaring;
    bool known() const { return min_source_rowid != kSDCGPresenceUnknown && max_source_rowid != kSDCGPresenceUnknown; }
    bool has_roaring() const { return !roaring.empty(); }
};

// Per-COLUMN presence list for ONE packed `.spcols` file: one ColumnSparsePresence per UPDATE
// column uid the file carries. Mirrors ColumnPresenceListPB. EMPTY (entries.empty()) for a
// DENSE entry, a legacy slot, or a HOMOGENEOUS (single equivalence class) sparse file: the
// reader then gates on the file-level presence. Parallel to the file list, 1:1 with
// column_files when present (same normalization rule as file_kinds / presences).
struct ColumnPresenceList {
    std::vector<ColumnSparsePresence> entries;
    bool empty() const { return entries.empty(); }
    // The per-column presence for |uid|, or nullptr when this file carries no override for it
    // (homogeneous file, dense entry, or a uid absent from this packed file).
    const ColumnSparsePresence* find(ColumnUID uid) const {
        for (const auto& e : entries) {
            if (e.column_uid == uid) return &e;
        }
        return nullptr;
    }
};

// An inline sparse patch decoded from InlineSparsePatchPB. It is a `.spcols`-equivalent
// overlay carried in tablet metadata (no file). It is a SEPARATE AXIS from the file lists
// (_column_files / _column_uids): inline patches are NOT files and never appear in
// column_ids() / column_files() / get_column_idx(). The layered overlay reader iterates
// inline patches via inline_patch_count()/inline_patch_at(i) and merges them with the
// file-backed layers ordered by `version`.
//
// Semantics mirror a SPARSE_PERCOL `.spcols`: K rows of a fixed column set, addressed by
// the ascending `source_rowids` (base-segment ordinals), applied version-ascending as a
// last-write-wins overlay. `column_values[i]` is the ColumnArraySerde blob for
// `column_uids[i]` (same order); the reader deserializes it into the read-schema type.
struct InlinePatch {
    int64_t version = 0;
    std::vector<ColumnUID> column_uids;
    int64_t row_count = 0;
    // Decoded once from the InlineSparsePatchPB.source_rowids LE bytes (ascending). The
    // reader consumes this directly instead of re-parsing the wire bytes.
    std::vector<uint32_t> source_rowids;
    // One serialized value column per uid (same order as column_uids). The reader
    // deserializes each via serde::ColumnArraySerde into the column type from the read schema.
    std::vector<std::string> column_values;
    int64_t min_source_rowid = kSDCGPresenceUnknown;
    int64_t max_source_rowid = kSDCGPresenceUnknown;
    bool known() const { return min_source_rowid != kSDCGPresenceUnknown && max_source_rowid != kSDCGPresenceUnknown; }
    // The column-position of |uid| within this patch (index into column_uids / column_values),
    // or -1 when the patch does not carry that uid.
    int32_t value_idx_of(ColumnUID uid) const {
        for (size_t i = 0; i < column_uids.size(); ++i) {
            if (column_uids[i] == uid) return static_cast<int32_t>(i);
        }
        return -1;
    }
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

    // === SDCG per-column presence (packed `.spcols` files) ===
    // The per-column presence list of file |file_idx| (indexes into relative_column_files()).
    // Returns an EMPTY list (no entries) when the file is dense, legacy, or a homogeneous
    // single-class sparse file: the reader then gates on the file-level presence above. Packed
    // files return one entry per UPDATE column uid, each with the exact covered-rowid roaring.
    static const ColumnPresenceList& empty_column_presence_list() {
        static const ColumnPresenceList kEmpty;
        return kEmpty;
    }
    const ColumnPresenceList& column_presence_list(size_t file_idx) const {
        return file_idx < _column_presence_lists.size() ? _column_presence_lists[file_idx]
                                                        : empty_column_presence_list();
    }
    const std::vector<ColumnPresenceList>& column_presence_lists() const { return _column_presence_lists; }
    // Per-column presence accessors keyed by (file_idx, uid). When file |file_idx| carries a
    // per-column override for |uid|, returns its exact bound / serialized roaring; otherwise
    // signals "unknown / fall back to the file-level presence" (kSDCGPresenceUnknown / empty).
    // These are exactly the gates the reader (Agent D) needs to apply a column at ONLY its
    // covered rows inside a packed union file.
    int64_t column_presence_min(size_t file_idx, ColumnUID uid) const {
        const auto* e = column_presence_list(file_idx).find(uid);
        return e != nullptr ? e->min_source_rowid : kSDCGPresenceUnknown;
    }
    int64_t column_presence_max(size_t file_idx, ColumnUID uid) const {
        const auto* e = column_presence_list(file_idx).find(uid);
        return e != nullptr ? e->max_source_rowid : kSDCGPresenceUnknown;
    }
    int64_t column_presence_count(size_t file_idx, ColumnUID uid) const {
        const auto* e = column_presence_list(file_idx).find(uid);
        return e != nullptr ? e->count : kSDCGPresenceUnknown;
    }
    // True iff file |file_idx| records a known [min,max] per-column range for |uid|. When false
    // the reader must fall back to the file-level presence (homogeneous / single-class file).
    bool column_presence_known(size_t file_idx, ColumnUID uid) const {
        const auto* e = column_presence_list(file_idx).find(uid);
        return e != nullptr && e->known();
    }
    // The serialized 32-bit CRoaring portable bitmap of the EXACT base-segment rowids |uid|
    // covers inside file |file_idx| (the authoritative apply gate). Empty string => no exact
    // set recorded; the reader falls back to the file-level presence.
    const std::string& column_presence_roaring(size_t file_idx, ColumnUID uid) const {
        static const std::string kEmpty;
        const auto* e = column_presence_list(file_idx).find(uid);
        return e != nullptr ? e->roaring : kEmpty;
    }

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
    // Packing-aware overload: also installs the per-column presence lists (1:1 with column_files
    // when present, else empty == every file gates on its file-level presence). Used by the lake
    // load path when the VerPB carries column_presence_lists; the local-engine save()/load()
    // path never calls this, so local behavior stays byte-identical.
    void set_sdcg_meta(std::vector<DeltaColumnFileKind> kinds, std::vector<int64_t> counts,
                       std::vector<SparsePresence> presences, std::vector<ColumnPresenceList> column_presence_lists,
                       int64_t source_rows) {
        _file_kinds = std::move(kinds);
        _sparse_row_counts = std::move(counts);
        _presences = std::move(presences);
        _column_presence_lists = std::move(column_presence_lists);
        _source_segment_num_rows = source_rows;
    }

    // === SDCG inline sparse patches (separate axis from the file lists) ===
    // Inline patches are `.spcols`-equivalent overlays carried in this entry's meta (no file).
    // They are NOT files: they never appear in column_ids() / column_files() / get_column_idx().
    // The layered overlay reader iterates them here and interleaves them with the file-backed
    // layers ordered by version (ascending apply, last-write-wins). Loaded only from the lake
    // VerPB; the local-engine save()/load() path never populates them (stays byte-identical).
    size_t inline_patch_count() const { return _inline_patches.size(); }
    const InlinePatch& inline_patch_at(size_t i) const { return _inline_patches[i]; }
    const std::vector<InlinePatch>& inline_patches() const { return _inline_patches; }

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
    // Per-COLUMN presence for packed sparse files; 1:1 with _column_files when present, else
    // empty. A dense / legacy / homogeneous slot carries an empty ColumnPresenceList (the
    // reader then gates on the file-level _presences entry).
    std::vector<ColumnPresenceList> _column_presence_lists;
    // Inline sparse patches (separate axis: NOT parallel to _column_files). Loaded from the lake
    // VerPB inline_patches; empty for dense / legacy / local-engine metadata.
    std::vector<InlinePatch> _inline_patches;
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
