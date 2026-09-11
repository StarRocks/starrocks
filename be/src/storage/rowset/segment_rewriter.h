// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/rowset/segment_writer.h"
#include "storage/rowset_update_state.h"

namespace starrocks {

class TabletSchema;

class Column;

// Directs how the segment rewrite produces vector indexes for the dest segment. Shared-data
// callers fill the location-provider-resolved .vi paths (keyed on the dest segment name) plus the
// schema's index_build_mode/threshold; the rewrite then mirrors the normal lake writer: sync
// indexes are built inline at the reader-visible path, async builds are deferred to the
// FE-scheduled VectorIndexBuildTask, and the actually-produced/scheduled index ids are returned
// through the caller's out_vector_index_ids. Shared-nothing callers leave the defaults (empty paths,
// sync, threshold 0) and pass no out param: the SegmentWriter uses its IndexDescriptor fallback
// paths and no ids are recorded.
struct RewriteVectorIndexOptions {
    std::map<int64_t, std::string> file_paths;
    bool defer_build = false;
    uint32_t build_threshold = 0;
};

class SegmentRewriter {
public:
    SegmentRewriter();
    ~SegmentRewriter() = default;

    // rewrite a segment file, add/replace some of it's columns
    // read from src, write to dest
    // this function will read data from src_file and write to dest file first
    // then append write_column to dest file
    //
    // |flat_json_config| is the table-level flat JSON config of the tablet whose data is rewritten,
    // or nullptr for a table that carries none (its tablet meta then has no flat_json_config field
    // at all, and SegmentWriter keeps the be.conf global behavior). It is deliberately NOT defaulted:
    // a rewrite that leaves it unset silently re-derives the JSON physical form from the be.conf
    // globals instead of the table's own flat_json properties -- the exact defect this parameter
    // exists to prevent -- so every caller is made to name where its config comes from.
    static Status rewrite_partial_update(const FileInfo& src, FileInfo* dest,
                                         const std::shared_ptr<const TabletSchema>& tschema,
                                         const std::shared_ptr<FlatJsonConfig>& flat_json_config,
                                         std::vector<uint32_t>& column_ids, MutableColumns& columns,
                                         uint32_t segment_id, const FooterPointerPB& partial_rowset_footer,
                                         SegmentFileMark segment_file_mark = {},
                                         RewriteVectorIndexOptions vector_index_opts = {},
                                         std::vector<int64_t>* out_vector_index_ids = nullptr);
    // Rewrite a cross-published partial-update segment into a file holding ONLY the rows |owned|
    // marks as this tablet's.
    //
    // rewrite_partial_update above byte-copies the source's already-written columns and appends the
    // resolved ones, so its output is rowid-identical to its source -- and therefore still holds every
    // row the split handed to the siblings. Everything downstream then has to compensate for those
    // rows: the resolved columns must be widened to segment length so the append has a value per
    // source row, the segment's delete vector must mask them so reads do not serve them, and the
    // UNSHARE compaction must rewrite the rowset later to drop them for good.
    //
    // This variant decodes every column instead, drops the rows |owned| excludes, and renumbers what
    // is left, so the output is an ordinary full segment private to this tablet with no foreign rows
    // in it at all. It costs a full re-encode rather than a copy plus append, which is why only the
    // cross-published path uses it.
    //
    // |owned| and |resolved_columns| are both indexed by the rows the narrowed publish iterator
    // EMITTED -- one entry per emitted row, owned or not -- and |emitted_rowid_base| says where that
    // run starts in the source file. Both halves are therefore filtered by the same mask, and a source
    // row outside the emitted run is not this tablet's either, so it goes as well.
    // Exposed for testing: the (emitted base, mask) -> source-row selection arithmetic both
    // owned-only rewrites share. It selects over the source rows [|source_row_base|, +|num_rows|), so
    // a caller that reads its source in chunks passes each chunk's absolute position here and keeps
    // handing over the whole mask -- re-slicing |owned| per chunk instead runs off its end as soon as
    // the emitted run stops before the source does. See the definition for why rows outside the
    // emitted run go too.
    static Filter build_owned_selection(size_t source_row_base, size_t num_rows, uint32_t emitted_rowid_base,
                                        const Filter& owned);

    // |dest| comes back carrying the metadata of the file this produced rather than of its source:
    // its row count and its sort-key fields, both marked authoritative by dropped_unowned_rows. The
    // replacement metadata is otherwise copied from the source segment, and dropping rows invalidates
    // exactly those fields -- the count outright, the sort-key bounds by leaving them too wide, and
    // the samples by addressing rows the file no longer holds.
    //
    // See rewrite_partial_update for |flat_json_config|.
    static Status rewrite_partial_update_owned_only(
            const FileInfo& src, SegmentFileInfo* dest, const std::shared_ptr<const TabletSchema>& tschema,
            const std::shared_ptr<FlatJsonConfig>& flat_json_config, const std::vector<uint32_t>& resolved_column_ids,
            MutableColumns& resolved_columns, const Filter& owned, uint32_t emitted_rowid_base, uint32_t segment_id,
            const FooterPointerPB& partial_rowset_footer, SegmentFileMark segment_file_mark = {},
            RewriteVectorIndexOptions vector_index_opts = {}, std::vector<int64_t>* out_vector_index_ids = nullptr);

    // See rewrite_partial_update for |flat_json_config|.
    static Status rewrite_auto_increment(const std::string& src_path, const std::string& dest_path,
                                         const TabletSchemaCSPtr& tschema,
                                         const std::shared_ptr<FlatJsonConfig>& flat_json_config,
                                         AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
                                         std::vector<uint32_t>& column_ids, MutableColumns* columns,
                                         SegmentFileMark segment_file_mark = {});
    // |filter_unowned| makes this rewrite keep only the rows |owned| marks, the same way
    // rewrite_partial_update_owned_only does, and |dest| then comes back with its own row count and
    // sort-key fields. It is a separate argument rather than |owned| being non-empty because the two
    // differ exactly where it matters: a publish iterator narrowed to this tablet's slice reports NO
    // mask (every row it emitted is this tablet's), and when that slice is empty the mask is empty
    // while the rewrite must still drop every source row.
    //
    // See rewrite_partial_update for |flat_json_config|.
    static Status rewrite_auto_increment_lake(
            const FileInfo& src, SegmentFileInfo* dest, const TabletSchemaCSPtr& tschema,
            const std::shared_ptr<FlatJsonConfig>& flat_json_config,
            starrocks::lake::AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
            const std::vector<uint32_t>& unmodified_column_ids, MutableColumns* unmodified_column_data,
            const starrocks::lake::Tablet* tablet, RewriteVectorIndexOptions vector_index_opts = {},
            std::vector<int64_t>* out_vector_index_ids = nullptr, const Filter& owned = Filter{},
            uint32_t emitted_rowid_base = 0, bool filter_unowned = false);
};

} // namespace starrocks
