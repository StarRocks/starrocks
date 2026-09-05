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
    static Status rewrite_partial_update(const FileInfo& src, FileInfo* dest,
                                         const std::shared_ptr<const TabletSchema>& tschema,
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
    // owned-only rewrites share. See the definition for why rows outside the emitted run go too.
    static Filter build_owned_selection(size_t num_rows, uint32_t emitted_rowid_base, const Filter& owned);

    static Status rewrite_partial_update_owned_only(
            const FileInfo& src, FileInfo* dest, const std::shared_ptr<const TabletSchema>& tschema,
            const std::vector<uint32_t>& resolved_column_ids, MutableColumns& resolved_columns, const Filter& owned,
            uint32_t emitted_rowid_base, uint32_t segment_id, const FooterPointerPB& partial_rowset_footer,
            SegmentFileMark segment_file_mark = {}, RewriteVectorIndexOptions vector_index_opts = {},
            std::vector<int64_t>* out_vector_index_ids = nullptr, size_t* out_num_rows = nullptr);

    static Status rewrite_auto_increment(const std::string& src_path, const std::string& dest_path,
                                         const TabletSchemaCSPtr& tschema,
                                         AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
                                         std::vector<uint32_t>& column_ids, MutableColumns* columns,
                                         SegmentFileMark segment_file_mark = {});
    static Status rewrite_auto_increment_lake(
            const FileInfo& src, FileInfo* dest, const TabletSchemaCSPtr& tschema,
            starrocks::lake::AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
            const std::vector<uint32_t>& unmodified_column_ids, MutableColumns* unmodified_column_data,
            const starrocks::lake::Tablet* tablet, RewriteVectorIndexOptions vector_index_opts = {},
            std::vector<int64_t>* out_vector_index_ids = nullptr, const Filter& owned = Filter{},
            uint32_t emitted_rowid_base = 0, size_t* out_num_rows = nullptr);
};

} // namespace starrocks
