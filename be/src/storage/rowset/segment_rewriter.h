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
    // See rewrite_partial_update for |flat_json_config|.
    static Status rewrite_auto_increment(const std::string& src_path, const std::string& dest_path,
                                         const TabletSchemaCSPtr& tschema,
                                         const std::shared_ptr<FlatJsonConfig>& flat_json_config,
                                         AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
                                         std::vector<uint32_t>& column_ids, MutableColumns* columns,
                                         SegmentFileMark segment_file_mark = {});
    // See rewrite_partial_update for |flat_json_config|.
    static Status rewrite_auto_increment_lake(
            const FileInfo& src, FileInfo* dest, const TabletSchemaCSPtr& tschema,
            const std::shared_ptr<FlatJsonConfig>& flat_json_config,
            starrocks::lake::AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
            const std::vector<uint32_t>& unmodified_column_ids, MutableColumns* unmodified_column_data,
            const starrocks::lake::Tablet* tablet, RewriteVectorIndexOptions vector_index_opts = {},
            std::vector<int64_t>* out_vector_index_ids = nullptr);
};

} // namespace starrocks
