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
            std::vector<int64_t>* out_vector_index_ids = nullptr);
};

} // namespace starrocks
