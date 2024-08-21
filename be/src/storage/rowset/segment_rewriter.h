// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/rowset_update_state.h"

namespace starrocks {

class TabletSchema;

class Column;

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
                                         std::vector<uint32_t>& column_ids,
                                         std::vector<std::unique_ptr<Column>>& columns, uint32_t segment_id,
                                         const FooterPointerPB& partial_rowset_footer);
    // this funciton will append write_column to src_file and rebuild segment footer
    static Status rewrite(const std::string& src, const FileEncryptionInfo& einfo, const TabletSchemaCSPtr& tschema,
                          std::vector<uint32_t>& column_ids, std::vector<std::unique_ptr<Column>>& columns,
                          uint32_t segment_id, const FooterPointerPB& partial_rowseet_footer);
    static Status rewrite_auto_increment(const std::string& src_path, const std::string& dest_path,
                                         const TabletSchemaCSPtr& tschema,
                                         AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
                                         std::vector<uint32_t>& column_ids,
                                         std::vector<std::unique_ptr<Column>>* columns);
    static Status rewrite_auto_increment_lake(
            const FileInfo& src, FileInfo* dest, const TabletSchemaCSPtr& tschema,
            starrocks::lake::AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
            const std::vector<uint32_t>& unmodified_column_ids,
            std::vector<std::unique_ptr<Column>>* unmodified_column_data, const starrocks::lake::Tablet* tablet);
};

} // namespace starrocks
