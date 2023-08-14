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
    static Status rewrite(const std::string& src, const std::string& dest, const TabletSchema& tschema,
                          std::vector<uint32_t>& column_ids, std::vector<std::unique_ptr<Column>>& columns,
                          uint32_t segment_id, const FooterPointerPB& partial_rowseet_footer);
    // this funciton will append write_column to src_file and rebuild segment footer
    static Status rewrite(const std::string& src, const TabletSchema& tschema, std::vector<uint32_t>& column_ids,
                          std::vector<std::unique_ptr<Column>>& columns, uint32_t segment_id,
                          const FooterPointerPB& partial_rowseet_footer);
    static Status rewrite(const std::string& src_path, const std::string& dest_path, const TabletSchema& tschema,
                          AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
                          std::vector<uint32_t>& column_ids, std::vector<std::unique_ptr<Column>>* columns);
    static Status rewrite(const std::string& src_path, const std::string& dest_path, const TabletSchema& tschema,
                          starrocks::lake::AutoIncrementPartialUpdateState& auto_increment_partial_update_state,
                          std::vector<uint32_t>& column_ids, std::vector<std::unique_ptr<Column>>* columns,
                          const starrocks::lake::TxnLogPB_OpWrite& op_write, starrocks::lake::Tablet* tablet);
};

} // namespace starrocks
