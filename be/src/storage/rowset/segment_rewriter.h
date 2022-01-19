// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/olap_file.pb.h"

namespace starrocks {

class TabletSchema;

namespace vectorized {
class Column;
} // namespace vectorized

class SegmentRewriter {
public:
    SegmentRewriter();
    ~SegmentRewriter();

    // rewrite a segment file, add/replace some of it's columns
    // read from src, write to dest
    static Status rewrite(const std::string& src, const TabletSchema& tschema, std::vector<uint32_t>& column_ids,
                          std::vector<std::unique_ptr<vectorized::Column>>& columns, size_t segment_id,
                          FooterPointerPB& partial_rowseet_footer);
};

} // namespace starrocks
