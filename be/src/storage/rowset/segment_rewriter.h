// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"

namespace starrocks {

class TabletSchema;

namespace vectorized {
class Column;
} // namespace vectorized

namespace segment_v2 {

class SegmentRewriter {
public:
    SegmentRewriter();
    ~SegmentRewriter();

    // rewrite a segment file, add/replace some of it's columns
    // read from src, write to dest
    static Status rewrite(const std::string& src, const std::string& dest, const TabletSchema& tschema,
                          std::vector<uint32_t>& column_ids, std::vector<std::unique_ptr<vectorized::Column>>& columns,
                          size_t segment_id);
};

} // namespace segment_v2
} // namespace starrocks
