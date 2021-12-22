// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.

#include "segment_rewriter.h"

namespace starrocks {

namespace segment_v2 {

SegmentRewriter::SegmentRewriter() {}

SegmentRewriter::~SegmentRewriter() {}

Status SegmentRewriter::rewrite(const std::string& src, const std::string& dest, const TabletSchema& tschema,
                                std::vector<uint32_t>& column_ids,
                                std::vector<std::unique_ptr<vectorized::Column>>& columns) {
    // TODO: impl
    return Status::OK();
}

} // namespace segment_v2

} // namespace starrocks
