// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"

namespace starrocks {
namespace pipeline {
class CrossJoinContext {
public:
    CrossJoinContext() : _build_chunk(std::make_shared<vectorized::Chunk>()), _right_table_complete(false) {}

    // Used in operators to reference right table's datas.
    vectorized::ChunkPtr _build_chunk;
    // used in operators to mark that the right table has been constructed.
    std::atomic<bool> _right_table_complete;
};

} // namespace pipeline
} // namespace starrocks
