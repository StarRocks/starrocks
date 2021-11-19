// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "env/env.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class FileBuilder {
public:
    virtual ~FileBuilder() = default;

    virtual Status add_chunk(vectorized::Chunk* chunk) = 0;

    virtual std::size_t file_size() = 0;

    virtual Status finish() = 0;
};

} // namespace starrocks