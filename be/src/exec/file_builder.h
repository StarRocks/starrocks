// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "fs/fs.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// this class is the base class of file builder, which defines the basic API of building any format of file
class FileBuilder {
public:
    virtual ~FileBuilder() = default;

    // appends this chunk to the file
    virtual Status add_chunk(vectorized::Chunk* chunk) = 0;

    // returns the size of underlying file or stream
    virtual std::size_t file_size() = 0;

    // close underlying file or stream properly, including flush and sync semantics
    virtual Status finish() = 0;
};

} // namespace starrocks