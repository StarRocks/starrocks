//
// Created by 郑志铨 on 2021/11/13.
//

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

    virtual uint64_t file_size() = 0;

    virtual Status finish() = 0;
};

}