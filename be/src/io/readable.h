// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#pragma once

#include "common/statusor.h"

namespace starrocks::io {

class Readable {
public:
    virtual ~Readable() = default;

    // Read at most |count| bytes data into |data|.
    // Returns the number of bytes read, 0 means EOF.
    virtual StatusOr<int64_t> read(void* data, int64_t count) = 0;
};

} // namespace starrocks::io
