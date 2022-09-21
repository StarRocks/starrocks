// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/row.h"

namespace starrocks {

using RowPtr = std::shared_ptr<Row>;

class RowIterator {

public:

    virtual StatusOr<RowPtr> get_next() = 0;

    virtual void close() = 0;
};

} // namespace starrocks