// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/row.h"

namespace starrocks {

class RowIterator {

public:

    virtual StatusOr<RowSharedPtr> get_next() = 0;

    virtual void close() = 0;
};

using RowIteratorSharedPtr = std::shared_ptr<RowIterator>;

} // namespace starrocks