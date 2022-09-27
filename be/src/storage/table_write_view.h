// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

namespace starrocks {

// TODO a view to write data to the underlying table
class TableWriteView {
public:
    void put() {}
    void put_batch() {}

    void commit(){};
};

using TableWriteViewSharedPtr = std::shared_ptr<TableWriteView>;

} // namespace starrocks