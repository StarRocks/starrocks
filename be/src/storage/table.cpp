// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/table.h"

#include "storage/table_read_view_impl.h"

namespace starrocks {

TableReadViewSharedPtr Table::create_table_read_view(const TableReadViewParams& params) {
    return std::make_shared<TableReadViewImpl>(params, _tablet_ids);
}

} // namespace starrocks
