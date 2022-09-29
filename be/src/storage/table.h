// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "storage/table_read_view.h"
#include "storage/table_write_view.h"
#include "storage/tablet.h"

namespace starrocks {

// Representation of a table. It includes the meta of the table such as table id,
// table name, information of local (on the same BE) and remote tablets. Operators
// should only see this table abstraction, and create read/write view via the table.
// And we can extend this abstraction to do more things in the future.
class Table {
public:
    // TODO construct with IMTStateTable
    explicit Table(std::string table_name, int64_t table_id, const std::vector<int64_t>& tablet_ids)
            : _table_name(table_name), _table_id(table_id), _tablet_ids(tablet_ids) {}

    // create a read view according to the parameters
    TableReadViewSharedPtr create_table_read_view(const TableReadViewParams& params);

    // TODO create write view
    TableWriteViewSharedPtr create_table_write_view() { return nullptr; }

private:
    std::string _table_name;
    int64_t _table_id;
    std::vector<int64_t> _tablet_ids;
};

using TableSharedPtr = std::shared_ptr<Table>;

} // namespace starrocks