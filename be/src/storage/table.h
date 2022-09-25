// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "storage/tablet.h"
#include "storage/table_read_view.h"
#include "storage/table_write_view.h"

namespace starrocks {

// Representation of a table. It includes the meta of the table such as table id,
// table name, information of local (on the same BE) and remote tablets. Operators
// should only see this table abstraction, and create read/write view via the table.
// And we can extend this abstraction to do more things in the future.
class Table {
public:

    // TODO for PoC, the table only has the information of the local tablet read
    // by the operator. And table id and name are useless currently
    explicit Table(TableId table_id, std::string table_name, TabletSharedPtr tablet)
            : _table_id(table_id),
              _table_name(std::move(table_name)),
              _tablet(std::move(tablet)) {}

    // create a read view according to the parameters
    TableReadViewSharedPtr create_table_read_view(const TableReadViewParams& params);

    // TODO create write view
    TableWriteViewSharedPtr create_table_write_view() { return nullptr; }

private:

    TableId _table_id;
    std::string _table_name;
    TabletSharedPtr _tablet;
};

using TableSharedPtr = std::shared_ptr<Table>;

} // namespace starrocks