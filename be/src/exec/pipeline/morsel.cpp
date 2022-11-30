// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/morsel.h"

#include "storage/rowset/rowset.h"

namespace starrocks::pipeline {

void Morsel::set_rowsets(std::vector<RowsetSharedPtr> rowsets) {
    _rowsets = std::move(rowsets);
}

const std::vector<RowsetSharedPtr>& Morsel::rowsets() const {
    return _rowsets;
}

void MorselQueue::set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) {
    _tablet_rowsets = tablet_rowsets;
}

} // namespace starrocks::pipeline