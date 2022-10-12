// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "common/statusor.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks {

class CompactionTask;

class CompactionTaskFactory {
public:
    CompactionTaskFactory(Version output_version, const TabletSharedPtr& tablet,
                          std::vector<RowsetSharedPtr>&& input_rowsets, double compaction_score,
                          CompactionType compaction_type)
            : _output_version(output_version),
              _tablet(tablet),
              _input_rowsets(std::move(input_rowsets)),
              _compaction_score(compaction_score),
              _compaction_type(compaction_type) {}
    ~CompactionTaskFactory() = default;

    std::shared_ptr<CompactionTask> create_compaction_task();

private:
    Version _output_version;
    TabletSharedPtr _tablet;
    std::vector<RowsetSharedPtr> _input_rowsets;
    double _compaction_score;
    CompactionType _compaction_type;
};

} // namespace starrocks