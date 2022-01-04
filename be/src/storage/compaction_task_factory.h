// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "common/statusor.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"

namespace starrocks {

class Tablet;
class CompactionTask;

class CompactionTaskFactory {
public:
    CompactionTaskFactory(Version output_version, Tablet* tablet, std::vector<RowsetSharedPtr>&& input_rowsets,
                          double compaction_score, uint8_t compaction_level)
            : _output_version(output_version),
              _tablet(tablet),
              _input_rowsets(input_rowsets),
              _compaction_score(compaction_score),
              _compaction_level(compaction_level) {}
    ~CompactionTaskFactory() = default;

    std::shared_ptr<CompactionTask> create_compaction_task();

private:
    StatusOr<size_t> _get_segment_iterator_num();

private:
    Version _output_version;
    Tablet* _tablet;
    std::vector<RowsetSharedPtr> _input_rowsets;
    double _compaction_score;
    uint8_t _compaction_level;
};

} // namespace starrocks