// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "storage/compaction_context.h"
#include "storage/compaction_policy.h"

namespace starrocks {

class CompactionTask;

// compaction policy for tablet,
// including cumulative compaction and base compaction
class BaseAndCumulativeCompactionPolicy : public CompactionPolicy {
public:
    BaseAndCumulativeCompactionPolicy(CompactionContext* compaction_context)
            : _compaction_context(compaction_context) {}
    ~BaseAndCumulativeCompactionPolicy() = default;

    // used to judge whether a tablet should do compaction or not
    bool need_compaction() override;

    // used to generate a CompactionTask for tablet
    std::shared_ptr<CompactionTask> create_compaction() override;

private:
    double _get_cumulative_compaction_score();
    void _pick_cumulative_rowsets(bool* has_delete_version, size_t* rowsets_compaction_score,
                                  std::vector<RowsetSharedPtr>* rowsets);
    bool _is_rowset_creation_time_ordered(const std::set<Rowset*, RowsetComparator>& level_0_rowsets);

    double _get_base_compaction_score();
    void _pick_base_rowsets(std::vector<RowsetSharedPtr>* rowsets);

    Status _check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);

    std::shared_ptr<CompactionTask> _create_cumulative_compaction();

    std::shared_ptr<CompactionTask> _create_base_compaction();

private:
    CompactionContext* _compaction_context;
};

} // namespace starrocks
