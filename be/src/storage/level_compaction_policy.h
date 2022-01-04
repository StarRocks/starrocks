// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "storage/compaction_context.h"
#include "storage/compaction_policy.h"

namespace starrocks {

class CompactionTask;

// Tablet构造CompactionContext，然后交给CompactionPolicy实时计算，不需要在tablet保留CompactionContext，这样子不会带来太大的内存开销

// Lazy level compaction policy for tablet
// Ref: Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree Based Key-Value Stores via Adaptive Removal of Superfluous Merging
class LevelCompactionPolicy : public CompactionPolicy {
public:
    LevelCompactionPolicy(CompactionContext* compaction_context) : _compaction_context(compaction_context) {}
    ~LevelCompactionPolicy() = default;

    // used to judge whether a tablet should do compaction or not
    bool need_compaction() override;

    // to calculate the compaction score of a tablet
    // which is used to decide the compaction sequence of tablets
    double compaction_score() override;

    // used to generator a CompactionTask for tablet
    std::shared_ptr<CompactionTask> create_compaction() override;

private:
    double _get_level_0_compaction_score();
    void _pick_level_0_rowsets(bool* has_delete_version, size_t* rowsets_compaction_score,
                               std::vector<RowsetSharedPtr>* rowsets);
    bool _is_rowset_creation_time_ordered(const std::set<Rowset*, RowsetComparator>& level_0_rowsets);

    double _get_level_1_compaction_score();
    void _pick_level_1_rowsets(std::vector<RowsetSharedPtr>* rowsets);

    Status _check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);

    std::shared_ptr<CompactionTask> _create_level_0_compaction();

    std::shared_ptr<CompactionTask> _create_level_1_compaction();

private:
    CompactionContext* _compaction_context;
};

} // namespace starrocks
