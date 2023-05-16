// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "storage/compaction_policy.h"

namespace starrocks::vectorized {

class SizeTieredCompactionPolicy : public CompactionPolicy {
public:
    SizeTieredCompactionPolicy(Tablet* tablet);

    SizeTieredCompactionPolicy(const SizeTieredCompactionPolicy&) = delete;
    SizeTieredCompactionPolicy& operator=(const SizeTieredCompactionPolicy&) = delete;

    // used to judge whether a tablet should do compaction or not
    bool need_compaction(double* score, CompactionType* type) override;

    // used to generate a CompactionTask for tablet
    std::shared_ptr<CompactionTask> create_compaction(TabletSharedPtr tablet) override;

protected:
    Status _pick_rowsets_to_size_tiered_compact(bool force_base_compaction, std::vector<RowsetSharedPtr>* input_rowsets,
                                                double* score);
    double _cal_compaction_score(int64_t segment_num, int64_t level_size, int64_t total_size, KeysType keys_type,
                                 bool reached_max_version);
    Status _check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);

    Tablet* _tablet;
    std::vector<RowsetSharedPtr> _rowsets;
    double _score;
    CompactionType _compaction_type;
    int64_t _max_level_size;
    int64_t _level_multiple;
};

} // namespace starrocks::vectorized
