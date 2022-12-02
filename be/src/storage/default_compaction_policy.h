// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "storage/compaction_policy.h"

namespace starrocks::vectorized {

class DefaultCumulativeBaseCompactionPolicy : public CompactionPolicy {
public:
    DefaultCumulativeBaseCompactionPolicy(Tablet* tablet) : _tablet(tablet) {}

    DefaultCumulativeBaseCompactionPolicy(const DefaultCumulativeBaseCompactionPolicy&) = delete;
    DefaultCumulativeBaseCompactionPolicy& operator=(const DefaultCumulativeBaseCompactionPolicy&) = delete;

    // used to judge whether a tablet should do compaction or not
    bool need_compaction(double* score, CompactionType* type) override;

    // used to generate a CompactionTask for tablet
    std::shared_ptr<CompactionTask> create_compaction(TabletSharedPtr tablet) override;

protected:
    Status _pick_rowsets_to_cumulative_compact(std::vector<RowsetSharedPtr>* input_rowsets, double* score);
    Status _pick_rowsets_to_base_compact(std::vector<RowsetSharedPtr>* input_rowsets, double* score);

    // _check_version_continuity_with_cumulative_point checks whether the input rowsets is continuous with cumulative point.
    Status _check_version_continuity_with_cumulative_point(const std::vector<RowsetSharedPtr>& rowsets);

    bool _fit_compaction_condition(const std::vector<RowsetSharedPtr>& rowsets, double compaction_score);

    Status _check_rowset_overlapping(const std::vector<RowsetSharedPtr>& rowsets);
    Status _check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);
    Status _check_version_overlapping(const std::vector<RowsetSharedPtr>& rowsets);

    Tablet* _tablet;
    std::vector<RowsetSharedPtr> _cumulative_rowsets;
    std::vector<RowsetSharedPtr> _base_rowsets;
    double _cumulative_score = 0;
    double _base_score = 0;
    CompactionType _compaction_type = INVALID_COMPACTION;
};

} // namespace starrocks::vectorized
