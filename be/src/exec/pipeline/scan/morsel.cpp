// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/morsel.h"

#include "storage/tablet_reader_params.h"

namespace starrocks {
namespace pipeline {

/// Morsel.
void PhysicalSplitScanMorsel::init_tablet_reader_params(vectorized::TabletReaderParams* params) {
    params->rowset_id = _rowset_id;
    params->segment_id = _segment_id;
    params->rowid_range = _rowid_range;
}

/// MorselQueue.
std::vector<TInternalScanRange*> FixedMorselQueue::olap_scan_ranges() const {
    std::vector<TInternalScanRange*> scan_ranges;
    scan_ranges.reserve(_morsels.size());
    for (const auto& morsel : _morsels) {
        auto* scan_morsel = down_cast<ScanMorsel*>(morsel.get());
        auto* scan_range = scan_morsel->get_olap_scan_range();
        scan_ranges.emplace_back(scan_range);
    }
    return scan_ranges;
}

std::optional<MorselPtr> FixedMorselQueue::try_get() {
    auto idx = _pop_index.load();
    // prevent _num_morsels from superfluous addition
    if (idx >= _num_morsels) {
        return {};
    }
    idx = _pop_index.fetch_add(1);
    if (idx < _num_morsels) {
        return std::move(_morsels[idx]);
    } else {
        return {};
    }
}

} // namespace pipeline
} // namespace starrocks