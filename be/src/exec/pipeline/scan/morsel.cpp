// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/morsel.h"

#include "exec/olap_utils.h"
#include "storage/range.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"

namespace starrocks {
namespace pipeline {

/// MorselQueue.
std::vector<TInternalScanRange*> _convert_morsels_to_olap_scan_ranges(const Morsels& morsels) {
    std::vector<TInternalScanRange*> scan_ranges;
    scan_ranges.reserve(morsels.size());
    for (const auto& morsel : morsels) {
        auto* scan_morsel = down_cast<ScanMorsel*>(morsel.get());
        auto* scan_range = scan_morsel->get_olap_scan_range();
        scan_ranges.emplace_back(scan_range);
    }
    return scan_ranges;
}

std::vector<TInternalScanRange*> FixedMorselQueue::olap_scan_ranges() const {
    return _convert_morsels_to_olap_scan_ranges(_morsels);
}

StatusOr<MorselPtr> FixedMorselQueue::try_get() {
    auto idx = _pop_index.load();
    // prevent _num_morsels from superfluous addition
    if (idx >= _num_morsels) {
        return nullptr;
    }
    idx = _pop_index.fetch_add(1);
    if (idx < _num_morsels) {
        return std::move(_morsels[idx]);
    } else {
        return nullptr;
    }
}

} // namespace pipeline
} // namespace starrocks
