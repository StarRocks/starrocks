// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "connector/data_source_provider.h"

#include <algorithm>
#include <functional>

#include "exec/pipeline/scan/scan_morsel.h"

namespace starrocks::connector {

StatusOr<pipeline::MorselQueuePtr> DataSourceProvider::convert_scan_range_to_morsel_queue(
        const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
        bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
        size_t num_total_scan_ranges, size_t scan_parallelism) {
    peek_scan_ranges(scan_ranges);

    pipeline::Morsels morsels;
    bool has_more_morsel = false;
    pipeline::ScanMorsel::build_scan_morsels(node_id, scan_ranges, accept_empty_scan_ranges(), &morsels,
                                             &has_more_morsel);

    if (partition_order_hint().has_value()) {
        bool asc = partition_order_hint().value();
        std::stable_sort(morsels.begin(), morsels.end(), [asc](auto& l, auto& r) {
            auto l_partition_id = down_cast<pipeline::ScanMorsel*>(l.get())->partition_id();
            auto r_partition_id = down_cast<pipeline::ScanMorsel*>(r.get())->partition_id();
            if (asc) {
                return std::less()(l_partition_id, r_partition_id);
            } else {
                return std::greater()(l_partition_id, r_partition_id);
            }
        });
    }

    if (output_chunk_by_bucket()) {
        std::stable_sort(morsels.begin(), morsels.end(), [](auto& l, auto& r) {
            return down_cast<pipeline::ScanMorsel*>(l.get())->owner_id() <
                   down_cast<pipeline::ScanMorsel*>(r.get())->owner_id();
        });
    }

    auto morsel_queue = std::make_unique<pipeline::DynamicMorselQueue>(std::move(morsels), has_more_morsel);
    if (scan_parallelism > 0) {
        morsel_queue->set_max_degree_of_parallelism(scan_parallelism);
    }
    return morsel_queue;
}

} // namespace starrocks::connector
