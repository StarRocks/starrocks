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

#include "gutil/casts.h"

namespace starrocks::connector {

Status DataSourceProvider::prepare(RuntimeState* state) {
    return Status::OK();
}

Status DataSourceProvider::open(RuntimeState* state) {
    return Status::OK();
}

void DataSourceProvider::close(RuntimeState* state) {}

bool DataSourceProvider::insert_local_exchange_operator() const {
    return false;
}

bool DataSourceProvider::accept_empty_scan_ranges() const {
    return true;
}

bool DataSourceProvider::stream_data_source() const {
    return false;
}

Status DataSourceProvider::init(ObjectPool* pool, RuntimeState* state) {
    return Status::OK();
}

bool DataSourceProvider::always_shared_scan() const {
    return true;
}

void DataSourceProvider::peek_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {}

void DataSourceProvider::default_data_source_mem_bytes(int64_t* min_value, int64_t* max_value) {
    *min_value = MIN_DATA_SOURCE_MEM_BYTES;
    *max_value = MAX_DATA_SOURCE_MEM_BYTES;
}

bool DataSourceProvider::sorted_by_keys_per_tablet() const {
    return false;
}

bool DataSourceProvider::output_chunk_by_bucket() const {
    return false;
}

bool DataSourceProvider::is_asc_hint() const {
    return true;
}

std::optional<bool> DataSourceProvider::partition_order_hint() const {
    return std::nullopt;
}

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
