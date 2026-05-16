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

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "connector/data_source.h"
#include "exec/pipeline/scan/morsel.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks {

class ExprContext;
class ObjectPool;

namespace connector {

class DataSourceProvider {
public:
    static constexpr int64_t MIN_DATA_SOURCE_MEM_BYTES = 16 * 1024 * 1024;     // 16MB
    static constexpr int64_t DEFAULT_DATA_SOURCE_MEM_BYTES = 64 * 1024 * 1024; // 64MB
    static constexpr int64_t MAX_DATA_SOURCE_MEM_BYTES = 256 * 1024 * 1024;    // 256MB
    static constexpr int64_t PER_FIELD_MEM_BYTES = 1 * 1024 * 1024;            // 1MB

    virtual ~DataSourceProvider() = default;

    virtual DataSourcePtr create_data_source(const TScanRange& scan_range) = 0;

    // non-pipeline APIs
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual void close(RuntimeState* state);

    virtual bool insert_local_exchange_operator() const;
    virtual bool accept_empty_scan_ranges() const;
    virtual bool stream_data_source() const;
    virtual Status init(ObjectPool* pool, RuntimeState* state);

    const std::vector<ExprContext*>& partition_exprs() const { return _partition_exprs; }
    const std::vector<TBucketProperty>& get_bucket_properties() const { return _bucket_properties; }

    virtual const TupleDescriptor* tuple_descriptor(RuntimeState* state) const = 0;

    virtual bool always_shared_scan() const;
    virtual void peek_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges);
    virtual void default_data_source_mem_bytes(int64_t* min_value, int64_t* max_value);

    virtual StatusOr<pipeline::MorselQueuePtr> convert_scan_range_to_morsel_queue(
            const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
            bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
            size_t num_total_scan_ranges, size_t scan_parallelism = 0);

    int64_t get_scan_dop() const { return scan_dop; }

    // possible physical distribution optimize of data source
    virtual bool sorted_by_keys_per_tablet() const;
    virtual bool output_chunk_by_bucket() const;
    virtual bool is_asc_hint() const;
    virtual std::optional<bool> partition_order_hint() const;

protected:
    std::vector<ExprContext*> _partition_exprs;
    std::vector<TBucketProperty> _bucket_properties;
    int64_t scan_dop = 0;
};
using DataSourceProviderPtr = std::unique_ptr<DataSourceProvider>;

} // namespace connector
} // namespace starrocks
