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

#include "common/statusor.h"
#include "connector/data_source.h"
#include "exec/pipeline/scan/morsel_queue_builder.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks {

class ExprContext;

namespace connector {

class DataSourceProvider {
public:
    static constexpr int64_t MIN_DATA_SOURCE_MEM_BYTES = 16 * 1024 * 1024;     // 16MB
    static constexpr int64_t DEFAULT_DATA_SOURCE_MEM_BYTES = 64 * 1024 * 1024; // 64MB
    static constexpr int64_t MAX_DATA_SOURCE_MEM_BYTES = 256 * 1024 * 1024;    // 256MB
    static constexpr int64_t PER_FIELD_MEM_BYTES = 1 * 1024 * 1024;            // 1MB

    virtual ~DataSourceProvider() = default;

    // First version we use TScanRange to define scan range
    // Later version we could use user-defined data.
    virtual DataSourcePtr create_data_source(const TScanRange& scan_range) = 0;
    // virtual DataSourcePtr create_data_source(const std::string& scan_range_spec)  = 0;

    // non-pipeline APIs
    Status prepare(RuntimeState* state) { return Status::OK(); }
    Status open(RuntimeState* state) { return Status::OK(); }
    void close(RuntimeState* state) {}

    // For some data source does not support scan ranges, dop is limited to 1,
    // and that will limit upper operators. And the solution is to insert a local exchange operator to fanout
    // and let upper operators have better parallelism.
    virtual bool insert_local_exchange_operator() const { return false; }

    // If this data source accept empty scan ranges, because for some data source there is no concept of scan ranges
    // such as MySQL/JDBC, so `accept_empty_scan_ranges` is false, and most in most cases, these data source(MySQL/JDBC)
    // the method `insert_local_exchange_operator` is true also.
    virtual bool accept_empty_scan_ranges() const { return true; }

    virtual Status init(ObjectPool* pool, RuntimeState* state) { return Status::OK(); }

    const std::vector<ExprContext*>& partition_exprs() const { return _partition_exprs; }
    const std::vector<TBucketProperty>& get_bucket_properties() const { return _bucket_properties; }

    virtual const TupleDescriptor* tuple_descriptor(RuntimeState* state) const = 0;

    virtual bool always_shared_scan() const { return true; }

    virtual void peek_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {}

    virtual void default_data_source_mem_bytes(int64_t* min_value, int64_t* max_value) {
        *min_value = MIN_DATA_SOURCE_MEM_BYTES;
        *max_value = MAX_DATA_SOURCE_MEM_BYTES;
    }

    virtual StatusOr<pipeline::MorselQueueBuilderPtr> convert_scan_range_to_morsel_queue_builder(
            const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
            bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
            size_t num_total_scan_ranges, size_t scan_parallelism = 0);

    int64_t get_scan_dop() const { return scan_dop; }

    // possible physical distribution optimize of data source
    virtual bool sorted_by_keys_per_tablet() const { return false; }
    virtual bool output_chunk_by_bucket() const { return false; }
    virtual bool is_asc_hint() const { return true; }
    virtual std::optional<bool> partition_order_hint() const { return std::nullopt; }

protected:
    std::vector<ExprContext*> _partition_exprs;
    std::vector<TBucketProperty> _bucket_properties;
    int64_t scan_dop = 0;
};

using DataSourceProviderPtr = std::unique_ptr<DataSourceProvider>;

} // namespace connector
} // namespace starrocks
