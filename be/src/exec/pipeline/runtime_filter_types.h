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
#include <mutex>
#include <utility>

#include "common/statusor.h"
#include "exec/pipeline/hashjoin/hash_joiner_fwd.h"
#include "exec/pipeline/schedule/observer.h"
#include "exprs/expr_context.h"
#include "exprs/predicate.h"
#include "exprs/runtime_filter_bank.h"
#include "gen_cpp/Types_types.h"
#include "gutil/casts.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {

class RuntimeFilterHolder;
using RuntimeFilterHolderPtr = std::unique_ptr<RuntimeFilterHolder>;
// TODO: rename RuntimeInFilter
using RuntimeInFilter = ExprContext;
using RuntimeMembershipFilter = RuntimeFilterBuildDescriptor;
using RuntimeMembershipFilterProbeDescriptor = RuntimeFilterProbeDescriptor;
using RuntimeMembershipFilterProbeDescriptorPtr = RuntimeMembershipFilterProbeDescriptor*;
using RuntimeMembershipFilterRunningContext = RuntimeFilter::RunningContext;
using RuntimeInFilterPtr = RuntimeInFilter*;
using RuntimeMembershipFilterPtr = RuntimeMembershipFilter*;
using RuntimeInFilters = std::vector<RuntimeInFilterPtr>;
using RuntimeInFilterList = std::list<RuntimeInFilterPtr>;
using RuntimeMembershipFilters = std::vector<RuntimeMembershipFilterPtr>;
using RuntimeMembershipFilterList = std::list<RuntimeMembershipFilterPtr>;
struct RuntimeFilterCollector;
using RuntimeFilterCollectorPtr = std::unique_ptr<RuntimeFilterCollector>;
using RuntimeFilterProbeCollector = RuntimeFilterProbeCollector;
struct RuntimeMembershipFilterBuildParam;
using OpTRuntimeBloomFilterBuildParams = std::vector<std::optional<RuntimeMembershipFilterBuildParam>>;

// Parameters used to build runtime bloom-filters.
struct RuntimeMembershipFilterBuildParam {
    RuntimeMembershipFilterBuildParam(bool multi_partitioned, bool eq_null, bool is_empty, Columns columns,
                                      MutableRuntimeFilterPtr runtime_filter)
            : multi_partitioned(multi_partitioned),
              eq_null(eq_null),
              is_empty(is_empty),
              columns(std::move(columns)),
              runtime_filter(std::move(runtime_filter)) {}
    bool multi_partitioned;
    bool eq_null;
    bool is_empty;
    Columns columns;
    MutableRuntimeFilterPtr runtime_filter;
};

// RuntimeFilterCollector contains runtime in-filters and bloom-filters, it is stored in RuntimeFilerHub
// and every HashJoinBuildOperatorFactory has its corresponding RuntimeFilterCollector.
struct RuntimeFilterCollector {
    RuntimeFilterCollector(RuntimeInFilterList&& in_filters, RuntimeMembershipFilterList&& bloom_filters)
            : _in_filters(std::move(in_filters)), _bloom_filters(std::move(bloom_filters)) {}
    RuntimeFilterCollector(RuntimeInFilterList in_filters) : _in_filters(std::move(in_filters)) {}

    RuntimeInFilterList& get_in_filters() { return _in_filters; }

    // In-filters are constructed by a node and may be pushed down to its descendant node.
    // Different tuple id and slot id between descendant and ancestor nodes may be referenced to the same column,
    // such as ProjectNode, so we need use ancestor's tuple slot mappings to rewrite in filters.
    void rewrite_in_filters(const std::vector<TupleSlotMapping>& mappings) {
        std::vector<TupleId> tuple_ids(1);
        for (const auto& mapping : mappings) {
            tuple_ids[0] = mapping.to_tuple_id;

            for (auto in_filter : _in_filters) {
                if (!in_filter->root()->is_bound(tuple_ids)) {
                    continue;
                }

                DCHECK(nullptr != dynamic_cast<ColumnRef*>(in_filter->root()->get_child(0)));
                auto column = ((ColumnRef*)in_filter->root()->get_child(0));

                if (column->slot_id() == mapping.to_slot_id) {
                    column->set_slot_id(mapping.from_slot_id);
                    column->set_tuple_id(mapping.from_tuple_id);
                }
            }
        }
    }

    std::vector<RuntimeInFilterPtr> get_in_filters_bounded_by_tuple_ids(const std::vector<TupleId>& tuple_ids) {
        std::vector<ExprContext*> selected_in_filters;
        for (auto* in_filter : _in_filters) {
            if (in_filter->root()->is_bound(tuple_ids)) {
                selected_in_filters.push_back(in_filter);
            }
        }
        return selected_in_filters;
    }

private:
    // local runtime in-filter
    RuntimeInFilterList _in_filters;
    // TODO: unused, FIXME later
    // global/local runtime bloom-filter(including max-min filter)
    RuntimeMembershipFilterList _bloom_filters;
};

class RuntimeFilterHolder {
public:
    void set_collector(RuntimeFilterCollectorPtr&& collector) {
        DCHECK(_collector.load(std::memory_order_acquire) == nullptr);
        _collector_ownership = std::move(collector);
        _collector.store(_collector_ownership.get(), std::memory_order_release);
    }
    RuntimeFilterCollector* get_collector() { return _collector.load(std::memory_order_acquire); }
    bool is_ready() { return get_collector() != nullptr; }

    void add_observer(RuntimeState* state, PipelineObserver* observer) {
        _local_rf_observable.add_observer(state, observer);
    }

    auto notify() { _local_rf_observable.notify_source_observers(); }
    Observable& observer() { return _local_rf_observable; }

private:
    Observable _local_rf_observable;
    RuntimeFilterCollectorPtr _collector_ownership;
    std::atomic<RuntimeFilterCollector*> _collector;
};

// RuntimeFilterHub is a mediator that used to gather all runtime filters generated by RuntimeFilterBuild instances.
// The life cycle of RuntimeFilterHub is the same as FragmentContext.
// RuntimeFilterHub maintains the mapping from RuntimeFilter Subscriber to RuntimeFilter Producer (RuntimeFilter Holder).
// Subscriber can be operator level (node_id, sequence) -> holder. or operator factory level (node_id, -1) -> holder.
// Typically, the operator level is used in colocate group execution mode, and the operator factory level is used in other modes.
class RuntimeFilterHub {
public:
    void add_holder(TPlanNodeId id, int32_t total_dop = -1) {
        if (total_dop > 0) {
            for (size_t i = 0; i < total_dop; ++i) {
                _holders[id].emplace(i, std::make_unique<RuntimeFilterHolder>());
            }
        } else {
            _holders[id].emplace(-1, std::make_unique<RuntimeFilterHolder>());
        }
    }

    void set_collector(TPlanNodeId id, RuntimeFilterCollectorPtr&& collector) {
        auto holder = get_holder(id, -1);
        holder->set_collector(std::move(collector));
        holder->notify();
    }

    void set_collector(TPlanNodeId id, int32_t sequence_id, RuntimeFilterCollectorPtr&& collector) {
        auto holder = get_holder(id, sequence_id);
        holder->set_collector(std::move(collector));
        holder->notify();
    }

    void close_all_in_filters(RuntimeState* state) {
        for (auto& [plan_node_id, seq_to_holder] : _holders) {
            for (const auto& [seq, holder] : seq_to_holder) {
                if (auto* collector = holder->get_collector()) {
                    for (auto& in_filter : collector->get_in_filters()) {
                        in_filter->close(state);
                    }
                }
            }
        }
    }

    bool is_colocate_runtime_filters(TPlanNodeId plan_node_id) const {
        auto it = _holders.find(plan_node_id);
        DCHECK(it != _holders.end());
        return it->second.find(-1) == it->second.end();
    }

    //  if strict is false, return instance level holder if not found pipeline level holder
    std::vector<RuntimeFilterHolder*> gather_holders(const std::set<TPlanNodeId>& ids, size_t driver_sequence,
                                                     bool strict = false) {
        std::vector<RuntimeFilterHolder*> holders;
        holders.reserve(ids.size());
        for (auto id : ids) {
            if (auto holder = get_holder(id, driver_sequence, strict)) {
                holders.push_back(holder);
            }
        }
        return holders;
    }

private:
    RuntimeFilterHolder* get_holder(TPlanNodeId id, int32_t sequence_id, bool strict = false) {
        auto it = _holders.find(id);
        DCHECK(it != _holders.end());
        auto it_holder = it->second.find(sequence_id);
        if (it_holder == it->second.end()) {
            if (strict) return nullptr;
            it_holder = it->second.find(-1);
            DCHECK(it_holder != it->second.end());
            return it_holder->second.get();
        }
        return it_holder->second.get();
    }

    using SequenceToHolder = std::unordered_map<int32_t, RuntimeFilterHolderPtr>;
    // Each HashJoinBuildOperatorFactory has a corresponding Holder indexed by its TPlanNodeId.
    // For instance level runtime filters, the sequence_id is -1
    // For pipeline level runtime filters, the sequence_id is corresponding operator driver sequence
    std::unordered_map<TPlanNodeId, SequenceToHolder> _holders;
};

// A ExecNode in non-pipeline engine can be decomposed into more than one OperatorFactories in pipeline engine.
// Pipeline framework do not care about that runtime filters take affects on which OperatorFactories, since
// it depends on Operators' implementation. so each OperatorFactory from the same ExecNode shared a
// RefCountedRuntimeFilterProbeCollector, in which refcount is introduced to guarantee that both prepare and
// close method of the RuntimeFilterProbeCollector inside this wrapper object is called only exactly-once.
class RefCountedRuntimeFilterProbeCollector;
using RefCountedRuntimeFilterProbeCollectorPtr = std::shared_ptr<RefCountedRuntimeFilterProbeCollector>;
class RefCountedRuntimeFilterProbeCollector {
public:
    RefCountedRuntimeFilterProbeCollector(size_t num_factories_generated,
                                          RuntimeFilterProbeCollector&& rf_probe_collector)
            : _count((num_factories_generated << 32) | num_factories_generated),
              _num_operators_generated(num_factories_generated),
              _rf_probe_collector(std::move(rf_probe_collector)) {}

    template <class... Args>
    Status prepare(RuntimeState* state, Args&&... args) {
        // TODO: stdpain assign operator nums here
        if ((_count.fetch_sub(1) & PREPARE_COUNTER_MASK) == _num_operators_generated) {
            RETURN_IF_ERROR(_rf_probe_collector.prepare(state, std::forward<Args>(args)...));
            RETURN_IF_ERROR(_rf_probe_collector.open(state));
        }
        return Status::OK();
    }

    void close(RuntimeState* state) {
        static constexpr size_t k = 1ull << 32;
        if ((_count.fetch_sub(k) & CLOSE_COUNTER_MASK) == k) {
            _rf_probe_collector.close(state);
        }
    }

    RuntimeFilterProbeCollector* get_rf_probe_collector() { return &_rf_probe_collector; }
    const RuntimeFilterProbeCollector* get_rf_probe_collector() const { return &_rf_probe_collector; }

private:
    static constexpr size_t PREPARE_COUNTER_MASK = 0xffff'ffffull;
    static constexpr size_t CLOSE_COUNTER_MASK = 0xffff'ffff'0000'0000ull;

    // a refcount, low 32 bit used count the close invocation times, and the high 32 bit used to count the
    // prepare invocation times.
    std::atomic<size_t> _count;
    // how many OperatorFactories into whom a ExecNode is decomposed.
    const size_t _num_operators_generated;
    // a wrapped RuntimeFilterProbeCollector initialized by a ExecNode, which contains runtime bloom filters.
    RuntimeFilterProbeCollector _rf_probe_collector;
};

// Used to merge runtime in-filters and bloom-filters generated by multiple HashJoinBuildOperator instances.
// When more than one HashJoinBuildOperator instances are appended to LocalExchangeSourceOperator(PartitionExchanger)
// instances, the build side table is partitioned, and each HashJoinBuildOperator instance is applied to the
// partition and a partial filter is generated after hash table has been constructed. the partial filters can
// not take effects on operators in front of LocalExchangeSourceOperators before they are merged into a total one.
class PartialRuntimeFilterMerger {
public:
    PartialRuntimeFilterMerger(ObjectPool* pool, size_t local_rf_limit, size_t global_rf_limit, int func_version)
            : _pool(pool),
              _local_rf_limit(local_rf_limit),
              _global_rf_limit(global_rf_limit),
              _func_version(func_version) {}

    void incr_builder() {
        _ht_row_counts.emplace_back(0);
        _partial_in_filters.emplace_back();
        _partial_bloom_filter_build_params.emplace_back();
        _num_active_builders++;
    }

    // mark runtime_filter as always true.
    StatusOr<bool> set_always_true() {
        _always_true = true;
        return _try_do_merge({});
    }

    RuntimeInFilterList get_total_in_filters() {
        // _partial_in_filters is empty means RF _is_always_true
        if (_partial_in_filters.empty()) return {};
        return {_partial_in_filters[0].begin(), _partial_in_filters[0].end()};
    }

    RuntimeMembershipFilterList get_total_bloom_filters() {
        return {_bloom_filter_descriptors.begin(), _bloom_filter_descriptors.end()};
    }

    // HashJoinBuildOperator call add_partial_filters to gather partial runtime filters. the last HashJoinBuildOperator
    // will merge partial runtime filters into total one finally.
    StatusOr<bool> add_partial_filters(size_t idx, size_t ht_row_count, RuntimeInFilters&& partial_in_filters,
                                       OpTRuntimeBloomFilterBuildParams&& partial_bloom_filter_build_params,
                                       RuntimeMembershipFilters&& bloom_filter_descriptors);

    Status merge_local_in_filters();
    Status merge_local_bloom_filters();
    Status merge_singleton_local_bloom_filters();
    Status merge_multi_partitioned_local_bloom_filters();

private:
    StatusOr<bool> _try_do_merge(RuntimeMembershipFilters&& bloom_filter_descriptors);

private:
    ObjectPool* _pool;

    const size_t _local_rf_limit;
    const size_t _global_rf_limit;
    const int _func_version;

    std::atomic<bool> _always_true{false};
    std::atomic<size_t> _num_active_builders{0};
    std::vector<size_t> _ht_row_counts;
    std::vector<RuntimeInFilters> _partial_in_filters;
    std::vector<OpTRuntimeBloomFilterBuildParams> _partial_bloom_filter_build_params;
    RuntimeMembershipFilters _bloom_filter_descriptors;
};

} // namespace starrocks::pipeline
