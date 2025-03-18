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
using RuntimeInFilter = starrocks::ExprContext;
using RuntimeBloomFilter = starrocks::RuntimeFilterBuildDescriptor;
using RuntimeBloomFilterProbeDescriptor = starrocks::RuntimeFilterProbeDescriptor;
using RuntimeBloomFilterProbeDescriptorPtr = RuntimeBloomFilterProbeDescriptor*;
using RuntimeBloomFilterRunningContext = RuntimeFilter::RunningContext;
using RuntimeInFilterPtr = RuntimeInFilter*;
using RuntimeBloomFilterPtr = RuntimeBloomFilter*;
using RuntimeInFilters = std::vector<RuntimeInFilterPtr>;
using RuntimeInFilterList = std::list<RuntimeInFilterPtr>;
using RuntimeBloomFilters = std::vector<RuntimeBloomFilterPtr>;
using RuntimeBloomFilterList = std::list<RuntimeBloomFilterPtr>;
struct RuntimeFilterCollector;
using RuntimeFilterCollectorPtr = std::unique_ptr<RuntimeFilterCollector>;
using RuntimeFilterProbeCollector = starrocks::RuntimeFilterProbeCollector;
using Predicate = starrocks::Predicate;
struct RuntimeBloomFilterBuildParam;
using OptRuntimeBloomFilterBuildParams = std::vector<std::optional<RuntimeBloomFilterBuildParam>>;
// Parameters used to build runtime bloom-filters.
struct RuntimeBloomFilterBuildParam {
    RuntimeBloomFilterBuildParam(bool multi_partitioned, bool eq_null, bool is_empty, Columns columns,
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
    RuntimeFilterCollector(RuntimeInFilterList&& in_filters, RuntimeBloomFilterList&& bloom_filters)
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
    RuntimeBloomFilterList _bloom_filters;
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
    PartialRuntimeFilterMerger(ObjectPool* pool, size_t local_rf_limit, size_t global_rf_limit)
            : _pool(pool), _local_rf_limit(local_rf_limit), _global_rf_limit(global_rf_limit) {}

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

    // HashJoinBuildOperator call add_partial_filters to gather partial runtime filters. the last HashJoinBuildOperator
    // will merge partial runtime filters into total one finally.
    StatusOr<bool> add_partial_filters(size_t idx, size_t ht_row_count, RuntimeInFilters&& partial_in_filters,
                                       OptRuntimeBloomFilterBuildParams&& partial_bloom_filter_build_params,
                                       RuntimeBloomFilters&& bloom_filter_descriptors) {
        DCHECK(idx < _partial_bloom_filter_build_params.size());
        // both _ht_row_counts, _partial_in_filters, _partial_bloom_filter_build_params are reserved beforehand,
        // each HashJoinBuildOperator mutates its corresponding slot indexed by driver_sequence, so concurrent
        // access need mutex to guard.
        _ht_row_counts[idx] = ht_row_count;
        _partial_in_filters[idx] = std::move(partial_in_filters);
        _partial_bloom_filter_build_params[idx] = std::move(partial_bloom_filter_build_params);

        return _try_do_merge(std::move(bloom_filter_descriptors));
    }

    RuntimeInFilterList get_total_in_filters() {
        // _partial_in_filters is empty means RF _is_always_true
        if (_partial_in_filters.empty()) return {};
        return {_partial_in_filters[0].begin(), _partial_in_filters[0].end()};
    }

    RuntimeBloomFilterList get_total_bloom_filters() {
        return {_bloom_filter_descriptors.begin(), _bloom_filter_descriptors.end()};
    }

    Status merge_local_in_filters() {
        bool can_merge_in_filters = true;
        size_t num_rows = 0;
        ssize_t k = -1;
        //squeeze _partial_in_filters and eliminate empty in-filter lists generated by empty hash tables.
        for (auto i = 0; i < _ht_row_counts.size(); ++i) {
            auto& in_filters = _partial_in_filters[i];
            // empty in-filter list is generated by empty hash tables, so skip it.
            if (_ht_row_counts[i] == 0) {
                continue;
            }
            // empty in-filter list is generated by non-empty hash tables(size>1024), in-filters can not be merged.
            if (in_filters.empty()) {
                can_merge_in_filters = false;
                break;
            }
            // move in-filter list indexed by i to slot indexed by k, eliminates holes in the middle.
            ++k;
            if (k < i) {
                _partial_in_filters[k] = std::move(_partial_in_filters[i]);
            }
            num_rows = std::max(num_rows, _ht_row_counts[i]);
        }

        can_merge_in_filters =
                can_merge_in_filters && (num_rows <= config::max_pushdown_conditions_per_column) && k >= 0;
        if (!can_merge_in_filters) {
            _partial_in_filters[0].clear();
            return Status::OK();
        }
        // only merge k partial in-filter list
        _partial_in_filters.resize(k + 1);

        auto& total_in_filters = _partial_in_filters[0];
        const auto num_in_filters = total_in_filters.size();
        for (auto i = 0; i < num_in_filters; ++i) {
            auto& total_in_filter = total_in_filters[i];
            if (total_in_filter == nullptr) {
                continue;
            }
            auto can_merge = std::all_of(_partial_in_filters.begin() + 1, _partial_in_filters.end(),
                                         [i](auto& in_filters) { return in_filters[i] != nullptr; });
            if (!can_merge) {
                total_in_filter = nullptr;
                continue;
            }
            for (int j = 1; j < _partial_in_filters.size(); ++j) {
                auto& in_filter = _partial_in_filters[j][i];
                DCHECK(in_filter != nullptr);
                auto* total_in_filter_pred = down_cast<Predicate*>(total_in_filter->root());
                auto* in_filter_pred = down_cast<Predicate*>(in_filter->root());
                RETURN_IF_ERROR(total_in_filter_pred->merge(in_filter_pred));
            }
        }
        total_in_filters.erase(std::remove(total_in_filters.begin(), total_in_filters.end(), nullptr),
                               total_in_filters.end());
        return Status::OK();
    }

    Status merge_local_bloom_filters() {
        if (_bloom_filter_descriptors.empty()) {
            return Status::OK();
        }
        auto multi_partitioned = _bloom_filter_descriptors[0]->layout().pipeline_level_multi_partitioned();
        if (multi_partitioned) {
            return merge_multi_partitioned_local_bloom_filters();
        } else {
            return merge_singleton_local_bloom_filters();
        }
    }

    Status merge_singleton_local_bloom_filters() {
        if (_partial_bloom_filter_build_params.empty()) {
            return Status::OK();
        }
        size_t row_count = 0;
        for (auto count : _ht_row_counts) {
            row_count += count;
        }

        for (auto& desc : _bloom_filter_descriptors) {
            desc->set_is_pipeline(true);
            // skip if it does not have consumer.
            if (!desc->has_consumer()) continue;
            // skip if ht.size() > limit, and it's only for local.
            if (!desc->has_remote_targets() && row_count > _local_rf_limit) continue;
            LogicalType build_type = desc->build_expr_type();
            auto rf = RuntimeFilterHelper::create_join_runtime_filter(_pool, build_type, desc->join_mode());
            if (rf == nullptr) continue;
            // set BF paramaters
            auto* bf = rf->get_bloom_filter();
            if (desc->has_remote_targets() && row_count > _global_rf_limit) {
                bf->clear_bf();
            } else {
                bf->init(row_count);
            }

            desc->set_runtime_filter(rf);
        }

        const auto& num_bloom_filters = _bloom_filter_descriptors.size();

        // remove empty params that generated in two cases:
        // 1. the corresponding HashJoinProbeOperator is finished in short-circuit style because HashJoinBuildOperator
        // above this operator has constructed an empty hash table.
        // 2. the HashJoinBuildOperator is finished in advance because the fragment instance is canceled
        _partial_bloom_filter_build_params.erase(
                std::remove_if(_partial_bloom_filter_build_params.begin(), _partial_bloom_filter_build_params.end(),
                               [](auto& opt_params) { return opt_params.empty(); }),
                _partial_bloom_filter_build_params.end());

        // there is no non-empty params, set all runtime filter to nullptr
        if (_partial_bloom_filter_build_params.empty()) {
            for (auto& desc : _bloom_filter_descriptors) {
                desc->set_runtime_filter(nullptr);
            }
            return Status::OK();
        }

        // all params must have the same size as num_bloom_filters
        DCHECK(std::all_of(_partial_bloom_filter_build_params.begin(), _partial_bloom_filter_build_params.end(),
                           [&num_bloom_filters](auto& opt_params) { return opt_params.size() == num_bloom_filters; }));

        for (auto i = 0; i < num_bloom_filters; ++i) {
            auto& desc = _bloom_filter_descriptors[i];
            if (desc->runtime_filter() == nullptr) {
                continue;
            }
            auto can_merge =
                    std::all_of(_partial_bloom_filter_build_params.begin(), _partial_bloom_filter_build_params.end(),
                                [i](auto& opt_params) { return opt_params[i].has_value(); });
            if (!can_merge) {
                desc->set_runtime_filter(nullptr);
                continue;
            }
            for (auto& opt_params : _partial_bloom_filter_build_params) {
                auto& opt_param = opt_params[i];
                DCHECK(opt_param.has_value());
                auto& param = opt_param.value();
                auto status = RuntimeFilterHelper::fill_runtime_bloom_filter(
                        param, desc->build_expr_type(), desc->runtime_filter(), kHashJoinKeyColumnOffset);
                if (!status.ok()) {
                    desc->set_runtime_filter(nullptr);
                    break;
                }
            }
        }
        return Status::OK();
    }

    Status merge_multi_partitioned_local_bloom_filters() {
        if (_partial_bloom_filter_build_params.empty()) {
            return Status::OK();
        }
        size_t row_count = 0;
        for (auto count : _ht_row_counts) {
            row_count += count;
        }
        for (auto& desc : _bloom_filter_descriptors) {
            desc->set_is_pipeline(true);
            // skip if it does not have consumer.
            if (!desc->has_consumer()) continue;
            // skip if ht.size() > limit, and it's only for local.
            if (!desc->has_remote_targets() && row_count > _local_rf_limit) continue;
            LogicalType build_type = desc->build_expr_type();
            auto rf = RuntimeFilterHelper::create_join_runtime_filter(_pool, build_type, desc->join_mode());
            if (rf == nullptr) continue;
            // set BF paramaters
            auto bf = rf->get_bloom_filter();
            if (desc->has_remote_targets() && row_count > _global_rf_limit) {
                bf->clear_bf();
            } else {
                bf->init(row_count);
            }

            desc->set_runtime_filter(rf);
        }

        const auto& num_bloom_filters = _bloom_filter_descriptors.size();

        // remove empty params that generated in two cases:
        // 1. the corresponding HashJoinProbeOperator is finished in short-circuit style because HashJoinBuildOperator
        // above this operator has constructed an empty hash table.
        // 2. the HashJoinBuildOperator is finished in advance because the fragment instance is canceled
        _partial_bloom_filter_build_params.erase(
                std::remove_if(_partial_bloom_filter_build_params.begin(), _partial_bloom_filter_build_params.end(),
                               [](auto& opt_params) { return opt_params.empty(); }),
                _partial_bloom_filter_build_params.end());

        // there is no non-empty params, set all runtime filter to nullptr
        if (_partial_bloom_filter_build_params.empty()) {
            for (auto& desc : _bloom_filter_descriptors) {
                desc->set_runtime_filter(nullptr);
            }
            return Status::OK();
        }

        // all params must have the same size as num_bloom_filters
        DCHECK(std::all_of(_partial_bloom_filter_build_params.begin(), _partial_bloom_filter_build_params.end(),
                           [&num_bloom_filters](auto& opt_params) { return opt_params.size() == num_bloom_filters; }));

        for (auto i = 0; i < num_bloom_filters; ++i) {
            auto& desc = _bloom_filter_descriptors[i];
            if (desc->runtime_filter() == nullptr) {
                continue;
            }
            auto can_merge =
                    std::all_of(_partial_bloom_filter_build_params.begin(), _partial_bloom_filter_build_params.end(),
                                [i](auto& opt_params) { return opt_params[i].has_value(); });
            if (!can_merge) {
                desc->set_runtime_filter(nullptr);
                continue;
            }
            auto* rf = desc->runtime_filter();
            for (auto& opt_params : _partial_bloom_filter_build_params) {
                auto& opt_param = opt_params[i];
                DCHECK(opt_param.has_value());
                auto& param = opt_param.value();
                if (param.is_empty) {
                    continue;
                }
                rf->concat(param.runtime_filter.get());
            }
        }
        return Status::OK();
    }

private:
    StatusOr<bool> _try_do_merge(RuntimeBloomFilters&& bloom_filter_descriptors) {
        if (1 == _num_active_builders--) {
            if (_always_true) {
                _partial_in_filters.clear();
                _bloom_filter_descriptors.clear();
                return true;
            }
            _bloom_filter_descriptors = std::move(bloom_filter_descriptors);
            RETURN_IF_ERROR(merge_local_in_filters());
            RETURN_IF_ERROR(merge_local_bloom_filters());
            return true;
        }
        return false;
    }

private:
    ObjectPool* _pool;
    const size_t _local_rf_limit;
    const size_t _global_rf_limit;
    std::atomic<bool> _always_true = false;
    std::atomic<size_t> _num_active_builders{0};
    std::vector<size_t> _ht_row_counts;
    std::vector<RuntimeInFilters> _partial_in_filters;
    std::vector<OptRuntimeBloomFilterBuildParams> _partial_bloom_filter_build_params;
    RuntimeBloomFilters _bloom_filter_descriptors;
};

} // namespace starrocks::pipeline
