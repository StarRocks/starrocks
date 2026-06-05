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

#include <atomic>
#include <list>
#include <memory>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "exec/runtime_filter/runtime_filter_descriptor.h"
#include "exec/runtime_filter/runtime_filter_probe.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Types_types.h"

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
using RuntimeFilterProbeCollector = starrocks::RuntimeFilterProbeCollector;

// RuntimeFilterCollector contains runtime in-filters and bloom-filters, it is stored in RuntimeFilerHub
// and every HashJoinBuildOperatorFactory has its corresponding RuntimeFilterCollector.
struct RuntimeFilterCollector {
    RuntimeFilterCollector(RuntimeInFilterList&& in_filters, RuntimeMembershipFilterList&& bloom_filters)
            : _in_filters(std::move(in_filters)), _bloom_filters(std::move(bloom_filters)) {
        for (auto filter : _in_filters) {
            DCHECK(filter->opened());
        }
    }

    RuntimeFilterCollector(RuntimeInFilterList in_filters) : _in_filters(std::move(in_filters)) {
        for (auto filter : _in_filters) {
            DCHECK(filter->opened());
        }
    }

    RuntimeInFilterList& get_in_filters() { return _in_filters; }

    static Status prepare_runtime_in_filters(RuntimeState* state, RuntimeInFilterList& in_filters) {
        for (auto& in_filter : in_filters) {
            RETURN_IF_ERROR(in_filter->prepare(state));
            RETURN_IF_ERROR(in_filter->open(state));
        }
        return Status::OK();
    }

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
        RuntimeFilterCollector* expected = nullptr;

        if (_collector.compare_exchange_strong(expected, collector.get(), std::memory_order_release,
                                               std::memory_order_acquire)) {
            _collector_ownership = std::move(collector);
        }
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

    // If strict is false, return instance level holder if not found pipeline level holder.
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
    // For instance level runtime filters, the sequence_id is -1.
    // For pipeline level runtime filters, the sequence_id is corresponding operator driver sequence.
    std::unordered_map<TPlanNodeId, SequenceToHolder> _holders;
};

} // namespace starrocks::pipeline
