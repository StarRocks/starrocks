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

#include <mutex>

#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {

class OperatorFactory : public OperatorRuntimeAccess {
public:
    OperatorFactory(int32_t id, std::string name, int32_t plan_node_id);
    virtual ~OperatorFactory() = default;
    // Create the operator for the specific sequence driver
    // For some operators, when share some status, need to know the degree_of_parallelism
    virtual OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) = 0;
    virtual bool is_source() const { return false; }
    int32_t id() const { return _id; }
    int32_t plan_node_id() const { return _plan_node_id; }
    virtual Status prepare(RuntimeState* state);
    virtual void close(RuntimeState* state);
    std::string get_name() const { return _name + "_(" + std::to_string(_plan_node_id) + ")"; }
    std::string get_raw_name() const { return _name; }
    // Local rf that take effects on this operator, and operator must delay to schedule to execution on core
    // util the corresponding local rf generated.
    virtual const LocalRFWaitingSet& rf_waiting_set() const { return _rf_waiting_set; }
    void bind_runtime_in_filters(RuntimeState* state, int32_t driver_sequence,
                                 std::vector<ExprContext*>* runtime_in_filters) override;

    // Invoked by the ExecNode-to-pipeline adapter to initialize fields involving runtime filter.
    void init_runtime_filter(RuntimeFilterHub* runtime_filter_hub, const std::vector<TTupleId>& tuple_ids,
                             const LocalRFWaitingSet& rf_waiting_set, const RowDescriptor& row_desc,
                             const std::shared_ptr<RefCountedRuntimeFilterProbeCollector>& runtime_filter_collector,
                             const std::vector<SlotId>& filter_null_value_columns,
                             const std::vector<TupleSlotMapping>& tuple_slot_mappings) {
        _runtime_filter_hub = runtime_filter_hub;
        _tuple_ids = tuple_ids;
        _rf_waiting_set = rf_waiting_set;
        _row_desc = row_desc;
        _runtime_filter_collector = runtime_filter_collector;
        _filter_null_value_columns = filter_null_value_columns;
        _tuple_slot_mappings = tuple_slot_mappings;
    }
    RuntimeFilterHub* runtime_filter_hub() { return _runtime_filter_hub; }

    RuntimeFilterProbeCollector* get_runtime_bloom_filters() override {
        if (_runtime_filter_collector == nullptr) {
            return nullptr;
        }
        return _runtime_filter_collector->get_rf_probe_collector();
    }
    const RuntimeFilterProbeCollector* get_runtime_bloom_filters() const override {
        if (_runtime_filter_collector == nullptr) {
            return nullptr;
        }
        return _runtime_filter_collector->get_rf_probe_collector();
    }

    const std::vector<SlotId>& get_filter_null_value_columns() const override { return _filter_null_value_columns; }

    void set_runtime_state(RuntimeState* state) { this->_state = state; }

    RuntimeState* runtime_state() const { return _state; }

    RowDescriptor* row_desc() { return &_row_desc; }

    // Whether it has any runtime in-filter or bloom-filter.
    // MUST be invoked after init_runtime_filter.
    bool has_runtime_filters() const;

    // Whether it has any runtime filter built by TopN node.
    bool has_topn_filter() const;

    // try to get runtime filter from cache
    void acquire_runtime_filter(RuntimeState* state);

    virtual bool support_event_scheduler() const { return false; }

protected:
    // when an operator waiting for local runtime filters is woken, the factory prepares
    // the shared instance-level filters exactly once before binding them to operators.
    void prepare_runtime_in_filters(RuntimeState* state) {
        // TODO(satanson): at present, prepare_runtime_in_filters is called in the PipelineDriverPoller thread sequentially,
        //  std::call_once's cost can be ignored, in the future, if mulitple PipelineDriverPollers are employed to dectect
        //  and wake blocked driver, std::call_once is sound but may be blocked.
        std::call_once(_prepare_runtime_in_filters_once, [this, state]() { this->_prepare_runtime_in_filters(state); });
    }

    std::vector<ExprContext*>& get_runtime_in_filters() { return _runtime_in_filters; }
    // acquire local colocate runtime filter
    std::vector<ExprContext*> get_colocate_runtime_in_filters(size_t driver_sequence);
    void _prepare_runtime_in_filters(RuntimeState* state);
    void _prepare_runtime_holders(const std::vector<RuntimeFilterHolder*>& holders,
                                  std::vector<ExprContext*>* runtime_in_filters);

    const int32_t _id;
    const std::string _name;
    const int32_t _plan_node_id;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    RuntimeFilterHub* _runtime_filter_hub = nullptr;
    std::vector<TupleId> _tuple_ids;
    // a set of TPlanNodeIds of HashJoinNode who generates Local RF that take effects on this operator.
    LocalRFWaitingSet _rf_waiting_set;
    std::once_flag _prepare_runtime_in_filters_once;
    RowDescriptor _row_desc;
    std::vector<ExprContext*> _runtime_in_filters;
    std::shared_ptr<RefCountedRuntimeFilterProbeCollector> _runtime_filter_collector = nullptr;
    std::vector<SlotId> _filter_null_value_columns;
    // Mappings from input slot to output slot of ancestor exec nodes (include itself).
    // It is used to rewrite runtime in filters.
    std::vector<TupleSlotMapping> _tuple_slot_mappings;

    RuntimeState* _state = nullptr;
};

using OpFactoryPtr = std::shared_ptr<OperatorFactory>;
using OpFactories = std::vector<OpFactoryPtr>;

} // namespace starrocks::pipeline
