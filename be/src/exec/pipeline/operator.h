// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "gutil/casts.h"
#include "runtime/mem_tracker.h"
#include "util/runtime_profile.h"

namespace starrocks {
class Expr;
class ExprContext;
class RuntimeProfile;
class RuntimeState;
namespace pipeline {
class Operator;
using OperatorPtr = std::shared_ptr<Operator>;
using Operators = std::vector<OperatorPtr>;

class Operator {
    friend class PipelineDriver;

public:
    Operator(int32_t id, const std::string& name, int32_t plan_node_id);
    virtual ~Operator() = default;

    virtual Status prepare(RuntimeState* state);

    virtual Status close(RuntimeState* state);

    // Whether we could pull chunk from this operator
    virtual bool has_output() const = 0;

    // Whether we could push chunk to this operator
    virtual bool need_input() const = 0;

    // Is this operator completely finished processing and no more
    // output chunks will be produced
    virtual bool is_finished() const = 0;

    // Notifies the operator that no more input chunk will be added.
    // The operator should finish processing.
    // The method should be idempotent, because it may be triggered
    // multiple times in the entire life cycle
    // finish function is used to finish the following operator of the current operator that encounters its EOS
    // and has no data to push into its following operator, but the operator is not finished until its buffered
    // data inside is processed.
    virtual void set_finishing(RuntimeState* state) = 0;

    // set_finished is used to shutdown both input and output stream of a operator and after its invocation
    // buffered data inside the operator is cleared.
    // This function is used to shutdown preceding operators of the current operator if it is finished in advance,
    // when the query or fragment instance is canceled, set_finished is also called to shutdown unfinished operators.
    // A complex source operator that interacts with the corresponding sink operator in its preceding drivers via
    // an implementation-specific context should override set_finished function, such as LocalExchangeSourceOperator.
    // For an ordinary operator, set_finished function is trivial and just has the same implementation with
    // set_finishing function.
    virtual void set_finished(RuntimeState* state) { set_finishing(state); }

    // Pull chunk from this operator
    // Use shared_ptr, because in some cases (local broadcast exchange),
    // the chunk need to be shared
    virtual StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) = 0;

    // Push chunk to this operator
    virtual Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) = 0;

    int32_t get_id() const { return _id; }

    int32_t get_plan_node_id() const { return _plan_node_id; }

    RuntimeProfile* get_runtime_profile() const { return _runtime_profile.get(); }

    std::string get_name() const {
        std::stringstream ss;
        ss << _name + "_" << this;
        return ss.str();
    }

protected:
    int32_t _id = 0;
    std::string _name;
    // Which plan node this operator belongs to
    int32_t _plan_node_id = -1;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    std::unique_ptr<MemTracker> _mem_tracker;

    // Common metrics
    RuntimeProfile::Counter* _push_timer = nullptr;
    RuntimeProfile::Counter* _pull_timer = nullptr;

    RuntimeProfile::Counter* _push_chunk_num_counter = nullptr;
    RuntimeProfile::Counter* _push_row_num_counter = nullptr;
    RuntimeProfile::Counter* _pull_chunk_num_counter = nullptr;
    RuntimeProfile::Counter* _pull_row_num_counter = nullptr;
};

class OperatorFactory {
public:
    OperatorFactory(int32_t id, const std::string& name, int32_t plan_node_id)
            : _id(id), _name(name), _plan_node_id(plan_node_id) {}
    virtual ~OperatorFactory() = default;
    // Create the operator for the specific sequence driver
    // For some operators, when share some status, need to know the degree_of_parallelism
    virtual OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) = 0;
    virtual bool is_source() const { return false; }
    int32_t plan_node_id() const { return _plan_node_id; }
    virtual Status prepare(RuntimeState* state) { return Status::OK(); }
    virtual void close(RuntimeState* state) {}
    std::string get_name() const { return _name + "_" + std::to_string(_id); }

    // Local rf that take effects on this operator, and operator must delay to schedule to execution on core
    // util the corresponding local rf generated.
    const LocalRFWaitingSet& rf_waiting_set() const { return _rf_waiting_set; }

    // invoked by ExecNode::init_runtime_filter_for_operator to initialize fields involving runtime filter
    void init_runtime_filter(RuntimeFilterHub* runtime_filter_hub, const std::vector<TTupleId>& tuple_ids,
                             const LocalRFWaitingSet& rf_waiting_set, const RowDescriptor& row_desc,
                             const std::shared_ptr<RefCountedRuntimeFilterProbeCollector>& runtime_filter_collector,
                             std::vector<SlotId>&& filter_null_value_columns,
                             std::vector<TupleSlotMapping>&& tuple_slot_mappings) {
        _runtime_filter_hub = runtime_filter_hub;
        _tuple_ids = tuple_ids;
        _rf_waiting_set = rf_waiting_set;
        _row_desc = row_desc;
        _runtime_filter_collector = runtime_filter_collector;
        _filter_null_value_columns = std::move(filter_null_value_columns);
        _tuple_slot_mappings = std::move(tuple_slot_mappings);
    }
    // when a operator that waiting for local runtime filters' completion is waked, it call prepare_runtime_in_filters
    // to bound its runtime in-filters.
    void prepare_runtime_in_filters(RuntimeState* state) {
        // TODO(satanson): at present, prepare_runtime_in_filters is called in the PipelineDriverPoller thread sequentially,
        //  std::call_once's cost can be ignored, in the future, if mulitple PipelineDriverPollers are employed to dectect
        //  and wake blocked driver, std::call_once is sound but may be blocked.
        std::call_once(_prepare_runtime_in_filters_once, [this, state]() { this->_prepare_runtime_in_filters(state); });
    }

    RuntimeFilterHub* runtime_filter_hub() { return _runtime_filter_hub; }

    std::vector<ExprContext*>& get_runtime_in_filters() { return _runtime_in_filters; }
    RuntimeFilterProbeCollector* get_runtime_bloom_filters() {
        if (_runtime_filter_collector) {
            return _runtime_filter_collector->get_rf_probe_collector();
        } else {
            return nullptr;
        }
    }
    const std::vector<SlotId>& get_filter_null_value_columns() const { return _filter_null_value_columns; }

    void set_runtime_state(RuntimeState* state) { this->_state = state; }

    RuntimeState* runtime_state() { return _state; }

    RowDescriptor* row_desc() { return &_row_desc; }

protected:
    int32_t _id = 0;
    std::string _name;
    int32_t _plan_node_id = -1;
};

using OpFactoryPtr = std::shared_ptr<OperatorFactory>;
using OpFactories = std::vector<OpFactoryPtr>;

} // namespace pipeline
} // namespace starrocks
