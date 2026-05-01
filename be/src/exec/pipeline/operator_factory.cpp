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

#include "exec/pipeline/operator_factory.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "base/failpoint/fail_point.h"
#include "common/logging.h"
#include "common/runtime_profile.h"
#include "common/system/backend_options.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {

Operator::Operator(OperatorFactory* factory, int32_t id, std::string name, int32_t plan_node_id, bool is_subordinate,
                   int32_t driver_sequence)
        : Operator(factory, id, std::move(name), plan_node_id, is_subordinate, driver_sequence, factory) {}

OperatorFactory::OperatorFactory(int32_t id, std::string name, int32_t plan_node_id)
        : _id(id), _name(std::move(name)), _plan_node_id(plan_node_id) {
    std::string upper_name(_name);
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    _runtime_profile =
            std::make_shared<RuntimeProfile>(strings::Substitute("$0_factory (id=$1)", upper_name, _plan_node_id));
    _runtime_profile->set_metadata(_id);
}

Status OperatorFactory::prepare(RuntimeState* state) {
    FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
    _state = state;
    if (_runtime_filter_collector) {
        // TODO(hcf) no proper profile for rf_filter_collector attached to
        RETURN_IF_ERROR(_runtime_filter_collector->prepare(state, _runtime_profile.get()));
        acquire_runtime_filter(state);
    }
    return Status::OK();
}

void OperatorFactory::close(RuntimeState* state) {
    if (_runtime_filter_collector) {
        _runtime_filter_collector->close(state);
    }
}

void OperatorFactory::bind_runtime_in_filters(RuntimeState* state, int32_t driver_sequence,
                                              std::vector<ExprContext*>* runtime_in_filters) {
    auto colocate_runtime_in_filters = get_colocate_runtime_in_filters(driver_sequence);
    runtime_in_filters->insert(runtime_in_filters->end(), colocate_runtime_in_filters.begin(),
                               colocate_runtime_in_filters.end());

    prepare_runtime_in_filters(state);
    const auto& instance_runtime_filters = get_runtime_in_filters();
    runtime_in_filters->insert(runtime_in_filters->end(), instance_runtime_filters.begin(),
                               instance_runtime_filters.end());
}

void OperatorFactory::_prepare_runtime_in_filters(RuntimeState* state) {
    auto holders = _runtime_filter_hub->gather_holders(_rf_waiting_set, -1, true);
    _prepare_runtime_holders(holders, &_runtime_in_filters);
}

void OperatorFactory::_prepare_runtime_holders(const std::vector<RuntimeFilterHolder*>& holders,
                                               std::vector<ExprContext*>* runtime_in_filters) {
    for (auto& holder : holders) {
        DCHECK(holder->is_ready());
        auto* collector = holder->get_collector();

        collector->rewrite_in_filters(_tuple_slot_mappings);

        auto&& in_filters = collector->get_in_filters_bounded_by_tuple_ids(_tuple_ids);
        for (auto* filter : in_filters) {
            DCHECK(filter->opened());
            runtime_in_filters->push_back(filter);
        }
    }
}

std::vector<ExprContext*> OperatorFactory::get_colocate_runtime_in_filters(size_t driver_sequence) {
    std::vector<ExprContext*> runtime_in_filter;
    auto holders = _runtime_filter_hub->gather_holders(_rf_waiting_set, driver_sequence, true);
    _prepare_runtime_holders(holders, &runtime_in_filter);
    return runtime_in_filter;
}

bool OperatorFactory::has_runtime_filters() const {
    if (!_rf_waiting_set.empty()) {
        return true;
    }

    if (_runtime_filter_collector == nullptr) {
        return false;
    }
    auto* global_rf_collector = _runtime_filter_collector->get_rf_probe_collector();
    return global_rf_collector != nullptr && !global_rf_collector->descriptors().empty();
}

bool OperatorFactory::has_topn_filter() const {
    if (_runtime_filter_collector == nullptr) {
        return false;
    }
    auto* global_rf_collector = _runtime_filter_collector->get_rf_probe_collector();
    return global_rf_collector != nullptr && global_rf_collector->has_topn_filter();
}

void OperatorFactory::acquire_runtime_filter(RuntimeState* state) {
    if (_runtime_filter_collector == nullptr) {
        return;
    }
    auto& descriptors = _runtime_filter_collector->get_rf_probe_collector()->descriptors();
    for (auto& [filter_id, desc] : descriptors) {
        if (desc->is_local() || desc->runtime_filter(-1) != nullptr) {
            continue;
        }
        auto* query_execution_services = state->query_execution_services();
        auto* runtime_filter_cache = query_execution_services->runtime->runtime_filter_cache;
        auto grf = runtime_filter_cache->get(state->query_id(), filter_id);
        runtime_filter_cache->add_rf_event({state->query_id(), filter_id, BackendOptions::get_localhost(),
                                            strings::Substitute("INSTALL_GRF_TO_OPERATOR(op_id=$0, success=$1",
                                                                this->_plan_node_id, grf != nullptr)});

        if (grf == nullptr) {
            continue;
        }

        VLOG_FILE << "OperatorFactory::acquire_runtime_filter(shared). filter_id = " << filter_id
                  << ", filter = " << grf->debug_string();
        desc->set_shared_runtime_filter(grf);
    }
}

} // namespace starrocks::pipeline
