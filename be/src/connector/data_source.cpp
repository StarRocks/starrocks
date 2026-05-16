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

#include "connector/data_source.h"

#include "exec/runtime_filter/runtime_filter_helper.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks::connector {

const std::string DataSource::PROFILE_NAME = "DataSource";

Status DataSource::open(RuntimeState* state) {
    return Status::OK();
}

void DataSource::close(RuntimeState* state) {}

Status DataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    return Status::OK();
}

int64_t DataSource::io_time_spent() const {
    return 0;
}

bool DataSource::can_estimate_mem_usage() const {
    return false;
}

int64_t DataSource::estimated_mem_usage() const {
    return 0;
}

const std::string DataSource::get_custom_coredump_msg() const {
    return "";
}

void DataSource::get_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {}

void DataSource::update_has_any_predicate() {
    auto f = [&]() {
        if (_conjunct_ctxs.size() > 0) return true;
        if (_runtime_filters != nullptr && _runtime_filters->size() > 0) return true;
        return false;
    };
    _has_any_predicate = f();
    return;
}

Status DataSource::parse_runtime_filters(RuntimeState* state) {
    if (_runtime_filters == nullptr || _runtime_filters->size() == 0) return Status::OK();
    for (const auto& item : _runtime_filters->descriptors()) {
        RuntimeFilterProbeDescriptor* probe = item.second;
        DCHECK(runtime_membership_filter_eval_context.driver_sequence != -1);
        const RuntimeFilter* filter = probe->runtime_filter(runtime_membership_filter_eval_context.driver_sequence);
        if (filter == nullptr) continue;
        SlotId slot_id;
        if (!probe->is_probe_slot_ref(&slot_id)) continue;
        LogicalType slot_type = probe->probe_expr_type();
        Expr* min_max_predicate = nullptr;
        RuntimeFilterHelper::create_min_max_value_predicate(state->obj_pool(), slot_id, slot_type, filter,
                                                            &min_max_predicate);
        if (min_max_predicate != nullptr) {
            ExprContext* ctx = state->obj_pool()->add(new ExprContext(min_max_predicate));
            RETURN_IF_ERROR(ctx->prepare(state));
            RETURN_IF_ERROR(ctx->open(state));
            _conjunct_ctxs.insert(_conjunct_ctxs.begin(), ctx);
        }
    }
    return Status::OK();
}

void DataSource::update_profile(const Profile& profile) {
    RuntimeProfile::Counter* mem_alloc_failed_counter = ADD_COUNTER(_runtime_profile, "MemAllocFailed", TUnit::UNIT);
    COUNTER_UPDATE(mem_alloc_failed_counter, profile.mem_alloc_failed_count);
}

Status DataSource::_init_chunk_if_needed(ChunkPtr* chunk, size_t n) {
    ASSIGN_OR_RETURN(*chunk, ChunkHelper::new_chunk_checked(*_tuple_desc, n));
    return Status::OK();
}

} // namespace starrocks::connector
