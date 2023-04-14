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

#include "exec/pipeline/scan/connector_scan_operator.h"

#include "column/chunk.h"
#include "exec/connector_scan_node.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// ==================== ConnectorScanOperatorFactory ====================

ConnectorScanOperatorFactory::ConnectorScanOperatorFactory(int32_t id, ScanNode* scan_node, size_t dop,
                                                           ChunkBufferLimiterPtr buffer_limiter)
        : ScanOperatorFactory(id, scan_node),
          _chunk_buffer(scan_node->is_shared_scan_enabled() ? BalanceStrategy::kRoundRobin : BalanceStrategy::kDirect,
                        dop, std::move(buffer_limiter)) {}

Status ConnectorScanOperatorFactory::do_prepare(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    RETURN_IF_ERROR(Expr::prepare(conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(conjunct_ctxs, state));

    return Status::OK();
}

void ConnectorScanOperatorFactory::do_close(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    Expr::close(conjunct_ctxs, state);
}

OperatorPtr ConnectorScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<ConnectorScanOperator>(this, _id, driver_sequence, dop, _scan_node);
}

const std::vector<ExprContext*>& ConnectorScanOperatorFactory::partition_exprs() const {
    auto* connector_scan_node = down_cast<ConnectorScanNode*>(_scan_node);
    auto* provider = connector_scan_node->data_source_provider();
    return provider->partition_exprs();
}

// ==================== ConnectorScanOperator ====================

ConnectorScanOperator::ConnectorScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                                             ScanNode* scan_node)
        : ScanOperator(factory, id, driver_sequence, dop, scan_node) {}

Status ConnectorScanOperator::do_prepare(RuntimeState* state) {
    const TQueryOptions& options = state->query_options();
    if (options.__isset.enable_connector_adaptive_io_tasks) {
        _enable_adaptive_io_tasks = options.enable_connector_adaptive_io_tasks;
    }

    bool shared_scan = _scan_node->is_shared_scan_enabled();
    _unique_metrics->add_info_string("SharedScan", shared_scan ? "True" : "False");
    _unique_metrics->add_info_string("AdaptiveIOTasks", _enable_adaptive_io_tasks ? "True" : "False");
    _start_running_time = GetCurrentTimeMicros();
    return Status::OK();
}

void ConnectorScanOperator::do_close(RuntimeState* state) {}

ChunkSourcePtr ConnectorScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    auto* scan_node = down_cast<ConnectorScanNode*>(_scan_node);
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    return std::make_shared<ConnectorChunkSource>(this, _chunk_source_profiles[chunk_source_index].get(),
                                                  std::move(morsel), scan_node, factory->get_chunk_buffer());
}

void ConnectorScanOperator::attach_chunk_source(int32_t source_index) {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& active_inputs = factory->get_active_inputs();
    auto key = std::make_pair(_driver_sequence, source_index);
    active_inputs.emplace(key);
}

void ConnectorScanOperator::detach_chunk_source(int32_t source_index) {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& active_inputs = factory->get_active_inputs();
    auto key = std::make_pair(_driver_sequence, source_index);
    active_inputs.erase(key);
}

bool ConnectorScanOperator::has_shared_chunk_source() const {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& active_inputs = factory->get_active_inputs();
    return !active_inputs.empty();
}

size_t ConnectorScanOperator::num_buffered_chunks() const {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& buffer = factory->get_chunk_buffer();
    return buffer.size(_driver_sequence);
}

ChunkPtr ConnectorScanOperator::get_chunk_from_buffer() {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& buffer = factory->get_chunk_buffer();
    ChunkPtr chunk = nullptr;
    if (buffer.try_get(_driver_sequence, &chunk)) {
        return chunk;
    }
    return nullptr;
}

size_t ConnectorScanOperator::buffer_size() const {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& buffer = factory->get_chunk_buffer();
    return buffer.limiter()->size();
}

size_t ConnectorScanOperator::buffer_capacity() const {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& buffer = factory->get_chunk_buffer();
    return buffer.limiter()->capacity();
}

size_t ConnectorScanOperator::default_buffer_capacity() const {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& buffer = factory->get_chunk_buffer();
    return buffer.limiter()->default_capacity();
}

ChunkBufferTokenPtr ConnectorScanOperator::pin_chunk(int num_chunks) {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& buffer = factory->get_chunk_buffer();
    return buffer.limiter()->pin(num_chunks);
}

bool ConnectorScanOperator::is_buffer_full() const {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& buffer = factory->get_chunk_buffer();
    return buffer.limiter()->is_full();
}

void ConnectorScanOperator::set_buffer_finished() {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& buffer = factory->get_chunk_buffer();
    buffer.set_finished(_driver_sequence);
}

connector::ConnectorType ConnectorScanOperator::connector_type() {
    auto* scan_node = down_cast<ConnectorScanNode*>(_scan_node);
    return scan_node->connector_type();
}

void ConnectorScanOperator::begin_driver_process() {
    last_check_time = 0;
    early_cut_all_io_tasks = 0;
    expected_io_tasks = current_io_tasks;
    // _unpluging = true;
}

void ConnectorScanOperator::begin_pull_chunk(ChunkPtr res) {
    _op_pull_rows = _op_pull_rows + 1;
}

void ConnectorScanOperator::after_pull_chunk(int64_t time) {
    _op_running_time += time;
}

void ConnectorScanOperator::after_driver_process() {
    last_check_time = 0;
    early_cut_all_io_tasks = 0;
    expected_io_tasks = current_io_tasks;
}

bool ConnectorScanOperator::is_running_all_io_tasks() const {
    if (!_enable_adaptive_io_tasks) {
        return _num_running_io_tasks >= _io_tasks_per_scan_operator;
    }

    if (expected_io_tasks != _io_tasks_per_scan_operator) {
        int64_t now = GetCurrentTimeMicros();
        if (early_cut_all_io_tasks == 0) {
            early_cut_all_io_tasks = now;
        } else if ((now - early_cut_all_io_tasks) > config::connector_io_tasks_check_interval) {
            expected_io_tasks = _io_tasks_per_scan_operator;
        }
    }

    bool ret = (expected_io_tasks != 0) && (_num_running_io_tasks >= expected_io_tasks);
    return ret;
}

int ConnectorScanOperator::available_pickup_morsel_count() {
    // if (!_enable_adaptive_io_tasks) return _io_tasks_per_scan_operator;

    const int MIN_IO_TASKS = config::connector_io_tasks_min_size;
    int64_t now = GetCurrentTimeMicros();

    int& io_tasks = current_io_tasks;
    io_tasks = std::max(MIN_IO_TASKS, io_tasks);

    if (!_enable_adaptive_io_tasks) {
        io_tasks = _io_tasks_per_scan_operator;
    }

    if ((now - last_check_time) <= config::connector_io_tasks_check_interval * 1000) {
        return io_tasks;
    }
    last_check_time = now;

    int64_t cs_running_time = (now - _start_running_time) * 1000;

    constexpr double SCALE = 100000000; //100ms

    // called every time before `pull_chunk` when there is chunk.
    // accumulate chunk source stats.
    double cs_speed = _cs_pull_rows * SCALE / (cs_running_time + 1);

    // accumulate scan operator stats.
    double op_speed = _op_pull_rows * SCALE / (_op_running_time + 1);

    // for slow query, at startup, cs/op could be both 0.
    // for this case, w'd better to ramup but to wait.
    std::string flag = "";
    if (cs_speed < (op_speed * 1.2)) {
        if (!try_add_before) {
            expect_ratio = (io_tasks + config::connector_io_tasks_inc) * 1.0 / io_tasks;
            io_tasks += config::connector_io_tasks_inc;
            try_add_before = true;
            flag += "[TRY]";
            last_cs_speed = cs_speed;
        } else {
            if (cs_speed < (last_cs_speed * expect_ratio)) {
                try_add_before = false;
                io_tasks -= config::connector_io_tasks_dec;
                flag += "[DEC]";
            } else {
                io_tasks += config::connector_io_tasks_dec;
                flag += "[INC]";
            }
        }
    } else {
        try_add_before = false;
        io_tasks -= config::connector_io_tasks_dec;
    }
    io_tasks = std::min(io_tasks, _io_tasks_per_scan_operator);
    io_tasks = std::max(io_tasks, MIN_IO_TASKS);

    if (!_enable_adaptive_io_tasks) {
        io_tasks = _io_tasks_per_scan_operator;
    }

    // VLOG_FILE << "[XXX] pick mosrsel. id = " << _driver_sequence << ", cs = " << cs_speed << "( " << _cs_pull_rows
    //           << "/" << cs_running_time << "), old = " << last_cs_speed << "(" << cs_speed / last_cs_speed
    //           << "), op = " << op_speed << "(" << _op_pull_rows << "/" << _op_running_time << "), value = " << io_tasks
    //           << ", current = " << _num_running_io_tasks << ", flag = " << flag << ", unplug = " << _unpluging  << ", chunk/thres = "
    //           << num_buffered_chunks() << "/" << _buffer_unplug_threshold();

    VLOG_FILE << "[XXX] pick mosrsel. id = " << _driver_sequence << ", cs = " << cs_speed << ", old = " << last_cs_speed
              << ", op = " << op_speed << ", proposal = " << io_tasks << ", current = " << _num_running_io_tasks
              << ", flag = " << flag << ", unplug = " << _unpluging << ", chunk/thres = " << num_buffered_chunks()
              << "/" << _buffer_unplug_threshold();

    last_cs_speed = cs_speed;
    return io_tasks;
}

// ==================== ConnectorChunkSource ====================
ConnectorChunkSource::ConnectorChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                                           ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer)
        : ChunkSource(op, runtime_profile, std::move(morsel), chunk_buffer),
          _scan_node(scan_node),
          _limit(scan_node->limit()),
          _runtime_in_filters(op->runtime_in_filters()),
          _runtime_bloom_filters(op->runtime_bloom_filters()) {
    _conjunct_ctxs = scan_node->conjunct_ctxs();
    _conjunct_ctxs.insert(_conjunct_ctxs.end(), _runtime_in_filters.begin(), _runtime_in_filters.end());
    auto* scan_morsel = (ScanMorsel*)_morsel.get();
    TScanRange* scan_range = scan_morsel->get_scan_range();

    if (scan_range->__isset.broker_scan_range) {
        scan_range->broker_scan_range.params.__set_non_blocking_read(true);
    }
    _data_source = scan_node->data_source_provider()->create_data_source(*scan_range);
    _data_source->set_predicates(_conjunct_ctxs);
    _data_source->set_runtime_filters(_runtime_bloom_filters);
    _data_source->set_read_limit(_limit);
    _data_source->set_runtime_profile(runtime_profile);
    _data_source->update_has_any_predicate();
    _op = down_cast<ConnectorScanOperator*>(op);
}

ConnectorChunkSource::~ConnectorChunkSource() {
    if (_runtime_state != nullptr) {
        close(_runtime_state);
    }
}

Status ConnectorChunkSource::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ChunkSource::prepare(state));
    _runtime_state = state;
    _data_source->parse_runtime_filters(state);
    return Status::OK();
}

const std::string ConnectorChunkSource::get_custom_coredump_msg() const {
    return _data_source->get_custom_coredump_msg();
}

void ConnectorChunkSource::close(RuntimeState* state) {
    if (_closed) return;
    _closed = true;
    _data_source->close(state);
}

Status ConnectorChunkSource::_open_data_source(RuntimeState* state) {
    if (_opened) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_data_source->open(state));
    if (!_data_source->has_any_predicate() && _limit != -1 && _limit < state->chunk_size()) {
        _ck_acc.set_max_size(_limit);
    } else {
        _ck_acc.set_max_size(state->chunk_size());
    }
    _opened = true;

    return Status::OK();
}

Status ConnectorChunkSource::_read_chunk(RuntimeState* state, ChunkPtr* chunk) {
    RETURN_IF_ERROR(_open_data_source(state));

    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }

    // Improve for select * from table limit x, x is small
    if (_reach_eof()) {
        return Status::EndOfFile("limit reach");
    }

    while (_status.ok()) {
        ChunkPtr tmp;
        _status = _data_source->get_next(state, &tmp);
        if (_status.ok()) {
            if (tmp->num_rows() == 0) continue;
            _ck_acc.push(tmp);
            if (_ck_acc.has_output()) break;
        } else if (!_status.is_end_of_file()) {
            if (_status.is_time_out()) {
                Status t = _status;
                _status = Status::OK();
                return t;
            } else {
                return _status;
            }
        } else {
            _ck_acc.finalize();
            DCHECK(_status.is_end_of_file());
        }
    }

    DCHECK(_status.ok() || _status.is_end_of_file());
    _scan_rows_num = _data_source->raw_rows_read();
    _scan_bytes = _data_source->num_bytes_read();
    _cpu_time_spent_ns = _data_source->cpu_time_spent();
    if (_ck_acc.has_output()) {
        *chunk = std::move(_ck_acc.pull());
        _op->_cs_pull_rows = _op->_cs_pull_rows + 1;
        _rows_read += (*chunk)->num_rows();
        _chunk_buffer.update_limiter(chunk->get());
        return Status::OK();
    }
    _ck_acc.reset();
    return Status::EndOfFile("");
}

const workgroup::WorkGroupScanSchedEntity* ConnectorChunkSource::_scan_sched_entity(
        const workgroup::WorkGroup* wg) const {
    DCHECK(wg != nullptr);
    return wg->connector_scan_sched_entity();
}

} // namespace starrocks::pipeline
