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

#include "exec/connector_scan_node.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// ==================== ConnectorScanOperatorFactory ====================
int64_t ConnectorScanOperatorMemShareArbitrator::update_chunk_source_mem_bytes(int64_t old_value, int64_t new_value) {
    int64_t diff = new_value - old_value;
    int64_t total = total_chunk_source_mem_bytes.fetch_add(diff) + diff;
    if (new_value == 0) return 0;
    if (total <= 0) return scan_mem_limit;
    return scan_mem_limit * (new_value * 1.0 / std::max(total, new_value));
}

struct ConnectorScanOperatorIOTasksMemLimiter {
    mutable std::mutex lock;

    const int64_t dop = 0;
    const bool shared_scan = false;
    // query scan mem limit means limit for all scan nodes.
    // scan mem limit means limit for this scan node.
    int64_t query_scan_mem_limit = std::numeric_limits<int64_t>::max();
    std::atomic<int64_t> scan_mem_limit = 0;
    std::atomic<int64_t> running_chunk_source_count = 0;
    int64_t data_source_mem_bytes = 0;
    std::atomic<int64_t> chunk_source_mem_bytes = 0;
    int64_t chunk_source_mem_bytes_update_count = 0;
    int64_t last_arb_chunk_source_mem_bytes = 0;
    mutable int64_t debug_output_timestamp = 0;

    ConnectorScanOperatorIOTasksMemLimiter(int64_t dop, bool shared_scan) : dop(dop), shared_scan(shared_scan) {}

    int available_chunk_source_count(int32_t plan_node_id, int driver_sequence) const {
        int64_t scan_mem_limit_value = scan_mem_limit.load(std::memory_order_relaxed);
        int64_t running_count = running_chunk_source_count.load(std::memory_order_relaxed);
        int64_t chunk_source_mem_bytes_value = get_chunk_source_mem_bytes();

        int64_t max_count = std::max(1L, scan_mem_limit_value / chunk_source_mem_bytes_value);
        int64_t avail_count = max_count;
        // int64_t avail_count = std::max(0L, max_count - running_count);
        int64_t per_count = avail_count / dop;

        if (shared_scan) {
            if (driver_sequence < (avail_count - per_count * dop)) {
                per_count += 1;
            }
        } else {
            per_count += 1;
        }

        [[maybe_unused]] auto build_debug_string = [&]() {
            std::stringstream ss;
            ss << "available_chunk_source_count. max_count=" << max_count << "(" << scan_mem_limit_value << "/"
               << chunk_source_mem_bytes_value << "), query_scan_mem_limit = " << query_scan_mem_limit
               << ", running_count = " << running_count << ", dop = " << dop << ", avail_count = " << avail_count
               << ", op_id = " << plan_node_id << "/" << driver_sequence << ", per_count = " << per_count;
            return ss.str();
        };

        if (VLOG_OPERATOR_IS_ON) {
            int64_t now = GetCurrentTimeMicros();
            // output every 1 second.
            if (now - debug_output_timestamp > 1000000) {
                VLOG_OPERATOR << build_debug_string();
                debug_output_timestamp = now;
            }
        }
        return per_count;
    }

    int64_t update_running_chunk_source_count(int delta) {
        return running_chunk_source_count.fetch_add(delta, std::memory_order_seq_cst);
    }

    void update_chunk_source_mem_bytes(int64_t value) {
        if (value == 0) return;
        value = std::min(value, query_scan_mem_limit);

        std::lock_guard<std::mutex> L(lock);
        int64_t total = get_chunk_source_mem_bytes() * chunk_source_mem_bytes_update_count + value;
        chunk_source_mem_bytes_update_count += 1;
        chunk_source_mem_bytes.store(total / chunk_source_mem_bytes_update_count, std::memory_order_relaxed);
    }

    void set_query_scan_mem_limit(int64_t value) { query_scan_mem_limit = value; }
    void update_scan_mem_limit(int64_t value) { scan_mem_limit.store(value, std::memory_order_relaxed); }
    void update_last_arb_chunk_source_mem_bytes(int64_t value) {
        value = std::min(value, query_scan_mem_limit);
        last_arb_chunk_source_mem_bytes = value;
    }

    void set_data_source_mem_bytes(int64_t value) { data_source_mem_bytes = value; }
    int64_t get_data_source_mem_bytes() const { return data_source_mem_bytes; }
    int64_t get_chunk_source_mem_bytes() const { return chunk_source_mem_bytes.load(std::memory_order_relaxed); }
};

ConnectorScanOperatorFactory::ConnectorScanOperatorFactory(int32_t id, ScanNode* scan_node, RuntimeState* state,
                                                           size_t dop, ChunkBufferLimiterPtr buffer_limiter)
        : ScanOperatorFactory(id, scan_node),
          _chunk_buffer(scan_node->is_shared_scan_enabled() ? BalanceStrategy::kRoundRobin : BalanceStrategy::kDirect,
                        dop, std::move(buffer_limiter)) {
    _io_tasks_mem_limiter = state->obj_pool()->add(
            new ConnectorScanOperatorIOTasksMemLimiter(dop, scan_node->is_shared_scan_enabled()));
}

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

void ConnectorScanOperatorFactory::set_chunk_source_mem_bytes(int64_t value) {
    _io_tasks_mem_limiter->update_chunk_source_mem_bytes(value);
    _io_tasks_mem_limiter->update_last_arb_chunk_source_mem_bytes(value);
}

void ConnectorScanOperatorFactory::set_scan_mem_limit(int64_t value) {
    _io_tasks_mem_limiter->update_scan_mem_limit(value);
    _io_tasks_mem_limiter->set_query_scan_mem_limit(value);
}

void ConnectorScanOperatorFactory::set_mem_share_arb(ConnectorScanOperatorMemShareArbitrator* arb) {
    _mem_share_arb = arb;
}

void ConnectorScanOperatorFactory::set_data_source_mem_bytes(int64_t value) {
    _io_tasks_mem_limiter->set_data_source_mem_bytes(value);
}

// ===============================================================
struct ConnectorScanOperatorAdaptiveProcessor {
    // ----------------------
    // op expected io tasks in this cycle.
    int expected_io_tasks = 0;
    // if scan operator is in drive process cycle.
    bool in_driver_process = false;

    // ----------------------
    // when this op starts to run.
    int64_t op_start_time = 0;

    // ----------------------
    // how long when there is no any io task at all.
    int64_t cs_total_halt_time = 0;
    int64_t cs_gen_chunks_time = 0;
    // how many chunks been generated by io tasks.
    std::atomic_int64_t cs_pull_chunks = 0;
    // total io time and running time of io tasks.
    std::atomic_int64_t cs_total_io_time = 0;
    std::atomic_int64_t cs_total_running_time = 0;
    std::atomic_int64_t cs_total_scan_bytes = 0;

    // ----------------------
    std::atomic_int64_t last_chunk_souce_finish_timestamp = 0;
    int64_t check_all_io_tasks_last_timestamp = 0;
    int64_t adjust_io_tasks_last_timestamp = 0;
    int64_t last_driver_output_full_timestamp = 0;

    // ----------------------
    // adjust strategy fields.
    bool try_add_io_tasks = false;
    double expected_speedup_ratio = 0;
    double last_cs_speed = 0;
    int64_t last_cs_pull_chunks = 0;
    int try_add_io_tasks_fail_count = 0;
    int check_slow_io = 0;
    int32_t slow_io_latency_ms = config::connector_io_tasks_adjust_interval_ms;

    // ------------------------
    bool started_running = false;
};

// ==================== ConnectorScanOperator ====================

ConnectorScanOperator::ConnectorScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                                             ScanNode* scan_node)
        : ScanOperator(factory, id, driver_sequence, dop, scan_node) {}

int64_t ConnectorScanOperator::_adjust_scan_mem_limit(int64_t old_value, int64_t new_value) {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    ConnectorScanOperatorIOTasksMemLimiter* L = factory->_io_tasks_mem_limiter;
    ConnectorScanOperatorMemShareArbitrator* arb = factory->_mem_share_arb;
    int64_t new_scan_mem_limit = arb->update_chunk_source_mem_bytes(old_value, new_value);

    L->update_scan_mem_limit(new_scan_mem_limit);
    L->update_last_arb_chunk_source_mem_bytes(new_value);
    ChunkBufferLimiter* limiter = factory->get_chunk_buffer().limiter();
    limiter->update_mem_limit(new_scan_mem_limit * ConnectorScanOperatorMemShareArbitrator::kChunkBufferMemRatio);

    [[maybe_unused]] auto build_debug_string = [&]() {
        std::stringstream ss;
        ss << "adjust_scan_mem_limit. scan node id = " << _plan_node_id
           << ", new mem scan limit = " << new_scan_mem_limit << ", chunk source mem bytes = " << old_value << " -> "
           << new_value;
        return ss.str();
    };
    VLOG_OPERATOR << build_debug_string();

    return new_scan_mem_limit;
}

Status ConnectorScanOperator::do_prepare(RuntimeState* state) {
    const TQueryOptions& options = state->query_options();
    if (options.__isset.enable_connector_adaptive_io_tasks) {
        _enable_adaptive_io_tasks = options.enable_connector_adaptive_io_tasks;
    }

    bool shared_scan = _scan_node->is_shared_scan_enabled();
    _unique_metrics->add_info_string("SharedScan", shared_scan ? "True" : "False");
    _unique_metrics->add_info_string("AdaptiveIOTasks", _enable_adaptive_io_tasks ? "True" : "False");
    _adaptive_processor = state->obj_pool()->add(new ConnectorScanOperatorAdaptiveProcessor());
    _adaptive_processor->op_start_time = GetCurrentTimeMicros();
    _adaptive_processor->started_running = false;
    if (options.__isset.connector_io_tasks_slow_io_latency_ms) {
        _adaptive_processor->slow_io_latency_ms = options.connector_io_tasks_slow_io_latency_ms;
    }
    return Status::OK();
}

void ConnectorScanOperator::do_close(RuntimeState* state) {
    if (_driver_sequence == 0 && _adaptive_processor->started_running) {
        auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
        ConnectorScanOperatorIOTasksMemLimiter* L = factory->_io_tasks_mem_limiter;
        _adjust_scan_mem_limit(L->last_arb_chunk_source_mem_bytes, 0);
    }
}

ChunkSourcePtr ConnectorScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    auto* scan_node = down_cast<ConnectorScanNode*>(_scan_node);
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);

    if (_driver_sequence == 0 && _adaptive_processor->started_running == false) {
        _adaptive_processor->started_running = true;
        ConnectorScanOperatorIOTasksMemLimiter* L = factory->_io_tasks_mem_limiter;
        _adjust_scan_mem_limit(0, L->last_arb_chunk_source_mem_bytes);
    }

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

size_t ConnectorScanOperator::buffer_memory_usage() const {
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    auto& buffer = factory->get_chunk_buffer();
    return buffer.memory_usage();
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
    _adaptive_processor->in_driver_process = true;
    _unpluging = true;

    if (_adaptive_processor->last_driver_output_full_timestamp != 0) {
        int64_t now = GetCurrentTimeMicros();
        int64_t wait = (now - _adaptive_processor->last_driver_output_full_timestamp) * 1000;
        _op_running_time_ns += wait;
        _adaptive_processor->last_driver_output_full_timestamp = 0;
    }
}

void ConnectorScanOperator::end_driver_process(PipelineDriver* driver) {
    _adaptive_processor->check_all_io_tasks_last_timestamp = 0;
    _adaptive_processor->in_driver_process = false;
    _unpluging = false;

    [[maybe_unused]] auto build_debug_string = [&]() {
        std::stringstream ss;
        ss << "end_driver_process. query_id = " << print_id(driver->query_ctx()->query_id())
           << ", op_id = " << _plan_node_id << "/" << _driver_sequence << ", rows = " << _op_pull_rows;
        return ss.str();
    };

    // VLOG_OPERATOR << build_debug_string();

    // we think when scan operator is blocked by output full state
    // it's still running, and it will affect consume chunk speed.
    if (driver->driver_state() == DriverState::OUTPUT_FULL) {
        int64_t now = GetCurrentTimeMicros();
        _adaptive_processor->last_driver_output_full_timestamp = now;
    }
}

bool ConnectorScanOperator::is_running_all_io_tasks() const {
    if (!_enable_adaptive_io_tasks) {
        return _num_running_io_tasks >= _io_tasks_per_scan_operator;
    }

    ConnectorScanOperatorAdaptiveProcessor& P = *_adaptive_processor;
    bool ret = (P.expected_io_tasks != 0) && (_num_running_io_tasks >= P.expected_io_tasks);

    // when in poll state, we don't want this operator sleeps too long time and miss out the chance
    // to adjust io tasks. so we will break it when it runs for too long time
    if (!P.in_driver_process && ret) {
        int64_t now = GetCurrentTimeMicros();
        if (P.check_all_io_tasks_last_timestamp == 0) {
            P.check_all_io_tasks_last_timestamp = now;
        } else if (num_buffered_chunks() > 0) {
            int64_t delta = now - P.check_all_io_tasks_last_timestamp;
            if (delta > config::connector_io_tasks_adjust_interval_ms * 1000) {
                return false;
            }
        }
    }
    return ret;
}
int ConnectorScanOperator::available_pickup_morsel_count() {
    if (!_enable_adaptive_io_tasks) return _io_tasks_per_scan_operator;

    ConnectorScanOperatorAdaptiveProcessor& P = *_adaptive_processor;
    int min_io_tasks = config::connector_io_tasks_min_size;
    auto* factory = down_cast<ConnectorScanOperatorFactory*>(_factory);
    ConnectorScanOperatorIOTasksMemLimiter* L = factory->_io_tasks_mem_limiter;
    int max_io_tasks = L->available_chunk_source_count(_plan_node_id, _driver_sequence);
    max_io_tasks = std::min(max_io_tasks, _io_tasks_per_scan_operator);
    min_io_tasks = std::min(min_io_tasks, max_io_tasks);

    int64_t now = GetCurrentTimeMicros();
    if (_num_running_io_tasks == 0) {
        // if there is no running io tasks, and we record the last chunk source finish time
        // then we can know how long there is no chunk source running at all.
        int64_t halt_time = 0;
        if (P.last_chunk_souce_finish_timestamp != 0) {
            halt_time = now - P.last_chunk_souce_finish_timestamp;
        }
        P.last_chunk_souce_finish_timestamp = now;
        P.cs_total_halt_time += halt_time;
    }

    // to avoid frequent adjustment.
    int& io_tasks = P.expected_io_tasks;
    io_tasks = std::max(min_io_tasks, io_tasks);
    io_tasks = std::min(max_io_tasks, io_tasks);
    if ((now - P.adjust_io_tasks_last_timestamp) <= config::connector_io_tasks_adjust_interval_ms * 1000) {
        return io_tasks;
    }
    P.adjust_io_tasks_last_timestamp = now;

    // scan operator(0) as representative of this scan node,
    // to adjust mem limit via mem share arbitrater.
    if (_driver_sequence == 0) {
        _adjust_scan_mem_limit(L->last_arb_chunk_source_mem_bytes, L->get_chunk_source_mem_bytes());
    }

    // adjust io tasks according information collected
    P.cs_gen_chunks_time = (now - P.op_start_time - P.cs_total_halt_time);
    // chunk sources in each scan operator don't generate chunks in uniform speed
    // but chunks are distributed evenly to scan operator. So we I think we'd better not to
    // use chunks generated by chunks source issues by this scan operator, but to use
    // chunks generated by all scan operators. A approximate value is to use `op_pull_chunks`
    // produced chunks per 10ms, but cs running time is microsecond unit.
    double balanced_cs_speed = _op_pull_chunks * 10000.0 / (P.cs_gen_chunks_time + 1);
    // consumed chunks per 10ms, but op_running time is nanosecond unit.
    double op_speed = _op_pull_chunks * 10000000.0 / (_op_running_time_ns + 1);
    // `cs_speed` is speed in this single scan operator.
    int64_t cs_pull_chunks = P.cs_pull_chunks.load();
    double cs_speed = cs_pull_chunks * 10000.0 / (P.cs_gen_chunks_time + 1);
    // chunk source: total io time and running time.
    // we can see if this is slow device. io_latency in ms unit.
    int64_t cs_total_io_time = P.cs_total_io_time.load();
    int64_t cs_total_scan_bytes = P.cs_total_scan_bytes.load();
    // how many 1MB read.
    double norm_io_count = (cs_total_scan_bytes * 1.0) / (1024 * 1024);
    double io_latency = cs_total_io_time * 0.000001 / norm_io_count;

    // adjust routines.
    auto try_add_io_tasks = [&]() {
        if (!P.try_add_io_tasks) return true;
        if (P.last_cs_pull_chunks == cs_pull_chunks) return true;
        return (cs_speed > (P.last_cs_speed * P.expected_speedup_ratio));
    };
    auto do_add_io_tasks = [&]() {
        P.try_add_io_tasks = true;
        const int smooth = config::connector_io_tasks_adjust_smooth;
        P.expected_speedup_ratio =
                (io_tasks + config::connector_io_tasks_adjust_step + smooth) * 1.0 / (io_tasks + smooth);
        io_tasks += config::connector_io_tasks_adjust_step;
    };
    auto do_sub_io_tasks = [&]() {
        io_tasks -= config::connector_io_tasks_adjust_step;
        P.try_add_io_tasks = false;
        P.try_add_io_tasks_fail_count += 1;
        if (P.try_add_io_tasks_fail_count >= 4) {
            P.try_add_io_tasks_fail_count = 0;
            io_tasks -= config::connector_io_tasks_adjust_step;
        }
    };

    auto check_slow_io = [&]() {
        if (((P.check_slow_io++) % 8) != 0) return;
        if (io_latency >= 2 * P.slow_io_latency_ms) {
            io_tasks = std::max(io_tasks, _io_tasks_per_scan_operator / 2);
        } else if (io_latency >= P.slow_io_latency_ms) {
            io_tasks = std::max(io_tasks, _io_tasks_per_scan_operator / 4);
        } else {
        }
    };

    // adjust io tasks according to feedback.
    auto do_adjustment = [&]() {
        if (balanced_cs_speed > op_speed) {
            do_sub_io_tasks();
            return;
        }

        check_slow_io();
        if (try_add_io_tasks()) {
            // if we don't try add io tasks before,
            // or if we've tried and we get expected speedup ratio.
            do_add_io_tasks();
        } else {
            do_sub_io_tasks();
        }
    };

    do_adjustment();
    io_tasks = std::min(io_tasks, max_io_tasks);
    io_tasks = std::max(io_tasks, min_io_tasks);

    [[maybe_unused]] auto build_debug_string = [&]() {
        auto doround = [](double x) { return round(x * 100.0) / 100.0; };
        std::stringstream ss;
        ss << "available_pickup_morsel_count. id = " << _plan_node_id << ", seq = " << _driver_sequence;
        ss << ", cs = " << doround(cs_speed) << "(" << cs_pull_chunks << "/" << P.cs_gen_chunks_time << ")";
        ss << ", last_cs = " << doround(P.last_cs_speed) << "(" << doround(cs_speed / P.last_cs_speed) << ")";
        ss << ", op = " << doround(op_speed) << "(" << _op_pull_chunks << "/" << (_op_running_time_ns / 1000) << ")";

        ss << ", cs/op = " << doround(balanced_cs_speed) << "/" << doround(op_speed) << "("
           << doround(balanced_cs_speed / op_speed) << ")";

        ss << ", proposal = " << io_tasks << "(" << doround(P.expected_speedup_ratio)
           << "), current = " << _num_running_io_tasks;

        ss << ", iolat = " << cs_total_io_time << "/" << norm_io_count << "(" << doround(io_latency)
           << "), iobytes = " << cs_total_scan_bytes;
        // ss << ", halt_time = " << P.cs_total_halt_time << ", buffer_full = " << is_buffer_full();
        return ss.str();
    };

    // VLOG_OPERATOR << build_debug_string();

    P.last_cs_speed = cs_speed;
    P.last_cs_pull_chunks = cs_pull_chunks;
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

ConnectorScanOperatorIOTasksMemLimiter* ConnectorChunkSource::_get_io_tasks_mem_limiter() const {
    auto* f = down_cast<ConnectorScanOperatorFactory*>(_scan_op->get_factory());
    return f->_io_tasks_mem_limiter;
}

void ConnectorChunkSource::close(RuntimeState* state) {
    if (_closed) return;

    ConnectorScanOperator* scan_op = down_cast<ConnectorScanOperator*>(_scan_op);
    if (scan_op->_enable_adaptive_io_tasks) {
        MemTracker* mem_tracker = state->query_ctx()->connector_scan_mem_tracker();
        mem_tracker->release(_request_mem_tracker_bytes);

        ConnectorScanOperatorIOTasksMemLimiter* limiter = _get_io_tasks_mem_limiter();
        limiter->update_running_chunk_source_count(-1);
        uint64_t chunk_num = std::min<uint64_t>(state->chunk_size(), _chunk_rows_read);

        bool need_update = true;
        int64_t data_source_mem_bytes = _data_source->estimated_mem_usage();
        // 1. if this data source can estimate memory usage but estimated mem usage == 0
        // it means in this chunk source, estimated memory usage is not available, then we don't update
        // 2. but if this data sourcen can not estimate memory usage, we will use default mem usage
        // in following code. We can adjust chunk mem usage.
        if (data_source_mem_bytes == 0 && _data_source->can_estimate_mem_usage()) {
            need_update = false;
        }

        int64_t chunk_source_mem_bytes = 0;
        if (need_update) {
            if (data_source_mem_bytes == 0) {
                data_source_mem_bytes = limiter->get_data_source_mem_bytes();
            }
            chunk_source_mem_bytes = data_source_mem_bytes + avg_row_mem_bytes() * chunk_num;
            limiter->update_chunk_source_mem_bytes(chunk_source_mem_bytes);
        }

        [[maybe_unused]] auto build_debug_string = [&]() {
            std::stringstream ss;
            ss << "try_mem_tracker. query_id = " << print_id(state->query_id())
               << ", op_id = " << _scan_op->get_plan_node_id() << "/" << _scan_op->get_driver_sequence()
               << ", release. this = " << (void*)this << ", request mem bytes = " << _request_mem_tracker_bytes
               << ", chunk source mem bytes = " << chunk_source_mem_bytes
               << ", chunk mem bytes = " << avg_row_mem_bytes() << " * " << chunk_num
               << ", data source mem usage = " << data_source_mem_bytes;
            return ss.str();
        };
        VLOG_OPERATOR << build_debug_string();
    }

    {
        connector::DataSource::Profile profile;
        profile.mem_alloc_failed_count = _mem_alloc_failed_count;
        _data_source->update_profile(profile);
    }
    _closed = true;
    _data_source->close(state);
}

Status ConnectorChunkSource::_open_data_source(RuntimeState* state, bool* mem_alloc_failed) {
    if (_opened) {
        return Status::OK();
    }

    ConnectorScanOperator* scan_op = down_cast<ConnectorScanOperator*>(_scan_op);
    if (scan_op->_enable_adaptive_io_tasks) {
        [[maybe_unused]] auto build_debug_string = [&](const std::string action) {
            std::stringstream ss;
            ss << "try_mem_tracker. query_id = " << print_id(state->query_id())
               << ", op_id = " << _scan_op->get_plan_node_id() << "/" << _scan_op->get_driver_sequence() << ", "
               << action << ". this = " << (void*)this << ", value = " << _request_mem_tracker_bytes;
            return ss.str();
        };

        ConnectorScanOperatorIOTasksMemLimiter* limiter = _get_io_tasks_mem_limiter();
        MemTracker* mem_tracker = state->query_ctx()->connector_scan_mem_tracker();

        int retry = 3;
        while (retry > 0) {
            retry--;
            *mem_alloc_failed = false;
            _request_mem_tracker_bytes = limiter->get_chunk_source_mem_bytes();
            if (mem_tracker->try_consume(_request_mem_tracker_bytes) == nullptr) {
                break;
            }
            *mem_alloc_failed = true;
            sched_yield();
        }

        if (*mem_alloc_failed) {
            // if this is the only running chunk source of this scan node
            // we have to let it run to avoid deadlock of data flow.
            // and in this case, memory is over-committed.
            int64_t running = limiter->update_running_chunk_source_count(1);
            if (running == 0) {
                *mem_alloc_failed = false;
                mem_tracker->consume(_request_mem_tracker_bytes);
            } else {
                limiter->update_running_chunk_source_count(-1);
                // VLOG_OPERATOR << build_debug_string("alloc failed");
                _request_mem_tracker_bytes = 0;
                return Status::OK();
            }
        } else {
            limiter->update_running_chunk_source_count(1);
        }
        VLOG_OPERATOR << build_debug_string("consume");
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
    ConnectorScanOperator* op = down_cast<ConnectorScanOperator*>(_scan_op);
    ConnectorScanOperatorAdaptiveProcessor& P = *(op->_adaptive_processor);

    DeferOp defer_op([&]() { P.last_chunk_souce_finish_timestamp = GetCurrentTimeMicros(); });

    int64_t total_time_ns = 0;
    int64_t delta_io_time_ns = 0;
    int64_t delta_scan_bytes = 0;
    {
        SCOPED_RAW_TIMER(&total_time_ns);
        int64_t prev_io_time_ns = get_io_time_spent();
        int64_t prev_scan_bytes = get_scan_bytes();

        bool mem_alloc_failed = false;
        RETURN_IF_ERROR(_open_data_source(state, &mem_alloc_failed));
        if (mem_alloc_failed) {
            _mem_alloc_failed_count += 1;
            return Status::TimedOut("");
        }
        if (state->is_cancelled()) {
            return Status::Cancelled("canceled state");
        }

        // Improve for select * from table limit x, x is small
        if (_reach_eof()) {
            _reach_limit.store(true);
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
        _io_time_spent_ns = _data_source->io_time_spent();
        delta_io_time_ns = _io_time_spent_ns - prev_io_time_ns;
        delta_scan_bytes = _scan_bytes - prev_scan_bytes;
    }

    if (_ck_acc.has_output()) {
        *chunk = std::move(_ck_acc.pull());
        P.cs_pull_chunks += 1;
        P.cs_total_running_time += total_time_ns;
        P.cs_total_io_time += delta_io_time_ns;
        P.cs_total_scan_bytes += delta_scan_bytes;
        _chunk_rows_read += (*chunk)->num_rows();
        _chunk_mem_bytes += (*chunk)->memory_usage();
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

uint64_t ConnectorChunkSource::avg_row_mem_bytes() const {
    if (_chunk_rows_read == 0) return 0;
    return _chunk_mem_bytes / _chunk_rows_read;
}

} // namespace starrocks::pipeline
