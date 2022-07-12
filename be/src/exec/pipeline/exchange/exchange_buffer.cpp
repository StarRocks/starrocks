// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/exchange/exchange_buffer.h"

#include "brpc/periodic_task.h"
#include "common/config.h"
#include "common/logging.h"
#include "util/defer_op.h"
#include "util/disposable_closure.h"
#include "util/priority_thread_pool.hpp"
#include "util/uid_util.h"

namespace starrocks::pipeline {

class ScheduleTask : public brpc::PeriodicTask {
public:
    ScheduleTask(std::shared_ptr<ExchangeBuffer> buffer) : _buffer(buffer) {}

    ~ScheduleTask() override {}

    bool OnTriggeringTask(timespec* next_abstime) override {
        _buffer->_schedule_count++;
        SCOPED_RAW_TIMER(&(_buffer->_schedule_time));
        while (true) {
            bool has_new_response = _buffer->process_rpc_result();
            bool has_sent_new_request = _buffer->try_to_send_rpc();
            if (_buffer->is_finished()) {
                return false;
            }
            if (!has_new_response && !has_sent_new_request) {
                *next_abstime = butil::milliseconds_from_now(1);
                _buffer->_rewardless_schedule_count++;
                return true;
            }
        }

        *next_abstime = butil::milliseconds_from_now(1);
        return true;
    }

    void OnDestroyingTask() override {
        _buffer.reset();
        delete this;
    }

private:
    std::shared_ptr<ExchangeBuffer> _buffer;
};

ExchangeBuffer::ExchangeBuffer(MultiExchangeBuffer* parent, TUniqueId instance_id, int32_t num_writers)
        : _parent(parent),
          _instance_id(instance_id),
          // num_writers * parent->_exchange_sink_dop is the max number of requests added at the same time during ExchangeSinkOperator::set_finishing phase
          // we should reserve enough space
          _buffer_capacity(num_writers * parent->_exchange_sink_dop + kBufferSize),
          _buffer(_buffer_capacity),
          _available_flags(_buffer_capacity),
          _results(_buffer_capacity),
          _finish_flags(_buffer_capacity),
          _num_writers(num_writers),
          _num_remaining_eos(num_writers),
          _num_uncancelled_sinkers(num_writers) {
    _finst_id.set_hi(instance_id.hi);
    _finst_id.set_lo(instance_id.lo);

    for (size_t i = 0; i < _buffer_capacity; i++) {
        _finish_flags[i].store(false);
        _available_flags[i].store(false);
    }
}

void ExchangeBuffer::add_request(TransmitChunkInfo& request) {
    DCHECK(_num_remaining_eos > 0);
    if (_is_finishing) {
        return;
    }
    if (request.params->eos()) {
        // if have chunk, add into buffer, else return
        bool should_reserve = false;
        if (request.params->chunks_size() > 0) {
            request.params->set_eos(false);
            should_reserve = true;
        }
        if (--_num_remaining_eos > 0) {
            // only reserve the last eos
            if (!should_reserve) {
                return;
            }
        } else {
            request.params->set_eos(true);
        }
    }
    if (!request.attachment.empty()) {
        _bytes_enqueued += request.attachment.size();
        _requests_enqueued++;
    }
    int64_t current_seqs = ++_last_arrived_seqs;
    DCHECK_LT(current_seqs - _last_acked_seqs, _buffer_capacity);
    int index = current_seqs % _buffer_capacity;
    // seqs allocation and buffer assignment are not atomic, use _available_flags to solve it
    _buffer[index] = request;
    DCHECK_EQ(_available_flags[index], false);
    _available_flags[index].store(true);
}

bool ExchangeBuffer::is_full() {
    int32_t size = _last_arrived_seqs - _last_in_flight_seqs;
    // before set_finishing phase, there are up to num_writers thread may add request at the same time,
    // we should reserve enough space
    return size + _num_writers > kBufferSize;
}

bool ExchangeBuffer::is_finished() {
    if (!_is_finishing) {
        return false;
    }
    return _num_in_flight_rpcs == 0;
}

void ExchangeBuffer::cancel_one_sinker() {
    if (--_num_uncancelled_sinkers == 0) {
        _is_finishing = true;
    }
}

bool ExchangeBuffer::is_concurreny_exceed_limit() {
    return _num_in_flight_rpcs >= config::pipeline_sink_brpc_dop;
}

bool ExchangeBuffer::try_to_send_rpc() {
    // try to send request in (_last_in_flight_seqs, _last_arrived_seqs]
    int64_t last_in_flight_seqs = _last_in_flight_seqs;
    int64_t last_arrived_seqs = _last_arrived_seqs;

    int64_t target_seqs = std::min(last_arrived_seqs, _last_acked_seqs + _buffer_capacity);
    bool has_sent_new_request = false;
    for (int64_t seq = last_in_flight_seqs + 1; seq <= target_seqs; seq++) {
        if (_is_finishing) {
            return has_sent_new_request;
        }
        if (is_concurreny_exceed_limit()) {
            return has_sent_new_request;
        }
        int index = seq % _buffer_capacity;
        while (!_available_flags[index]) {
            _mm_pause();
        }
        auto request = _buffer[index];
        // The order of data transmiting in IO level may not be strictly the same as
        // the order of submitting data packets
        // But we must guarantee that first packet must be received first
        if (_last_acked_seqs == -1 && _num_in_flight_rpcs > 0) {
            return false;
        }
        if (request.params->eos()) {
            if (_num_in_flight_rpcs > 0) {
                // The order of data transmiting in IO level may not be strictly the same as
                // the order of submitting data packets
                // But we must guarantee that eos packent must be the last packet
                return has_sent_new_request;
            }
        }

        *request.params->mutable_finst_id() = _finst_id;
        request.params->set_sequence(seq);
        if (!request.attachment.empty()) {
            _bytes_sent += request.attachment.size();
            _requests_sent++;
        }

        auto* closure = new DisposableClosure<PTransmitChunkResult, ExchangeBufferClosureContext>(
                {request.params->sequence(), GetCurrentTimeNanos(), MonotonicMicros(), request.params->eos()});

        closure->addFailedHandler([this](const ExchangeBufferClosureContext& ctx) noexcept {
            --_num_in_flight_rpcs;
            _is_finishing = true;
            std::string err_msg = fmt::format("transmit chunk rpc failed: {}", print_id(_instance_id));
            _parent->_fragment_ctx->cancel(Status::InternalError(err_msg));
            LOG(WARNING) << err_msg;
        });
        closure->addSuccessHandler(
                [this](const ExchangeBufferClosureContext& ctx, const PTransmitChunkResult& result) noexcept {
                    --_num_in_flight_rpcs;
                    Status status(result.status());
                    if (!status.ok()) {
                        _is_finishing = true;
                        _parent->_fragment_ctx->cancel(result.status());
                        LOG(WARNING) << fmt::format("transmit chunk rpc failed:{}, msg:{}", print_id(_instance_id),
                                                    status.message());
                    } else {
                        int64_t seq = ctx.sequence;
                        int index = seq % _buffer_capacity;
                        _results[index].send_timestamp = ctx.send_timestamp;
                        _results[index].received_timestamp = result.receive_timestamp();
                        _results[index].start_timestamp = ctx.start_timestamp;
                        _results[index].finish_timestamp = MonotonicMicros();
                        _finish_flags[index].store(true);
                        if (ctx.is_eos) {
                            _is_finishing = true;
                        }
                    }
                });

        ++_num_in_flight_rpcs;
        closure->cntl.Reset();
        closure->cntl.set_timeout_ms(_parent->_brpc_timeout_ms);
        closure->cntl.request_attachment().append(request.attachment);
        request.brpc_stub->transmit_chunk(&closure->cntl, request.params.get(), &closure->result, closure);
        _available_flags[index].store(false);
        // after rpc is sent, the attachment in buffer is no longer meanningful
        _buffer[index].attachment.clear();
        _last_in_flight_seqs++;
        has_sent_new_request = true;
    }
    return has_sent_new_request;
}

bool ExchangeBuffer::process_rpc_result() {
    // iterate result in (_last_acked_seqs, _last_in_flight_seqs]
    // only one thread execute it, no other threads will update
    int64_t last_acked_seqs = _last_acked_seqs;
    int64_t last_in_flight_seqs = _last_in_flight_seqs;
    for (int64_t seq = last_acked_seqs + 1; seq <= last_in_flight_seqs; seq++) {
        int index = seq % _buffer_capacity;
        if (_finish_flags[index].load()) {
            _last_acked_seqs++;
            auto& result = _results[index];
            update_network_time(result.send_timestamp, result.received_timestamp);
            _finish_flags[index].store(false);
        } else {
            break;
        }
    }
    return _last_acked_seqs > last_acked_seqs;
}

void ExchangeBuffer::update_network_time(const int64_t send_timestamp, const int64_t receive_timestamp) {
    int32_t concurrency = _num_in_flight_rpcs;
    _network_time.update(receive_timestamp - send_timestamp, concurrency);
}

MultiExchangeBuffer::MultiExchangeBuffer(FragmentContext* fragment_ctx,
                                         const std::vector<TPlanFragmentDestination>& destinations, bool is_dest_merge,
                                         size_t num_sinkers)
        : _fragment_ctx(fragment_ctx),
          _mem_tracker(fragment_ctx->runtime_state()->instance_mem_tracker()),
          _exchange_sink_dop(num_sinkers),
          _is_dest_merge(is_dest_merge),
          _brpc_timeout_ms(std::min(3600, fragment_ctx->runtime_state()->query_options().query_timeout) * 1000) {
    // calculate _num_writers, this must be before create ExchangeBuffer
    phmap::flat_hash_map<int64_t, int32_t> num_writers;
    for (const auto& dest : destinations) {
        const auto& instance_id = dest.fragment_instance_id;
        if (instance_id.lo == -1) {
            continue;
        }
        auto it = num_writers.find(instance_id.lo);
        if (it != num_writers.end()) {
            it->second += num_sinkers;
        } else {
            num_writers[instance_id.lo] = num_sinkers;
        }
    }
    for (const auto& dest : destinations) {
        const auto& instance_id = dest.fragment_instance_id;
        // instance_id.lo == -1 indicates that the destination is pseudo for bucket shuffle join.
        if (instance_id.lo == -1) {
            continue;
        }
        if (_buffers.find(instance_id.lo) == _buffers.end()) {
            auto exchange_buffer = std::make_shared<ExchangeBuffer>(this, instance_id, num_writers.at(instance_id.lo));
            _buffers.insert({instance_id.lo, exchange_buffer});
        }
    }
    // submit schedule task
    for (auto& [_, buffer] : _buffers) {
        brpc::PeriodicTaskManager::StartTaskAt(new ScheduleTask(buffer), butil::microseconds_from_now(1));
    }
}

MultiExchangeBuffer::~MultiExchangeBuffer() {}

Status MultiExchangeBuffer::prepare() {
    return Status::OK();
}

void MultiExchangeBuffer::add_request(TransmitChunkInfo& request) {
    auto& buffer = _buffers.at(request.fragment_instance_id.lo);
    buffer->add_request(request);
}

bool MultiExchangeBuffer::is_full() {
    bool is_full = false;
    for (auto& [_, buffer] : _buffers) {
        if (buffer->is_full()) {
            is_full = true;
            break;
        }
    }

    int64_t last_full_timestamp = _last_full_timestamp;
    int64_t full_time = _full_time;

    if (is_full && last_full_timestamp == -1) {
        _last_full_timestamp.compare_exchange_weak(last_full_timestamp, MonotonicNanos());
    }
    if (!is_full && last_full_timestamp != -1) {
        // The following two update operations cannot guarantee atomicity as a whole without lock
        // But we can accept bias in estimatation
        _full_time.compare_exchange_weak(full_time, full_time + (MonotonicNanos() - last_full_timestamp));
        _last_full_timestamp.compare_exchange_weak(last_full_timestamp, -1);
    }

    return is_full;
}

void MultiExchangeBuffer::set_finishing() {
    int64_t expected = -1;
    if (!_pending_timestamp.compare_exchange_strong(expected, MonotonicNanos())) {
        return;
    }
}

bool MultiExchangeBuffer::is_finished() {
    for (auto& [_, buffer] : _buffers) {
        if (!buffer->is_finished()) {
            return false;
        }
    }
    return true;
}

void MultiExchangeBuffer::cancel_one_sinker() {
    for (auto& [_, buffer] : _buffers) {
        buffer->cancel_one_sinker();
    }
}

void MultiExchangeBuffer::update_profile(RuntimeProfile* profile) {
    bool flag = false;
    if (!_is_profile_updated.compare_exchange_strong(flag, true)) {
        return;
    }
    // collect profile from each buffer
    auto* network_timer = ADD_TIMER(profile, "NetworkTime");
    COUNTER_SET(network_timer, network_time());
    auto* full_timer = ADD_TIMER(profile, "FullTime");
    COUNTER_SET(full_timer, _full_time);

    auto* pending_finish_timer = ADD_TIMER(profile, "PendingFinishTime");
    COUNTER_SET(pending_finish_timer, MonotonicNanos() - _pending_timestamp);

    // schedule related
    auto* schedule_counter = ADD_COUNTER(profile, "BufferScheduleTaskRunCount", TUnit::UNIT);
    auto* rewardless_schedule_counter = ADD_COUNTER(profile, "BufferScheduleTaskRewardlessRunCount", TUnit::UNIT);
    auto* schedule_timer = ADD_TIMER(profile, "BufferScheduleTaskRunTime");
    int64_t total_schedule_count = 0;
    int64_t total_rewardless_schedule_count = 0;
    int64_t total_schedule_time = 0;
    for (auto& [_, buffer] : _buffers) {
        total_schedule_count += buffer->_schedule_count;
        total_schedule_time += buffer->_schedule_time;
        total_rewardless_schedule_count += buffer->_rewardless_schedule_count;
    }

    COUNTER_SET(schedule_counter, total_schedule_count);
    COUNTER_SET(schedule_timer, total_schedule_time);
    COUNTER_SET(rewardless_schedule_counter, total_rewardless_schedule_count);

    auto* bytes_sent_counter = ADD_COUNTER(profile, "BytesSent", TUnit::BYTES);
    auto* request_sent_counter = ADD_COUNTER(profile, "RequestSent", TUnit::UNIT);
    int64_t total_bytes_enqueued = 0;
    int64_t total_requests_enqueued = 0;
    for (auto& [_, buffer] : _buffers) {
        COUNTER_UPDATE(bytes_sent_counter, buffer->_bytes_sent);
        COUNTER_UPDATE(request_sent_counter, buffer->_requests_sent);
        total_bytes_enqueued += buffer->_bytes_enqueued;
        total_requests_enqueued += buffer->_requests_enqueued;
    }

    if (total_bytes_enqueued > bytes_sent_counter->value()) {
        auto* bytes_unsent_counter = ADD_COUNTER(profile, "BytesUnsent", TUnit::BYTES);
        auto* request_unsent_counter = ADD_COUNTER(profile, "RequestUnsent", TUnit::UNIT);
        COUNTER_SET(bytes_unsent_counter, total_bytes_enqueued - bytes_sent_counter->value());
        COUNTER_SET(request_unsent_counter, total_requests_enqueued - request_sent_counter->value());
    }

    profile->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            [bytes_sent_counter, network_timer] {
                return RuntimeProfile::units_per_second(bytes_sent_counter, network_timer);
            },
            "");
}

int64_t MultiExchangeBuffer::network_time() {
    int64_t max = 0;
    for (auto& [_, buffer] : _buffers) {
        double avg_concurrency = static_cast<double>(buffer->_network_time.accumulated_concurrency /
                                                     std::max(1, buffer->_network_time.times));
        int64_t avg_accumulated_time =
                static_cast<int64_t>(buffer->_network_time.accumulated_time / std::max(1.0, avg_concurrency));
        if (avg_accumulated_time > max) {
            max = avg_accumulated_time;
        }
    }
    return max;
}

} // namespace starrocks::pipeline