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

#include "exec/pipeline/exchange/sink_buffer.h"

#include <bthread/bthread.h>

#include <chrono>

#include "fmt/core.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace starrocks::pipeline {

SinkBuffer::SinkBuffer(FragmentContext* fragment_ctx, const std::vector<TPlanFragmentDestination>& destinations,
                       bool is_dest_merge)
        : _fragment_ctx(fragment_ctx),
          _mem_tracker(fragment_ctx->runtime_state()->instance_mem_tracker()),
          _brpc_timeout_ms(fragment_ctx->runtime_state()->query_options().query_timeout * 1000),
          _is_dest_merge(is_dest_merge),
          _rpc_http_min_size(fragment_ctx->runtime_state()->get_rpc_http_min_size()) {
    for (const auto& dest : destinations) {
        const auto& instance_id = dest.fragment_instance_id;
        // instance_id.lo == -1 indicates that the destination is pseudo for bucket shuffle join.
        if (instance_id.lo == -1) {
            continue;
        }

        auto it = _num_sinkers.find(instance_id.lo);
        if (it == _num_sinkers.end()) {
            _num_sinkers[instance_id.lo] = 0;
            _request_seqs[instance_id.lo] = -1;
            _max_continuous_acked_seqs[instance_id.lo] = -1;
            _discontinuous_acked_seqs[instance_id.lo] = std::unordered_set<int64_t>();
            _buffers[instance_id.lo] = std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>();
            _num_finished_rpcs[instance_id.lo] = 0;
            _num_in_flight_rpcs[instance_id.lo] = 0;
            _network_times[instance_id.lo] = TimeTrace{};
            _eos_query_stats[instance_id.lo] = std::make_shared<QueryStatistics>();
            _mutexes[instance_id.lo] = std::make_unique<Mutex>();
            _dest_addrs[instance_id.lo] = dest.brpc_server;

            PUniqueId finst_id;
            finst_id.set_hi(instance_id.hi);
            finst_id.set_lo(instance_id.lo);
            _instance_id2finst_id[instance_id.lo] = std::move(finst_id);
        }
    }
}

SinkBuffer::~SinkBuffer() {
    // In some extreme cases, the pipeline driver has not been created yet, and the query is over
    // At this time, sink_buffer also needs to be able to be destructed correctly
    _is_finishing = true;

    DCHECK(is_finished());

    {
        // Since the deallocation of _buffers may happen inside bthread, make sure all the allocations and
        // deallocations are executed using the process level MemTracker
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
        _buffers.clear();
    }
}

void SinkBuffer::incr_sinker(RuntimeState* state) {
    _num_uncancelled_sinkers++;
    for (auto& [_, num_sinkers_per_instance] : _num_sinkers) {
        num_sinkers_per_instance++;
    }
    _num_remaining_eos += _num_sinkers.size();
}

Status SinkBuffer::add_request(TransmitChunkInfo& request) {
    DCHECK(_num_remaining_eos > 0);
    if (_is_finishing) {
        return Status::OK();
    }
    if (!request.attachment.empty()) {
        _bytes_enqueued += request.attachment.size();
        _request_enqueued++;
    }
    {
        auto& instance_id = request.fragment_instance_id;
        RETURN_IF_ERROR(_try_to_send_rpc(instance_id, [&]() {
            // Since the deallocation of _buffers may happen inside bthread, make sure all the allocations and
            // deallocations are executed using the process level MemTracker
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
            _buffers[instance_id.lo].push(request);
        }));
    }

    return Status::OK();
}

bool SinkBuffer::is_full() const {
    // std::queue' read is concurrent safe without mutex
    // Judgement may not that accurate because we do not known in advance which
    // instance the data to be sent corresponds to
    size_t max_buffer_size = config::pipeline_sink_buffer_size * _buffers.size();
    size_t buffer_size = 0;
    for (auto& [_, buffer] : _buffers) {
        buffer_size += buffer.size();
    }
    const bool is_full = buffer_size > max_buffer_size;

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

void SinkBuffer::set_finishing() {
    _pending_timestamp = MonotonicNanos();
}

bool SinkBuffer::is_finished() const {
    if (!_is_finishing) {
        return false;
    }

    return _num_sending_rpc == 0 && _total_in_flight_rpc == 0;
}

void SinkBuffer::update_profile(RuntimeProfile* profile) {
    auto* rpc_count = ADD_COUNTER(profile, "RpcCount", TUnit::UNIT);
    auto* rpc_avg_timer = ADD_TIMER(profile, "RpcAvgTime");
    auto* network_timer = ADD_TIMER(profile, "NetworkTime");
    auto* wait_timer = ADD_TIMER(profile, "WaitTime");
    auto* overall_timer = ADD_TIMER(profile, "OverallTime");

    COUNTER_SET(rpc_count, _rpc_count);
    COUNTER_SET(rpc_avg_timer, _rpc_cumulative_time / std::max(_rpc_count.load(), static_cast<int64_t>(1)));

    COUNTER_SET(network_timer, _network_time());
    COUNTER_SET(overall_timer, _last_receive_time - _first_send_time);

    // WaitTime consists two parts
    // 1. buffer full time
    // 2. pending finish time
    COUNTER_SET(wait_timer, _full_time);
    COUNTER_UPDATE(wait_timer, MonotonicNanos() - _pending_timestamp);

    auto* bytes_sent_counter = ADD_COUNTER(profile, "BytesSent", TUnit::BYTES);
    auto* request_sent_counter = ADD_COUNTER(profile, "RequestSent", TUnit::UNIT);
    COUNTER_SET(bytes_sent_counter, _bytes_sent);
    COUNTER_SET(request_sent_counter, _request_sent);

    auto* bytes_unsent_counter = ADD_COUNTER(profile, "BytesUnsent", TUnit::BYTES);
    auto* request_unsent_counter = ADD_COUNTER(profile, "RequestUnsent", TUnit::UNIT);
    COUNTER_SET(bytes_unsent_counter, _bytes_enqueued - _bytes_sent);
    COUNTER_SET(request_unsent_counter, _request_enqueued - _request_sent);

    profile->add_derived_counter(
            "NetworkBandwidth", TUnit::BYTES_PER_SECOND,
            [bytes_sent_counter, network_timer] {
                return RuntimeProfile::units_per_second(bytes_sent_counter, network_timer);
            },
            "");
    profile->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            [bytes_sent_counter, overall_timer] {
                return RuntimeProfile::units_per_second(bytes_sent_counter, overall_timer);
            },
            "");
}

int64_t SinkBuffer::_network_time() {
    int64_t max = 0;
    for (auto& [_, time_trace] : _network_times) {
        double average_concurrency =
                static_cast<double>(time_trace.accumulated_concurrency) / std::max(1, time_trace.times);
        int64_t average_accumulated_time =
                static_cast<int64_t>(time_trace.accumulated_time / std::max(1.0, average_concurrency));
        if (average_accumulated_time > max) {
            max = average_accumulated_time;
        }
    }
    return max;
}

void SinkBuffer::cancel_one_sinker(RuntimeState* const state) {
    if (--_num_uncancelled_sinkers == 0) {
        _is_finishing = true;
    }
    if (state != nullptr) {
        LOG(INFO) << fmt::format(
                "fragment_instance_id {} -> {}, _num_uncancelled_sinkers {}, _is_finishing {}, _num_remaining_eos {}",
                print_id(_fragment_ctx->fragment_instance_id()), print_id(state->fragment_instance_id()),
                _num_uncancelled_sinkers, _is_finishing, _num_remaining_eos);
    }
}

void SinkBuffer::_update_network_time(const TUniqueId& instance_id, const int64_t send_timestamp,
                                      const int64_t receiver_post_process_time) {
    const int64_t get_response_timestamp = MonotonicNanos();
    _last_receive_time = get_response_timestamp;
    int32_t concurrency = _num_in_flight_rpcs[instance_id.lo];
    int64_t time_usage = get_response_timestamp - send_timestamp - receiver_post_process_time;
    _network_times[instance_id.lo].update(time_usage, concurrency);
    _rpc_cumulative_time += time_usage;
    _rpc_count++;
}

void SinkBuffer::_process_send_window(const TUniqueId& instance_id, const int64_t sequence) {
    // Both sender side and receiver side can tolerate disorder of tranmission
    // if receiver side is not ExchangeMergeSortSourceOperator
    if (!_is_dest_merge) {
        return;
    }
    auto& seqs = _discontinuous_acked_seqs[instance_id.lo];
    seqs.insert(sequence);
    auto& max_continuous_acked_seq = _max_continuous_acked_seqs[instance_id.lo];
    std::unordered_set<int64_t>::iterator it;
    while ((it = seqs.find(max_continuous_acked_seq + 1)) != seqs.end()) {
        seqs.erase(it);
        ++max_continuous_acked_seq;
    }
}

void SinkBuffer::_try_to_merge_query_statistics(TransmitChunkInfo& request) {
    if (!request.params->has_query_statistics()) {
        return;
    }
    auto& query_statistics = request.params->query_statistics();
    bool need_merge = false;
    if (query_statistics.scan_rows() > 0 || query_statistics.scan_bytes() > 0 || query_statistics.cpu_cost_ns() > 0) {
        need_merge = true;
    }
    if (!need_merge && query_statistics.stats_items_size() > 0) {
        for (int i = 0; i < query_statistics.stats_items_size(); i++) {
            const auto& stats_item = query_statistics.stats_items(i);
            if (stats_item.scan_rows() > 0 || stats_item.scan_bytes()) {
                need_merge = true;
                break;
            }
        }
    }
    if (need_merge) {
        auto& instance_id = request.fragment_instance_id;
        _eos_query_stats[instance_id.lo]->merge_pb(query_statistics);
        request.params->clear_query_statistics();
    }
}

Status SinkBuffer::_try_to_send_rpc(const TUniqueId& instance_id, const std::function<void()>& pre_works) {
    std::lock_guard<Mutex> l(*_mutexes[instance_id.lo]);
    pre_works();

    DeferOp decrease_defer([this]() { --_num_sending_rpc; });
    ++_num_sending_rpc;

    for (;;) {
        if (_is_finishing) {
            return Status::OK();
        }

        auto& buffer = _buffers[instance_id.lo];

        bool too_much_brpc_process = false;
        if (_is_dest_merge) {
            // discontinuous_acked_window_size means that we are not received all the ack
            // with sequence from _max_continuous_acked_seqs[x] to _request_seqs[x]
            // Limit the size of the window to avoid buffering too much out-of-order data at the receiving side
            int64_t discontinuous_acked_window_size =
                    _request_seqs[instance_id.lo] - _max_continuous_acked_seqs[instance_id.lo];
            too_much_brpc_process = discontinuous_acked_window_size >= config::pipeline_sink_brpc_dop;
        } else {
            too_much_brpc_process = _num_in_flight_rpcs[instance_id.lo] >= config::pipeline_sink_brpc_dop;
        }
        if (buffer.empty() || too_much_brpc_process) {
            return Status::OK();
        }

        TransmitChunkInfo& request = buffer.front();
        bool need_wait = false;
        DeferOp pop_defer([&need_wait, &buffer]() {
            if (need_wait) {
                return;
            }

            {
                // Since the deallocation of _buffers may happen inside bthread, make sure all the allocations and
                // deallocations are executed using the process level MemTracker
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
                buffer.pop();
            }
        });

        // The order of data transmiting in IO level may not be strictly the same as
        // the order of submitting data packets
        // But we must guarantee that first packet must be received first
        if (_num_finished_rpcs[instance_id.lo] == 0 && _num_in_flight_rpcs[instance_id.lo] > 0) {
            need_wait = true;
            return Status::OK();
        }
        if (request.params->eos()) {
            DeferOp eos_defer([this, &instance_id, &need_wait]() {
                if (need_wait) {
                    return;
                }
                if (--_num_remaining_eos == 0) {
                    _is_finishing = true;
                }
                --_num_sinkers[instance_id.lo];
            });
            // Only the last eos is sent to ExchangeSourceOperator. it must be guaranteed that
            // eos is the last packet to send to finish the input stream of the corresponding of
            // ExchangeSourceOperator and eos is sent exactly-once.
            if (_num_sinkers[instance_id.lo] > 1) {
                // to reduce uncessary rpc requests, we merge all query statistics in eos requests into one and send it through the last eos request
                _try_to_merge_query_statistics(request);
                if (request.params->chunks_size() == 0) {
                    continue;
                } else {
                    request.params->set_eos(false);
                }
            } else {
                // The order of data transmiting in IO level may not be strictly the same as
                // the order of submitting data packets
                // But we must guarantee that eos packent must be the last packet
                if (_num_in_flight_rpcs[instance_id.lo] > 0) {
                    need_wait = true;
                    return Status::OK();
                }
                // this is the last eos query, set query stats
                _eos_query_stats[instance_id.lo]->merge_pb(request.params->query_statistics());
                request.params->clear_query_statistics();
                _eos_query_stats[instance_id.lo]->to_pb(request.params->mutable_query_statistics());
                _eos_query_stats[instance_id.lo]->clear();
            }
        }

        *request.params->mutable_finst_id() = _instance_id2finst_id[instance_id.lo];
        request.params->set_sequence(++_request_seqs[instance_id.lo]);

        if (!request.attachment.empty()) {
            _bytes_sent += request.attachment.size();
            _request_sent++;
        }

        auto* closure = new DisposableClosure<PTransmitChunkResult, ClosureContext>(
                {instance_id, request.params->sequence(), MonotonicNanos()});
        if (_first_send_time == -1) {
            _first_send_time = MonotonicNanos();
        }

        closure->addFailedHandler([this](const ClosureContext& ctx) noexcept {
            _is_finishing = true;
            {
                std::lock_guard<Mutex> l(*_mutexes[ctx.instance_id.lo]);
                ++_num_finished_rpcs[ctx.instance_id.lo];
                --_num_in_flight_rpcs[ctx.instance_id.lo];
            }
            --_total_in_flight_rpc;

            const auto& dest_addr = _dest_addrs[ctx.instance_id.lo];
            std::string err_msg = fmt::format("transmit chunk rpc failed [dest_instance_id={}] [dest={}:{}]",
                                              print_id(ctx.instance_id), dest_addr.hostname, dest_addr.port);

            _fragment_ctx->cancel(Status::ThriftRpcError(err_msg));
            LOG(WARNING) << err_msg;
        });
        closure->addSuccessHandler([this](const ClosureContext& ctx, const PTransmitChunkResult& result) noexcept {
            Status status(result.status());
            {
                std::lock_guard<Mutex> l(*_mutexes[ctx.instance_id.lo]);
                ++_num_finished_rpcs[ctx.instance_id.lo];
                --_num_in_flight_rpcs[ctx.instance_id.lo];
            }
            if (!status.ok()) {
                _is_finishing = true;
                _fragment_ctx->cancel(status);

                const auto& dest_addr = _dest_addrs[ctx.instance_id.lo];
                LOG(WARNING) << fmt::format("transmit chunk rpc failed [dest_instance_id={}] [dest={}:{}] [msg={}]",
                                            print_id(ctx.instance_id), dest_addr.hostname, dest_addr.port,
                                            status.message());
            } else {
                static_cast<void>(_try_to_send_rpc(ctx.instance_id, [&]() {
                    _update_network_time(ctx.instance_id, ctx.send_timestamp, result.receiver_post_process_time());
                    _process_send_window(ctx.instance_id, ctx.sequence);
                }));
            }
            --_total_in_flight_rpc;
        });

        ++_total_in_flight_rpc;
        ++_num_in_flight_rpcs[instance_id.lo];

        // Attachment will be released by process_mem_tracker in closure->Run() in bthread, when receiving the response,
        // so decrease the memory usage of attachment from instance_mem_tracker immediately before sending the request.
        _mem_tracker->release(request.attachment_physical_bytes);
        GlobalEnv::GetInstance()->process_mem_tracker()->consume(request.attachment_physical_bytes);

        closure->cntl.Reset();
        closure->cntl.set_timeout_ms(_brpc_timeout_ms);

        Status st;
        if (bthread_self()) {
            st = _send_rpc(closure, request);
        } else {
            // Since the deallocation of protobuf request must be done in the bthread.
            // When the driver worker thread sends request and creates the protobuf request,
            // also use process_mem_tracker to record the memory of the protobuf request.
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
            // must in the same scope following the above
            st = _send_rpc(closure, request);
        }
        return st;
    }
    return Status::OK();
}

Status SinkBuffer::_send_rpc(DisposableClosure<PTransmitChunkResult, ClosureContext>* closure,
                             const TransmitChunkInfo& request) {
    auto expected_iobuf_size = request.attachment.size() + request.params->ByteSizeLong() + sizeof(size_t) * 2;
    if (UNLIKELY(expected_iobuf_size > _rpc_http_min_size)) {
        butil::IOBuf iobuf;
        butil::IOBufAsZeroCopyOutputStream wrapper(&iobuf);
        request.params->SerializeToZeroCopyStream(&wrapper);
        // append params to iobuf
        size_t params_size = iobuf.size();
        closure->cntl.request_attachment().append(&params_size, sizeof(params_size));
        closure->cntl.request_attachment().append(iobuf);
        // append attachment
        size_t attachment_size = request.attachment.size();
        closure->cntl.request_attachment().append(&attachment_size, sizeof(attachment_size));
        closure->cntl.request_attachment().append(request.attachment);
        VLOG_ROW << "issue a http rpc, attachment's size = " << attachment_size
                 << " , total size = " << closure->cntl.request_attachment().size();

        if (UNLIKELY(expected_iobuf_size != closure->cntl.request_attachment().size())) {
            LOG(WARNING) << "http rpc expected iobuf size " << expected_iobuf_size << " != "
                         << " real iobuf size " << closure->cntl.request_attachment().size();
        }
        closure->cntl.http_request().set_content_type("application/proto");
        // create http_stub as needed
        auto res = HttpBrpcStubCache::getInstance()->get_http_stub(request.brpc_addr);
        if (!res.ok()) {
            return res.status();
        }
        res.value()->transmit_chunk_via_http(&closure->cntl, nullptr, &closure->result, closure);
    } else {
        closure->cntl.request_attachment().append(request.attachment);
        request.brpc_stub->transmit_chunk(&closure->cntl, request.params.get(), &closure->result, closure);
    }
    return Status::OK();
}

} // namespace starrocks::pipeline
