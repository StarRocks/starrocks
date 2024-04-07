// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/local_tablets_channel.h"

#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "common/closure_guard.h"
#include "common/statusor.h"
#include "exec/tablet_info.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/ref_counted.h"
#include "gutil/strings/join.h"
#include "runtime/descriptors.h"
#include "runtime/global_dict/types.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "runtime/load_channel.h"
#include "runtime/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "serde/protobuf_serde.h"
#include "storage/delta_writer.h"
#include "storage/memtable.h"
#include "storage/segment_flush_executor.h"
#include "storage/segment_replicate_executor.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"
#include "util/brpc_stub_cache.h"
#include "util/compression/block_compression.h"
#include "util/faststring.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

std::atomic<uint64_t> LocalTabletsChannel::_s_tablet_writer_count;

LocalTabletsChannel::LocalTabletsChannel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                         MemTracker* mem_tracker)
        : TabletsChannel(),
          _load_channel(load_channel),
          _key(key),
          _mem_tracker(mem_tracker),
          _mem_pool(std::make_unique<MemPool>()) {
    static std::once_flag once_flag;
    std::call_once(once_flag, [] {
        REGISTER_GAUGE_STARROCKS_METRIC(tablet_writer_count, [&]() { return _s_tablet_writer_count.load(); });
    });
}

LocalTabletsChannel::~LocalTabletsChannel() {
    _s_tablet_writer_count -= _delta_writers.size();
    _mem_pool.reset();
}

Status LocalTabletsChannel::open(const PTabletWriterOpenRequest& params, std::shared_ptr<OlapTableSchemaParam> schema) {
    _txn_id = params.txn_id();
    _index_id = params.index_id();
    _schema = schema;
    _tuple_desc = _schema->tuple_desc();
    _node_id = params.node_id();

    _num_remaining_senders.store(params.num_senders(), std::memory_order_release);
    _senders = std::vector<Sender>(params.num_senders());

    RETURN_IF_ERROR(_open_all_writers(params));
    return Status::OK();
}

void LocalTabletsChannel::add_segment(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                                      PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    auto it = _delta_writers.find(request->tablet_id());
    if (it == _delta_writers.end()) {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs(
                fmt::format("PTabletWriterAddSegmentRequest tablet_id {} not exists", request->tablet_id()));
        return;
    }
    auto& delta_writer = it->second;

    AsyncDeltaWriterSegmentRequest req;
    req.cntl = cntl;
    req.request = request;
    req.response = response;
    req.done = done;

    delta_writer->write_segment(req);
    closure_guard.release();
}

void LocalTabletsChannel::add_chunk(vectorized::Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                                    PTabletWriterAddBatchResult* response) {
    auto t0 = std::chrono::steady_clock::now();

    if (UNLIKELY(!request.has_sender_id())) {
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs("no sender_id in PTabletWriterAddChunkRequest");
        return;
    }
    if (UNLIKELY(request.sender_id() < 0)) {
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs("negative sender_id in PTabletWriterAddChunkRequest");
        return;
    }
    if (UNLIKELY(request.sender_id() >= _senders.size())) {
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs(
                fmt::format("invalid sender_id {} in PTabletWriterAddChunkRequest, limit={}", request.sender_id(),
                            _senders.size()));
        return;
    }
    if (UNLIKELY(!request.has_packet_seq())) {
        response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
        response->mutable_status()->add_error_msgs("no packet_seq in PTabletWriterAddChunkRequest");
        return;
    }

    {
        std::lock_guard lock(_senders[request.sender_id()].lock);

        // receive exists packet
        if (_senders[request.sender_id()].receive_sliding_window.count(request.packet_seq()) != 0) {
            if (_senders[request.sender_id()].success_sliding_window.count(request.packet_seq()) == 0 ||
                request.eos()) {
                // still in process
                response->mutable_status()->set_status_code(TStatusCode::DUPLICATE_RPC_INVOCATION);
                response->mutable_status()->add_error_msgs(fmt::format(
                        "packet_seq {} in PTabletWriterAddChunkRequest already process", request.packet_seq()));
                return;
            } else {
                // already success
                LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                          << " packet_seq " << request.packet_seq()
                          << " in PTabletWriterAddChunkRequest already success";
                response->mutable_status()->set_status_code(TStatusCode::OK);
                return;
            }
        } else {
            // receive packet before sliding window
            if (request.packet_seq() <= _senders[request.sender_id()].last_sliding_packet_seq) {
                LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                          << " packet_seq " << request.packet_seq()
                          << " in PTabletWriterAddChunkRequest less than last success packet_seq "
                          << _senders[request.sender_id()].last_sliding_packet_seq;
                response->mutable_status()->set_status_code(TStatusCode::OK);
                return;
            } else if (request.packet_seq() >
                       _senders[request.sender_id()].last_sliding_packet_seq + _max_sliding_window_size) {
                response->mutable_status()->set_status_code(TStatusCode::INVALID_ARGUMENT);
                response->mutable_status()->add_error_msgs(fmt::format(
                        "packet_seq {} in PTabletWriterAddChunkRequest forward last success packet_seq {} too much",
                        request.packet_seq(), _senders[request.sender_id()].last_sliding_packet_seq));
                return;
            } else {
                _senders[request.sender_id()].receive_sliding_window.insert(request.packet_seq());
            }
        }
    }

    auto res = _create_write_context(chunk, request, response);
    if (!res.ok()) {
        res.status().to_protobuf(response->mutable_status());
        return;
    } else {
        // Assuming that most writes will be successful, by setting the status code to OK before submitting
        // `AsyncDeltaWriterRequest`s, there will be no lock contention most of the time in
        // `WriteContext::update_status()`
        response->mutable_status()->set_status_code(TStatusCode::OK);
    }

    auto context = std::move(res).value();
    auto channel_size = chunk != nullptr ? _tablet_id_to_sorted_indexes.size() : 0;
    auto tablet_ids = request.tablet_ids().data();
    auto channel_row_idx_start_points = context->_channel_row_idx_start_points.get(); // May be a nullptr
    auto row_indexes = context->_row_indexes.get();                                   // May be a nullptr

    auto count_down_latch = BThreadCountDownLatch(1);

    context->set_count_down_latch(&count_down_latch);

    std::unordered_map<int64_t, std::vector<int64_t>> node_id_to_abort_tablets;
    context->set_node_id_to_abort_tablets(&node_id_to_abort_tablets);

    for (int i = 0; i < channel_size; ++i) {
        size_t from = channel_row_idx_start_points[i];
        size_t size = channel_row_idx_start_points[i + 1] - from;
        if (size == 0) {
            continue;
        }
        auto tablet_id = tablet_ids[row_indexes[from]];
        auto it = _delta_writers.find(tablet_id);
        DCHECK(it != _delta_writers.end());
        auto& delta_writer = it->second;

        AsyncDeltaWriterRequest req;
        req.chunk = chunk;
        req.indexes = row_indexes + from;
        req.indexes_size = size;
        req.commit_after_write = false;

        // The reference count of context is increased in the constructor of WriteCallback
        // and decreased in the destructor of WriteCallback.
        auto cb = new WriteCallback(context);

        delta_writer->write(req, cb);
    }

    // _channel_row_idx_start_points no longer used, release it to free memory.
    context->_channel_row_idx_start_points.reset();

    bool close_channel = false;

    // NOTE: Must close sender *AFTER* the write requests submitted, otherwise a delta writer commit request may
    // be executed ahead of the write requests submitted by other senders.
    if (request.eos() && _close_sender(request.partition_ids().data(), request.partition_ids_size()) == 0) {
        close_channel = true;
        _commit_tablets(request, context);
    }

    // Must reset the context pointer before waiting on the |count_down_latch|,
    // because the |count_down_latch| is decreased in the destructor of the context,
    // and the destructor of the context cannot be called unless we reset the pointer
    // here.
    context.reset();

    // This will only block the bthread, will not block the pthread
    count_down_latch.wait();

    // Abort tablets which primary replica already failed
    if (response->status().status_code() != TStatusCode::OK) {
        _abort_replica_tablets(request, response->status().error_msgs()[0], node_id_to_abort_tablets);
    }

    // We need wait all secondary replica commit before we close the channel
    if (_is_replicated_storage && close_channel && response->status().status_code() == TStatusCode::OK) {
        bool timeout = false;
        for (auto& [tablet_id, delta_writer] : _delta_writers) {
            // Wait util seconary replica commit/abort by primary
            if (delta_writer->replica_state() == vectorized::Secondary) {
                int i = 0;
                do {
                    auto state = delta_writer->get_state();
                    if (state == vectorized::kCommitted || state == vectorized::kAborted ||
                        state == vectorized::kUninitialized) {
                        break;
                    }
                    i++;
                    // only sleep in bthread
                    bthread_usleep(10000); // 10ms
                    auto t1 = std::chrono::steady_clock::now();
                    if (std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() / 1000 >
                        request.timeout_ms()) {
                        LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                                  << " wait tablet " << tablet_id << " secondary replica finish timeout "
                                  << request.timeout_ms() << "ms still in state " << state;
                        timeout = true;
                        break;
                    }

                    if (i % 6000 == 0) {
                        LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id())
                                  << " wait tablet " << tablet_id << " secondary replica finish already "
                                  << std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() / 1000
                                  << "ms still in state " << state;
                    }
                } while (true);
            }
            if (timeout) {
                break;
            }
        }
    }

    {
        std::lock_guard lock(_senders[request.sender_id()].lock);

        _senders[request.sender_id()].success_sliding_window.insert(request.packet_seq());
        while (_senders[request.sender_id()].success_sliding_window.size() > _max_sliding_window_size / 2) {
            auto last_success_iter = _senders[request.sender_id()].success_sliding_window.cbegin();
            auto last_receive_iter = _senders[request.sender_id()].receive_sliding_window.cbegin();
            if (_senders[request.sender_id()].last_sliding_packet_seq + 1 == *last_success_iter &&
                *last_success_iter == *last_receive_iter) {
                _senders[request.sender_id()].receive_sliding_window.erase(last_receive_iter);
                _senders[request.sender_id()].success_sliding_window.erase(last_success_iter);
                _senders[request.sender_id()].last_sliding_packet_seq++;
            }
        }
    }

    if (close_channel) {
        _load_channel->remove_tablets_channel(_index_id);

        // persist txn.
        std::vector<TabletSharedPtr> tablets;
        tablets.reserve(request.tablet_ids().size());
        for (const auto tablet_id : request.tablet_ids()) {
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
            if (tablet != nullptr) {
                tablets.emplace_back(std::move(tablet));
            }
        }
        auto st = StorageEngine::instance()->txn_manager()->persist_tablet_related_txns(tablets);
        LOG_IF(WARNING, !st.ok()) << "failed to persist transactions: " << st;
    }

    int64_t last_execution_time_us = 0;
    if (response->has_execution_time_us()) {
        last_execution_time_us = response->execution_time_us();
    }
    auto t1 = std::chrono::steady_clock::now();
    response->set_execution_time_us(last_execution_time_us +
                                    std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
    response->set_wait_lock_time_us(0); // We didn't measure the lock wait time, just give the caller a fake time

    // reset error message if it already set by other replica
    {
        std::lock_guard l(_status_lock);
        if (!_status.ok()) {
            response->mutable_status()->set_status_code(_status.code());
            response->mutable_status()->add_error_msgs(_status.get_error_msg());
        }
    }
}

void LocalTabletsChannel::_abort_replica_tablets(
        const PTabletWriterAddChunkRequest& request, const std::string& abort_reason,
        const std::unordered_map<int64_t, std::vector<int64_t>>& node_id_to_abort_tablets) {
    for (auto& [node_id, tablet_ids] : node_id_to_abort_tablets) {
        auto& endpoint = _node_id_to_endpoint[node_id];
        auto stub = ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(endpoint.host(), endpoint.port());
        if (stub == nullptr) {
            auto msg =
                    fmt::format("Failed to Connect node {} {}:{} failed.", node_id, endpoint.host(), endpoint.port());
            LOG(WARNING) << msg;
            continue;
        }

        PTabletWriterCancelRequest cancel_request;
        *cancel_request.mutable_id() = request.id();
        cancel_request.set_sender_id(0);
        cancel_request.mutable_tablet_ids()->CopyFrom({tablet_ids.begin(), tablet_ids.end()});
        cancel_request.set_txn_id(_txn_id);
        cancel_request.set_index_id(_index_id);
        cancel_request.set_reason(abort_reason);

        auto closure = new ReusableClosure<PTabletWriterCancelResult>();

        closure->ref();
        closure->cntl.set_timeout_ms(request.timeout_ms());

        string node_abort_tablet_id_list_str;
        JoinInts(tablet_ids, ",", &node_abort_tablet_id_list_str);

        stub->tablet_writer_cancel(&closure->cntl, &cancel_request, &closure->result, closure);

        VLOG(1) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id()) << " Cancel "
                << tablet_ids.size() << " tablets " << node_abort_tablet_id_list_str << " request to "
                << endpoint.host() << ":" << endpoint.port();
    }
}

void LocalTabletsChannel::_commit_tablets(const PTabletWriterAddChunkRequest& request,
                                          std::shared_ptr<LocalTabletsChannel::WriteContext> context) {
    vector<int64_t> commit_tablet_ids;
    std::unordered_map<int64_t, std::vector<int64_t>> node_id_to_abort_tablets;
    {
        std::lock_guard l1(_partitions_ids_lock);
        for (auto& [tablet_id, delta_writer] : _delta_writers) {
            // Secondary replica will commit/abort by Primary replica
            if (delta_writer->replica_state() != vectorized::Secondary) {
                if (UNLIKELY(_partition_ids.count(delta_writer->partition_id()) == 0)) {
                    // no data load, abort txn without printing log
                    delta_writer->abort(false);

                    // secondary replicas need abort by primary replica
                    if (_is_replicated_storage) {
                        auto& replicas = delta_writer->replicas();
                        for (int i = 1; i < replicas.size(); ++i) {
                            node_id_to_abort_tablets[replicas[i].node_id()].emplace_back(tablet_id);
                        }
                    }
                } else {
                    auto cb = new WriteCallback(context);
                    delta_writer->commit(cb);
                    commit_tablet_ids.emplace_back(tablet_id);
                }
            }
        }
    }
    string commit_tablet_id_list_str;
    JoinInts(commit_tablet_ids, ",", &commit_tablet_id_list_str);
    LOG(INFO) << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(request.id()) << " commit "
              << commit_tablet_ids.size() << " tablets: " << commit_tablet_id_list_str;

    // abort seconary replicas located on other nodes which have no data
    _abort_replica_tablets(request, "", node_id_to_abort_tablets);
}

int LocalTabletsChannel::_close_sender(const int64_t* partitions, size_t partitions_size) {
    std::lock_guard l(_partitions_ids_lock);
    for (int i = 0; i < partitions_size; i++) {
        _partition_ids.insert(partitions[i]);
    }
    // when replicated storage is true, the partitions of each sender will be different
    // So we need to make sure that all partitions are added to _partition_ids when committing
    int n = _num_remaining_senders.fetch_sub(1);
    DCHECK_GE(n, 1);
    return n - 1;
}

Status LocalTabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& params) {
    std::vector<SlotDescriptor*>* index_slots = nullptr;
    int32_t schema_hash = 0;
    for (auto& index : _schema->indexes()) {
        if (index->index_id == _index_id) {
            index_slots = &index->slots;
            schema_hash = index->schema_hash;
            break;
        }
    }
    if (index_slots == nullptr) {
        return Status::InvalidArgument(fmt::format("Unknown index_id: {}", _key.to_string()));
    }
    // init global dict info if needed
    for (auto& slot : params.schema().slot_descs()) {
        vectorized::GlobalDictMap global_dict;
        if (slot.global_dict_words_size()) {
            for (size_t i = 0; i < slot.global_dict_words_size(); i++) {
                const std::string& dict_word = slot.global_dict_words(i);
                auto* data = _mem_pool->allocate(dict_word.size());
                RETURN_IF_UNLIKELY_NULL(data, Status::MemoryAllocFailed("alloc mem for global dict failed"));
                memcpy(data, dict_word.data(), dict_word.size());
                Slice slice(data, dict_word.size());
                global_dict.emplace(slice, i);
            }
            vectorized::GlobalDictsWithVersion<vectorized::GlobalDictMap> dict;
            dict.dict = std::move(global_dict);
            dict.version = slot.has_global_dict_version() ? slot.global_dict_version() : 0;
            _global_dicts.emplace(std::make_pair(slot.col_name(), std::move(dict)));
        }
    }

    _is_replicated_storage = params.is_replicated_storage();
    std::vector<int64_t> tablet_ids;
    tablet_ids.reserve(params.tablets_size());
    std::vector<int64_t> failed_tablet_ids;
    for (const PTabletWithPartition& tablet : params.tablets()) {
        vectorized::DeltaWriterOptions options;
        options.tablet_id = tablet.tablet_id();
        options.schema_hash = schema_hash;
        options.txn_id = _txn_id;
        options.partition_id = tablet.partition_id();
        options.load_id = params.id();
        options.slots = index_slots;
        options.global_dicts = &_global_dicts;
        options.parent_span = _load_channel->get_span();
        options.index_id = _index_id;
        options.node_id = _node_id;
        options.timeout_ms = params.timeout_ms();
        options.write_quorum = params.write_quorum();
        if (params.is_replicated_storage()) {
            for (auto& replica : tablet.replicas()) {
                options.replicas.emplace_back(replica);
                if (_node_id_to_endpoint.count(replica.node_id()) == 0) {
                    _node_id_to_endpoint[replica.node_id()] = replica;
                }
            }
            if (options.replicas.size() > 0 && options.replicas[0].node_id() == options.node_id) {
                options.replica_state = vectorized::Primary;
            } else {
                options.replica_state = vectorized::Secondary;
            }
        } else {
            options.replica_state = vectorized::Peer;
        }
        options.merge_condition = params.merge_condition();

        auto res = AsyncDeltaWriter::open(options, _mem_tracker);
        if (res.status().ok()) {
            auto writer = std::move(res).value();
            _delta_writers.emplace(tablet.tablet_id(), std::move(writer));
            tablet_ids.emplace_back(tablet.tablet_id());
        } else {
            if (options.replica_state == vectorized::Secondary) {
                failed_tablet_ids.emplace_back(tablet.tablet_id());
            } else {
                return res.status();
            }
        }
    }
    _s_tablet_writer_count += _delta_writers.size();
    DCHECK_EQ(_delta_writers.size(), params.tablets_size());
    // In order to get sorted index for each tablet
    std::sort(tablet_ids.begin(), tablet_ids.end());
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        _tablet_id_to_sorted_indexes.emplace(tablet_ids[i], i);
    }
    if (_is_replicated_storage) {
        std::stringstream ss;
        ss << "LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << print_id(params.id()) << " open "
           << _delta_writers.size() << " delta writer: ";
        for (auto& [tablet_id, delta_writer] : _delta_writers) {
            ss << "[" << tablet_id << ":" << delta_writer->replica_state() << "]";
        }
        ss << " " << failed_tablet_ids.size() << " failed_tablets: ";
        for (auto& tablet_id : failed_tablet_ids) {
            ss << tablet_id << ",";
        }
        LOG(INFO) << ss.str();
    }
    return Status::OK();
}

void LocalTabletsChannel::cancel() {
    for (auto& it : _delta_writers) {
        it.second->cancel(Status::Cancelled("cancel"));
    }
}

void LocalTabletsChannel::abort() {
    vector<int64_t> tablet_ids;
    tablet_ids.reserve(_delta_writers.size());
    for (auto& it : _delta_writers) {
        (void)it.second->abort(false);
        tablet_ids.emplace_back(it.first);
    }
    string tablet_id_list_str;
    JoinInts(tablet_ids, ",", &tablet_id_list_str);
    LOG(INFO) << "cancel LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << _key.id
              << " index_id: " << _key.index_id << " #tablet:" << _delta_writers.size()
              << " tablet_ids:" << tablet_id_list_str;
}

void LocalTabletsChannel::abort(const std::vector<int64_t>& tablet_ids, const std::string& reason) {
    bool abort_with_exception = !reason.empty();
    for (auto tablet_id : tablet_ids) {
        auto it = _delta_writers.find(tablet_id);
        if (it != _delta_writers.end()) {
            it->second->cancel(Status::Cancelled(reason));
            it->second->abort(abort_with_exception);
        }
    }
    string tablet_id_list_str;
    JoinInts(tablet_ids, ",", &tablet_id_list_str);
    LOG(INFO) << "cancel LocalTabletsChannel txn_id: " << _txn_id << " load_id: " << _key.id
              << " index_id: " << _key.index_id << " tablet_ids:" << tablet_id_list_str;

    if (abort_with_exception) {
        std::lock_guard l(_status_lock);
        _status = Status::Aborted(reason);
    }
}

StatusOr<std::shared_ptr<LocalTabletsChannel::WriteContext>> LocalTabletsChannel::_create_write_context(
        vectorized::Chunk* chunk, const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response) {
    if (chunk == nullptr && !request.eos()) {
        return Status::InvalidArgument("PTabletWriterAddChunkRequest has no chunk or eos");
    }

    auto context = std::make_shared<WriteContext>(response);

    if (chunk == nullptr) {
        return std::move(context);
    }

    if (UNLIKELY(request.tablet_ids_size() != chunk->num_rows())) {
        return Status::InvalidArgument("request.tablet_ids_size() != chunk.num_rows()");
    }

    const auto channel_size = _tablet_id_to_sorted_indexes.size();
    context->_row_indexes = std::make_unique<uint32_t[]>(chunk->num_rows());
    context->_channel_row_idx_start_points = std::make_unique<uint32_t[]>(channel_size + 1);

    auto& row_indexes = context->_row_indexes;
    auto& channel_row_idx_start_points = context->_channel_row_idx_start_points;

    // compute row indexes for each channel
    for (uint32_t i = 0; i < request.tablet_ids_size(); ++i) {
        uint32_t channel_index = _tablet_id_to_sorted_indexes[request.tablet_ids(i)];
        channel_row_idx_start_points[channel_index]++;
    }

    // NOTE: we make the last item equal with number of rows of this chunk
    for (int i = 1; i <= channel_size; ++i) {
        channel_row_idx_start_points[i] += channel_row_idx_start_points[i - 1];
    }

    auto tablet_ids = request.tablet_ids().data();
    auto tablet_ids_size = request.tablet_ids_size();
    for (int i = tablet_ids_size - 1; i >= 0; --i) {
        const auto& tablet_id = tablet_ids[i];
        auto it = _tablet_id_to_sorted_indexes.find(tablet_id);
        if (UNLIKELY(it == _tablet_id_to_sorted_indexes.end())) {
            return Status::InternalError("invalid tablet id");
        }
        uint32_t channel_index = it->second;
        row_indexes[channel_row_idx_start_points[channel_index] - 1] = i;
        channel_row_idx_start_points[channel_index]--;
    }
    return std::move(context);
}

void LocalTabletsChannel::WriteCallback::run(const Status& st, const CommittedRowsetInfo* committed_info,
                                             const FailedRowsetInfo* failed_info) {
    _context->update_status(st);
    if (failed_info != nullptr) {
        PTabletInfo tablet_info;
        tablet_info.set_tablet_id(failed_info->tablet_id);
        // unused but it's required field of protobuf
        tablet_info.set_schema_hash(0);
        _context->add_failed_tablet_info(&tablet_info);

        // failed tablets from seconary replica
        if (failed_info->replicate_token) {
            const auto failed_tablet_infos = failed_info->replicate_token->failed_tablet_infos();
            for (const auto& failed_tablet_info : *failed_tablet_infos) {
                _context->add_failed_tablet_info(failed_tablet_info.get());
            }

            // primary replica already fail, we need cancel all secondary replica whether it's failed or not
            auto failed_replica_node_ids = failed_info->replicate_token->replica_node_ids();
            for (auto& node_id : failed_replica_node_ids) {
                _context->add_failed_replica_node_id(node_id, failed_info->tablet_id);
            }
        }
    }
    if (committed_info != nullptr) {
        // committed tablets from primary replica
        // TODO: dup code with SegmentFlushToken::submit
        PTabletInfo tablet_info;
        tablet_info.set_tablet_id(committed_info->tablet->tablet_id());
        tablet_info.set_schema_hash(committed_info->tablet->schema_hash());
        const auto& rowset_global_dict_columns_valid_info =
                committed_info->rowset_writer->global_dict_columns_valid_info();
        const auto* rowset_global_dicts = committed_info->rowset_writer->rowset_global_dicts();
        for (const auto& item : rowset_global_dict_columns_valid_info) {
            if (item.second && rowset_global_dicts != nullptr &&
                rowset_global_dicts->find(item.first) != rowset_global_dicts->end()) {
                tablet_info.add_valid_dict_cache_columns(item.first);
                tablet_info.add_valid_dict_collected_version(rowset_global_dicts->at(item.first).version);
            } else {
                tablet_info.add_invalid_dict_cache_columns(item.first);
            }
        }
        _context->add_committed_tablet_info(&tablet_info);

        // committed tablets from seconary replica
        if (committed_info->replicate_token) {
            const auto replicated_tablet_infos = committed_info->replicate_token->replicated_tablet_infos();
            for (const auto& synced_tablet_info : *replicated_tablet_infos) {
                _context->add_committed_tablet_info(synced_tablet_info.get());
            }
        }
    }
    delete this;
}

std::shared_ptr<TabletsChannel> new_local_tablets_channel(LoadChannel* load_channel, const TabletsChannelKey& key,
                                                          MemTracker* mem_tracker) {
    return std::make_shared<LocalTabletsChannel>(load_channel, key, mem_tracker);
}

} // namespace starrocks
