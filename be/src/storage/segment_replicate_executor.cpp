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

#include "storage/segment_replicate_executor.h"

#include <fmt/format.h>

#include <memory>
#include <utility>

#include "fs/fs_posix.h"
#include "gen_cpp/data.pb.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/delta_writer.h"
#include "util/brpc_stub_cache.h"
#include "util/raw_container.h"

namespace starrocks {

class SegmentReplicateTask final : public Runnable {
public:
    SegmentReplicateTask(ReplicateToken* replicate_token, std::unique_ptr<SegmentPB> segment, bool eos)
            : _replicate_token(replicate_token), _segment(std::move(segment)), _eos(eos) {}

    ~SegmentReplicateTask() override = default;

    void run() override { _replicate_token->_sync_segment(std::move(_segment), _eos); }

private:
    ReplicateToken* _replicate_token;
    std::unique_ptr<SegmentPB> _segment;
    bool _eos;
};

ReplicateChannel::ReplicateChannel(const DeltaWriterOptions* opt, std::string host, int32_t port, int64_t node_id)
        : _opt(opt), _host(std::move(host)), _port(port), _node_id(node_id) {
    _closure = new ReusableClosure<PTabletWriterAddSegmentResult>();
    _closure->ref();
}

ReplicateChannel::~ReplicateChannel() {
    if (_closure != nullptr) {
        _closure->join();
        if (_closure->unref()) {
            delete _closure;
        }
        _closure = nullptr;
    }
}

std::string ReplicateChannel::debug_string() {
    return fmt::format("SyncChannnel [host: {}, port: {}, load_id: {}, tablet_id: {}, txn_id: {}]", _host, _port,
                       print_id(_opt->load_id), _opt->tablet_id, _opt->txn_id);
}

Status ReplicateChannel::_init() {
    if (_inited) {
        return Status::OK();
    }
    _inited = true;

    _stub = ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(_host, _port);
    if (_stub == nullptr) {
        auto msg = fmt::format("Failed to Connect {} failed.", debug_string().c_str());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    return Status::OK();
}

Status ReplicateChannel::sync_segment(SegmentPB* segment, butil::IOBuf& data, bool eos,
                                      std::vector<std::unique_ptr<PTabletInfo>>* replicate_tablet_infos,
                                      std::vector<std::unique_ptr<PTabletInfo>>* failed_tablet_infos) {
    RETURN_IF_ERROR(_st);

    // 1. init sync channel
    _st = _init();
    RETURN_IF_ERROR(_st);

    // 2. send segment sync request
    _send_request(segment, data, eos);

    // 3. wait result
    RETURN_IF_ERROR(_wait_response(replicate_tablet_infos, failed_tablet_infos));

    VLOG(1) << "Sync tablet " << _opt->tablet_id << " segment id " << (segment == nullptr ? -1 : segment->segment_id())
            << " eos " << eos << " to [" << _host << ":" << _port << "] res " << _closure->result.DebugString();

    return _st;
}

Status ReplicateChannel::async_segment(SegmentPB* segment, butil::IOBuf& data, bool eos,
                                       std::vector<std::unique_ptr<PTabletInfo>>* replicate_tablet_infos,
                                       std::vector<std::unique_ptr<PTabletInfo>>* failed_tablet_infos) {
    RETURN_IF_ERROR(_st);

    VLOG(1) << "Async tablet " << _opt->tablet_id << " segment id " << (segment == nullptr ? -1 : segment->segment_id())
            << " eos " << eos << " to [" << _host << ":" << _port;

    // 1. init sync channel
    _st = _init();
    RETURN_IF_ERROR(_st);

    // 2. wait pre request's result
    RETURN_IF_ERROR(_wait_response(replicate_tablet_infos, failed_tablet_infos));

    // 3. send segment sync request
    _send_request(segment, data, eos);

    // 4. wait if eos=true
    if (eos) {
        RETURN_IF_ERROR(_wait_response(replicate_tablet_infos, failed_tablet_infos));
    }

    VLOG(1) << "Asynced tablet " << _opt->tablet_id << " segment id "
            << (segment == nullptr ? -1 : segment->segment_id()) << " eos " << eos << " to [" << _host << ":" << _port
            << "] res " << _closure->result.DebugString();

    return _st;
}

void ReplicateChannel::_send_request(SegmentPB* segment, butil::IOBuf& data, bool eos) {
    PTabletWriterAddSegmentRequest request;
    request.set_allocated_id(const_cast<starrocks::PUniqueId*>(&_opt->load_id));
    request.set_tablet_id(_opt->tablet_id);
    request.set_eos(eos);
    request.set_txn_id(_opt->txn_id);
    request.set_index_id(_opt->index_id);

    _closure->ref();
    _closure->reset();
    _closure->cntl.set_timeout_ms(_opt->timeout_ms);

    if (segment != nullptr) {
        request.set_allocated_segment(segment);
        _closure->cntl.request_attachment().append(data);
    }

    _stub->tablet_writer_add_segment(&_closure->cntl, &request, &_closure->result, _closure);

    request.release_id();
    if (segment != nullptr) {
        request.release_segment();
    }
}

Status ReplicateChannel::_wait_response(std::vector<std::unique_ptr<PTabletInfo>>* replicate_tablet_infos,
                                        std::vector<std::unique_ptr<PTabletInfo>>* failed_tablet_infos) {
    if (_closure->join()) {
        if (_closure->cntl.Failed()) {
            _st = Status::InternalError(_closure->cntl.ErrorText());
            LOG(WARNING) << "Failed to send rpc to " << debug_string() << " err=" << _st;
            return _st;
        }
        _st = _closure->result.status();
        if (!_st.ok()) {
            LOG(WARNING) << "Failed to send rpc to " << debug_string() << " err=" << _st;
            return _st;
        }

        for (size_t i = 0; i < _closure->result.tablet_vec_size(); ++i) {
            replicate_tablet_infos->emplace_back(std::make_unique<PTabletInfo>());
            replicate_tablet_infos->back()->Swap(_closure->result.mutable_tablet_vec(i));
        }

        for (size_t i = 0; i < _closure->result.failed_tablet_vec_size(); ++i) {
            failed_tablet_infos->emplace_back(std::make_unique<PTabletInfo>());
            failed_tablet_infos->back()->Swap(_closure->result.mutable_failed_tablet_vec(i));
        }
    }

    return Status::OK();
}

void ReplicateChannel::cancel() {
    if (!_init().ok()) {
        return;
    }

    // cancel rpc request, accelerate the release of related resources
    // Cancel an already-cancelled call_id has no effect.
    _closure->cancel();
}

ReplicateToken::ReplicateToken(std::unique_ptr<ThreadPoolToken> replicate_pool_token, const DeltaWriterOptions* opt)
        : _replicate_token(std::move(replicate_pool_token)), _status(), _opt(opt), _fs(new_fs_posix()) {
    // first replica is primary replica, skip it
    for (size_t i = 1; i < opt->replicas.size(); ++i) {
        _replicate_channels.emplace_back(std::make_unique<ReplicateChannel>(
                opt, opt->replicas[i].host(), opt->replicas[i].port(), opt->replicas[i].node_id()));
    }
    if (opt->write_quorum == WriteQuorumTypePB::ONE) {
        _max_fail_replica_num = opt->replicas.size();
    } else if (opt->write_quorum == WriteQuorumTypePB::ALL) {
        _max_fail_replica_num = 0;
    } else {
        _max_fail_replica_num = opt->replicas.size() - (opt->replicas.size() / 2 + 1);
    }
}

Status ReplicateToken::submit(std::unique_ptr<SegmentPB> segment, bool eos) {
    RETURN_IF_ERROR(status());
    if (segment == nullptr && !eos) {
        return Status::InternalError(fmt::format("{} segment=null eos=false", debug_string()));
    }
    auto task = std::make_shared<SegmentReplicateTask>(this, std::move(segment), eos);
    return _replicate_token->submit(std::move(task));
}

void ReplicateToken::cancel(const Status& st) {
    set_status(st);
}

void ReplicateToken::shutdown() {
    _replicate_token->shutdown();
}

Status ReplicateToken::wait() {
    _replicate_token->wait();
    std::lock_guard l(_status_lock);
    return _status;
}

std::string ReplicateToken::debug_string() {
    return fmt::format("[ReplicateToken tablet_id: {}, txn_id: {}]", _opt->tablet_id, _opt->txn_id);
}

void ReplicateToken::_sync_segment(std::unique_ptr<SegmentPB> segment, bool eos) {
    // If previous sync has failed, return directly
    if (!status().ok()) return;

    // 1. read segment from local storage
    butil::IOBuf data;
    if (segment) {
        // 1.1 read segment file
        if (segment->has_path()) {
            auto res = _fs->new_random_access_file(segment->path());
            if (!res.ok()) {
                LOG(WARNING) << "Failed to open segment file " << segment->DebugString() << " by " << debug_string()
                             << " err " << res.status();
                return set_status(res.status());
            }
            auto rfile = std::move(res.value());
            auto buf = new uint8[segment->data_size()];
            data.append_user_data(buf, segment->data_size(), [](void* buf) { delete[](uint8*) buf; });
            auto st = rfile->read_fully(buf, segment->data_size());
            if (!st.ok()) {
                LOG(WARNING) << "Failed to read segment " << segment->DebugString() << " by " << debug_string()
                             << " err " << st;
                return set_status(st);
            }
        }
        if (segment->has_delete_path()) {
            auto res = _fs->new_random_access_file(segment->delete_path());
            if (!res.ok()) {
                LOG(WARNING) << "Failed to open delete file " << segment->DebugString() << " by " << debug_string()
                             << " err " << res.status();
                return set_status(res.status());
            }
            auto rfile = std::move(res.value());
            auto buf = new uint8[segment->delete_data_size()];
            data.append_user_data(buf, segment->delete_data_size(), [](void* buf) { delete[](uint8*) buf; });
            auto st = rfile->read_fully(buf, segment->delete_data_size());
            if (!st.ok()) {
                LOG(WARNING) << "Failed to read delete file " << segment->DebugString() << " by " << debug_string()
                             << " err " << st;
                return set_status(st);
            }
        }
    }

    // 2. send segment to secondary replica
    for (auto& channel : _replicate_channels) {
        auto st = Status::OK();
        if (_failed_node_id.count(channel->node_id()) == 0) {
            st = channel->async_segment(segment.get(), data, eos, &_replicated_tablet_infos, &_failed_tablet_infos);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to sync segment " << channel->debug_string() << " err " << st;
                channel->cancel();
                _failed_node_id.insert(channel->node_id());
            }
        }

        if (_failed_node_id.size() > _max_fail_replica_num) {
            LOG(WARNING) << "Failed to sync segment err " << st << " by " << debug_string() << " fail_num "
                         << _failed_node_id.size() << " max_fail_num " << _max_fail_replica_num;
            for (auto& channel : _replicate_channels) {
                if (_failed_node_id.count(channel->node_id()) == 0) {
                    channel->cancel();
                }
            }
            return set_status(st);
        }
    }
}

Status SegmentReplicateExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int data_dir_num = static_cast<int>(data_dirs.size());
    int min_threads = std::max<int>(1, config::flush_thread_num_per_store);
    int max_threads = data_dir_num * min_threads;
    return ThreadPoolBuilder("segment_replicate")
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_replicate_pool);
}

Status SegmentReplicateExecutor::update_max_threads(int max_threads) {
    if (_replicate_pool != nullptr) {
        return _replicate_pool->update_max_threads(max_threads);
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

std::unique_ptr<ReplicateToken> SegmentReplicateExecutor::create_replicate_token(
        const DeltaWriterOptions* opt, ThreadPool::ExecutionMode execution_mode) {
    return std::make_unique<ReplicateToken>(_replicate_pool->new_token(execution_mode), opt);
}

} // namespace starrocks
