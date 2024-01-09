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

#include "storage/segment_flush_executor.h"

#include <fmt/format.h>

#include <atomic>
#include <memory>
#include <utility>

#include "common/closure_guard.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "io/io_profiler.h"
#include "runtime/current_thread.h"
#include "service/brpc.h"
#include "storage/delta_writer.h"

namespace starrocks {

// A task responsible for flushing the segment received the primary tablet. It will also
// respond to the brpc BackendInternalServiceImpl<T>::tablet_writer_add_segment if release()
// is not called.
class SegmentFlushTask final : public Runnable {
public:
    SegmentFlushTask(SegmentFlushToken* flush_token, DeltaWriter* writer, brpc::Controller* cntl,
                     const PTabletWriterAddSegmentRequest* request, PTabletWriterAddSegmentResult* response,
                     google::protobuf::Closure* done)
            : _flush_token(flush_token),
              _writer(writer),
              _cntl(cntl),
              _request(request),
              _response(response),
              _done(done) {}

    // Destructor which will respond to the brpc if run() or release() is not called.
    ~SegmentFlushTask() override {
        if (_run_or_released.load()) {
            return;
        }
        Status status = Status::Cancelled(
                fmt::format("Segment flush task does not run, and it may be cancelled,"
                            " txn_id: {}, tablet id: {}, flush token status: {}",
                            _request->txn_id(), _request->tablet_id(), _flush_token->status().to_string()));
        _send_fail_response(status);
        VLOG(1) << "Segment flush task is destructed with failure response"
                << ", txn_id: " << _request->txn_id() << ", tablet id: " << _request->tablet_id()
                << ", flush token status: " << _flush_token->status();
    }

    // Run the task if release() is not called which will flush the segment, and respond the brpc
    // BackendInternalServiceImpl<T>::tablet_writer_add_segment.
    void run() override {
        bool expect = false;
        if (!_run_or_released.compare_exchange_strong(expect, true)) {
            return;
        }

        // if token status is not ok, respond with failure
        auto token_st = _flush_token->status();
        if (!token_st.ok()) {
            _send_fail_response(token_st);
            return;
        }

        auto st = Status::OK();
        if (_request->has_segment() && _cntl->request_attachment().size() > 0) {
            auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, _writer->tablet()->tablet_id());
            auto& segment_pb = _request->segment();
            st = _writer->write_segment(segment_pb, _cntl->request_attachment());
        } else if (!_request->eos()) {
            st = Status::InternalError(fmt::format("request {} has no segment", _request->DebugString()));
        }

        bool eos = _request->eos();
        if (st.ok() && eos) {
            st = _writer->close();
            if (st.ok()) {
                st = _writer->commit();
            }
        }

        if (!st.ok()) {
            Status cancel_st = Status::InternalError("cancel writer because fail to run flush task, " + st.to_string());
            _writer->cancel(cancel_st);
            _send_fail_response(st);
            LOG(ERROR) << "failed to flush segment, txn_id: " << _request->txn_id()
                       << ", tablet id: " << _request->tablet_id() << ", status: " << st;
        } else {
            _send_success_response(eos, st);
        }
    }

    // Release the task which means it should not be run and respond to the brpc.
    void release() {
        bool expect = false;
        _run_or_released.compare_exchange_strong(expect, true);
        VLOG(1) << "Segment flush task is released"
                << ", txn_id: " << _request->txn_id() << ", tablet id: " << _request->tablet_id()
                << ", flush token status: " << _flush_token->status();
    }

private:
    void _send_success_response(bool eos, Status& st) {
        if (eos) {
            auto* tablet_info = _response->add_tablet_vec();
            tablet_info->set_tablet_id(_writer->tablet()->tablet_id());
            tablet_info->set_schema_hash(_writer->tablet()->schema_hash());
            tablet_info->set_node_id(_writer->node_id());
            const auto& rowset_global_dict_columns_valid_info =
                    _writer->committed_rowset_writer()->global_dict_columns_valid_info();
            const auto* rowset_global_dicts = _writer->committed_rowset_writer()->rowset_global_dicts();
            for (const auto& item : rowset_global_dict_columns_valid_info) {
                if (item.second && rowset_global_dicts != nullptr &&
                    rowset_global_dicts->find(item.first) != rowset_global_dicts->end()) {
                    tablet_info->add_valid_dict_cache_columns(item.first);
                    tablet_info->add_valid_dict_collected_version(rowset_global_dicts->at(item.first).version);
                } else {
                    tablet_info->add_invalid_dict_cache_columns(item.first);
                }
            }
        }

        st.to_protobuf(_response->mutable_status());
        _done->Run();
    }

    void _send_fail_response(Status& st) {
        auto* tablet_info = _response->add_failed_tablet_vec();
        tablet_info->set_tablet_id(_writer->tablet()->tablet_id());
        tablet_info->set_node_id(_writer->node_id());
        tablet_info->set_schema_hash(0);
        st.to_protobuf(_response->mutable_status());
        _done->Run();
    }

    SegmentFlushToken* _flush_token;
    DeltaWriter* _writer;
    brpc::Controller* _cntl;
    const PTabletWriterAddSegmentRequest* _request;
    PTabletWriterAddSegmentResult* _response;
    google::protobuf::Closure* _done;
    // whether run() or release() has been called
    std::atomic<bool> _run_or_released = false;
};

SegmentFlushToken::SegmentFlushToken(std::unique_ptr<ThreadPoolToken> flush_pool_token)
        : _flush_token(std::move(flush_pool_token)) {}

Status SegmentFlushToken::submit(DeltaWriter* writer, brpc::Controller* cntl,
                                 const PTabletWriterAddSegmentRequest* request, PTabletWriterAddSegmentResult* response,
                                 google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    Status token_st = status();
    if (!token_st.ok()) {
        Status st = Status::InternalError("Segment flush token is not ok. The status: " + token_st.to_string());
        st.to_protobuf(response->mutable_status());
        return st;
    }

    auto task = std::make_shared<SegmentFlushTask>(this, writer, cntl, request, response, done);
    auto submit_st = _flush_token->submit(std::move(task));
    if (submit_st.ok()) {
        closure_guard.release();
    } else {
        task->release();
        submit_st.to_protobuf(response->mutable_status());
    }

    return submit_st;
}

void SegmentFlushToken::cancel(const Status& st) {
    set_status(st);
}

void SegmentFlushToken::shutdown() {
    _flush_token->shutdown();
}

Status SegmentFlushToken::wait() {
    _flush_token->wait();
    return status();
}

Status SegmentFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int data_dir_num = static_cast<int>(data_dirs.size());
    int min_threads = std::max<int>(1, config::flush_thread_num_per_store);
    int max_threads = std::max(data_dir_num * min_threads, min_threads);
    return ThreadPoolBuilder("segment_flush")
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_flush_pool);
}

Status SegmentFlushExecutor::update_max_threads(int max_threads) {
    if (_flush_pool != nullptr) {
        return _flush_pool->update_max_threads(max_threads);
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

std::unique_ptr<SegmentFlushToken> SegmentFlushExecutor::create_flush_token(ThreadPool::ExecutionMode execution_mode) {
    return std::make_unique<SegmentFlushToken>(_flush_pool->new_token(execution_mode));
}

} // namespace starrocks
