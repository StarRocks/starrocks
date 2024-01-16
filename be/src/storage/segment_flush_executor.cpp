// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include "storage/segment_flush_executor.h"

#include <fmt/format.h>

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

SegmentFlushToken::SegmentFlushToken(std::unique_ptr<ThreadPoolToken> flush_pool_token,
                                     std::shared_ptr<starrocks::vectorized::DeltaWriter> delta_writer)
        : _flush_token(std::move(flush_pool_token)), _writer(std::move(delta_writer)) {}

Status SegmentFlushToken::submit(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                                 PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);

    auto submit_st = _flush_token->submit_func([this, cntl, request, response, done] {
        auto& writer = this->_writer;
        auto st = Status::OK();
        if (request->has_segment() && cntl->request_attachment().size() > 0) {
            auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, _writer->tablet()->tablet_id());
            auto& segment_pb = request->segment();
            st = writer->write_segment(segment_pb, cntl->request_attachment());
        } else if (!request->eos()) {
            st = Status::InternalError(fmt::format("request {} has no segment", request->DebugString()));
        }
        if (st.ok()) {
            if (request->eos()) {
                st = writer->close();
                if (st.ok()) {
                    st = writer->commit();
                }
                if (st.ok()) {
                    auto* tablet_info = response->add_tablet_vec();
                    tablet_info->set_tablet_id(writer->tablet()->tablet_id());
                    tablet_info->set_schema_hash(writer->tablet()->schema_hash());
                    tablet_info->set_node_id(writer->node_id());
                    const auto& rowset_global_dict_columns_valid_info =
                            writer->committed_rowset_writer()->global_dict_columns_valid_info();
                    const auto* rowset_global_dicts = writer->committed_rowset_writer()->rowset_global_dicts();
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
            }
        }
        if (!st.ok()) {
            writer->abort(true);
            auto* tablet_info = response->add_failed_tablet_vec();
            tablet_info->set_tablet_id(writer->tablet()->tablet_id());
            tablet_info->set_node_id(writer->node_id());
            tablet_info->set_schema_hash(0);
        }
        st.to_protobuf(response->mutable_status());
        done->Run();
    });
    if (submit_st.ok()) {
        closure_guard.release();
    } else {
        submit_st.to_protobuf(response->mutable_status());
    }

    return submit_st;
}

void SegmentFlushToken::cancel() {
    _flush_token->shutdown();
}

void SegmentFlushToken::wait() {
    _flush_token->wait();
}

Status SegmentFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int data_dir_num = static_cast<int>(data_dirs.size());
    int min_threads = std::max<int>(1, config::flush_thread_num_per_store);
    int max_threads = data_dir_num * min_threads;
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

std::unique_ptr<SegmentFlushToken> SegmentFlushExecutor::create_flush_token(
        const std::shared_ptr<starrocks::vectorized::DeltaWriter>& delta_writer,
        ThreadPool::ExecutionMode execution_mode) {
    return std::make_unique<SegmentFlushToken>(_flush_pool->new_token(execution_mode), delta_writer);
}

} // namespace starrocks
