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

#include "exec/pipeline/sink/export_sink_operator.h"

#include "exec/data_sink.h"
#include "exec/file_builder.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/sink/sink_io_buffer.h"
#include "exec/plain_text_builder.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream.h"
#include "fs/fs_broker.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

class ExportSinkIOBuffer final : public SinkIOBuffer {
public:
    ExportSinkIOBuffer(const TExportSink& t_export_sink, std::vector<ExprContext*>& output_expr_ctxs,
                       int32_t num_sinkers, FragmentContext* fragment_ctx)
            : SinkIOBuffer(num_sinkers),
              _t_export_sink(t_export_sink),
              _output_expr_ctxs(output_expr_ctxs),
              _fragment_ctx(fragment_ctx) {}

    ~ExportSinkIOBuffer() override = default;

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override;

    void close(RuntimeState* state) override;

private:
    void _process_chunk(bthread::TaskIterator<ChunkPtr>& iter) override;

    Status _open_file_writer();

    Status _gen_file_name(std::string* file_name);

    TExportSink _t_export_sink;
    const std::vector<ExprContext*> _output_expr_ctxs;
    std::unique_ptr<FileBuilder> _file_builder;
    FragmentContext* _fragment_ctx;
};

Status ExportSinkIOBuffer::prepare(RuntimeState* state, RuntimeProfile* parent_profile) {
    bool expected = false;
    if (!_is_prepared.compare_exchange_strong(expected, true)) {
        return Status::OK();
    }
    _state = state;

    bthread::ExecutionQueueOptions options;
    options.executor = SinkIOExecutor::instance();
    _exec_queue_id = std::make_unique<bthread::ExecutionQueueId<ChunkPtr>>();
    int ret = bthread::execution_queue_start<ChunkPtr>(_exec_queue_id.get(), &options,
                                                       &ExportSinkIOBuffer::execute_io_task, this);
    if (ret != 0) {
        _exec_queue_id.reset();
        return Status::InternalError("start execution queue error");
    }

    return Status::OK();
}

void ExportSinkIOBuffer::close(RuntimeState* state) {
    if (_file_builder != nullptr) {
        set_io_status(_file_builder->finish());
        _file_builder.reset();
    }
    SinkIOBuffer::close(state);
}

void ExportSinkIOBuffer::_process_chunk(bthread::TaskIterator<ChunkPtr>& iter) {
    sleep(5);
    std::cout << "NUM_PENDING: " << _num_pending_chunks;

    DeferOp op([&]() {
        auto nc = _num_pending_chunks.fetch_sub(1);
        DCHECK_GE(nc, 1L);
    });

    if (_is_finished) {
        return;
    }

    if (_num_pending_chunks <= 0) {
        while (!_is_cancelled) {
            sleep(1);
        }
        std::cout << "END sleep: " << (int)_is_cancelled << ":" << (int)_is_finished << ":" << (int)_num_pending_chunks
                  << std::endl;
    }

    if (_is_cancelled && !_is_finished) {
        if (_num_pending_chunks == 1) {
            close(_state);
        }
        std::cout << "LXH C 1" << std::endl;
        sleep(config::sleep_lxh_s);
        std::cout << "LXH C 2" << std::endl;
        return;
    }

    if (_file_builder == nullptr) {
        if (Status status = _open_file_writer(); !status.ok()) {
            LOG(WARNING) << "open file write failed, error: " << status.to_string();
            _fragment_ctx->cancel(status);
            return;
        }
    }
    const auto& chunk = *iter;
    if (chunk == nullptr) {
        // this is the last chunk
        DCHECK_EQ(_num_pending_chunks, 1);
        close(_state);
        return;
    }
    if (Status status = _file_builder->add_chunk(chunk.get()); !status.ok()) {
        LOG(WARNING) << "add chunk to file builder failed, error: " << status.to_string();
        _fragment_ctx->cancel(status);
        return;
    }
}

Status ExportSinkIOBuffer::_open_file_writer() {
    std::unique_ptr<WritableFile> output_file;
    std::string file_name;
    RETURN_IF_ERROR(_gen_file_name(&file_name));
    std::string file_path = _t_export_sink.export_path + "/" + file_name;
    WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::MUST_CREATE};

    const auto& file_type = _t_export_sink.file_type;
    switch (file_type) {
    case TFileType::FILE_LOCAL: {
        ASSIGN_OR_RETURN(output_file, FileSystem::Default()->new_writable_file(options, file_path));
        break;
    }
    case TFileType::FILE_BROKER: {
        if (_t_export_sink.__isset.use_broker && !_t_export_sink.use_broker) {
            ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(file_path, FSOptions(&_t_export_sink)));
            ASSIGN_OR_RETURN(output_file, fs->new_writable_file(options, file_path));
        } else {
            if (_t_export_sink.broker_addresses.empty()) {
                LOG(WARNING) << "ExportSink broker_addresses empty";
                return Status::InternalError("ExportSink broker_addresses empty");
            }
            const TNetworkAddress& broker_addr = _t_export_sink.broker_addresses[0];
            BrokerFileSystem fs_broker(broker_addr, _t_export_sink.properties);
            ASSIGN_OR_RETURN(output_file, fs_broker.new_writable_file(options, file_path));
        }
        break;
    }
    case TFileType::FILE_STREAM:
        return Status::NotSupported(strings::Substitute("Unsupported file type $0", file_type));
    }

    _file_builder = std::make_unique<PlainTextBuilder>(
            PlainTextBuilderOptions{.column_terminated_by = _t_export_sink.column_separator,
                                    .line_terminated_by = _t_export_sink.row_delimiter},
            std::move(output_file), _output_expr_ctxs);

    _state->add_export_output_file(file_path);
    return Status::OK();
}

Status ExportSinkIOBuffer::_gen_file_name(std::string* file_name) {
    if (!_t_export_sink.__isset.file_name_prefix) {
        return Status::InternalError("file name prefix is not set");
    }
    std::stringstream file_name_ss;
    // now file-number is 0.
    // <file-name-prefix>_<file-number>.csv.<timestamp>
    file_name_ss << _t_export_sink.file_name_prefix << "0.csv." << UnixMillis();
    *file_name = file_name_ss.str();
    return Status::OK();
}

Status ExportSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _export_sink_buffer->prepare(state, _unique_metrics.get());
}

void ExportSinkOperator::close(RuntimeState* state) {
    std::cout << "before close ExportSinkOpertor" << std::endl;
    Operator::close(state);
    std::cout << "after close ExportSinkOpertor" << std::endl;
}

bool ExportSinkOperator::need_input() const {
    return _export_sink_buffer->need_input();
}

bool ExportSinkOperator::is_finished() const {
    return _export_sink_buffer->is_finished();
}

Status ExportSinkOperator::set_finishing(RuntimeState* state) {
    std::cout << "SET FINISH 1" << std::endl;
    if (_num_sinkers.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        std::cout << "SET FINISH 2" << std::endl;
        _is_audit_report_done = false;
        state->exec_env()->wg_driver_executor()->report_audit_statistics(state->query_ctx(), state->fragment_ctx(),
                                                                         &_is_audit_report_done);
        std::cout << "SET FINISH 3" << std::endl;
    }
    std::cout << "SET FINISH 4" << std::endl;
    Status st = _export_sink_buffer->set_finishing();
    std::cout << "SET FINISH 5" << std::endl;
    return st;
}

bool ExportSinkOperator::pending_finish() const {
    // audit report not finish, we need check until finish
    if (!_is_audit_report_done) {
        return true;
    }
    bool ret = !_export_sink_buffer->is_finished();
    if (!ret) {
        std::cout << "pending finish 4: " << (int)(ret) << std::endl;
    }
    return ret;
}

Status ExportSinkOperator::set_cancelled(RuntimeState* state) {
    std::cout << "before cancel export operator" << std::endl;
    _export_sink_buffer->cancel_one_sinker();
    std::cout << "end cancel export operator" << std::endl;
    return Status::OK();
}

StatusOr<ChunkPtr> ExportSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from export sink operator");
}

Status ExportSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    std::cout << "BEFORE push chunk" << std::endl;
    Status st = _export_sink_buffer->append_chunk(state, chunk);
    std::cout << "AFTER push chunk" << std::endl;
    return st;
}

Status ExportSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    _export_sink_buffer =
            std::make_shared<ExportSinkIOBuffer>(_t_export_sink, _output_expr_ctxs, _total_num_sinkers, _fragment_ctx);
    return Status::OK();
}

void ExportSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
