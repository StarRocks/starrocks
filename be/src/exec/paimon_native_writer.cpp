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

#include "paimon_native_writer.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/type.h"
#include "column/chunk.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gutil/strings/split.h"
#include "paimon/commit_message.h"
#include "paimon/file_store_write.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/write_context.h"
#include "paimon_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/arrow/starrocks_column_to_arrow.h"
#include "util/arrow/utils.h"

namespace starrocks {

PaimonNativeWriter::PaimonNativeWriter(PaimonTableDescriptor* paimon_table, std::vector<ExprContext*> output_expr,
                                       std::vector<std::string> data_column_names,
                                       std::vector<std::string> data_column_types,
                                       RuntimeProfile::Counter* convert_timer)
        : _paimon_table(paimon_table),
          _output_expr(std::move(output_expr)),
          _data_column_names(std::move(data_column_names)),
          _data_column_types(std::move(data_column_types)),
          _convert_timer(convert_timer) {}

PaimonNativeWriter::~PaimonNativeWriter() = default;

Status PaimonNativeWriter::do_init(RuntimeState* runtime_state) {
    if (_paimon_table->get_primary_keys().size() != 0) {
        return Status::NotSupported("PaimonNativeWriter doesn't support primary key.");
    }
    ASSIGN_OR_RETURN(::ArrowSchema schema, get_arrow_schema());

    const std::map<std::string, std::string>& paimon_options = _paimon_table->get_paimon_options();
    const std::string& root_path = paimon_options.at(PaimonOptions::ROOT_PATH);

    std::string paimon_native_commit_user = runtime_state->query_options().paimon_native_commit_user;
    if (paimon_native_commit_user == "") {
        paimon_native_commit_user = "root";
    }

    const std::vector<std::string>& partition_keys = _paimon_table->get_partition_keys();
    paimon::WriteContextBuilder builder(root_path, paimon_native_commit_user, &schema);
    paimon::Result<std::unique_ptr<paimon::WriteContext>> result =
            builder.SetPartitionKeys(partition_keys)
                    .SetOptions(paimon_options)
                    .AddOption(paimon::Options::FILE_SYSTEM, PaimonFileSystemFactory::IDENTIFIER)
                    .AddOption(PaimonOptions::ROOT_PATH, root_path)
                    .Finish();
    if (!result.ok()) {
        return Status::InternalError(
                fmt::format("Failed to build Paimon WriteContext, reason: {}", result.status().ToString()));
    }
    return create_file_store_write(std::move(result).value());
}

Status PaimonNativeWriter::write(RuntimeState* runtime_state, const ChunkPtr& chunk) {
    if (_file_store_write == nullptr) {
        RETURN_IF_ERROR(do_init(runtime_state));
    }
    ASSIGN_OR_RETURN(auto record_batch, convert_chunk_to_record_batch(chunk));
    auto st = _file_store_write->Write(record_batch);
    if (!st.ok()) {
        return Status::InternalError(st.message());
    }
    return Status::OK();
}

Status PaimonNativeWriter::commit(RuntimeState* runtime_state) {
    if (_file_store_write == nullptr) {
        return Status::OK();
    }
    auto st = _file_store_write->PrepareCommit();
    if (!st.ok()) {
        return Status::InternalError(fmt::format("Failed to prepare paimon commit, reason: {}", st.status().message()));
    }
    auto res = paimon::CommitMessage::SerializeList(st.value(), paimon::GetDefaultPool());
    if (!res.ok()) {
        return Status::InternalError(
                fmt::format("Failed to serialized commit message, reason: ", res.status().message()));
    }
    commit_message = std::move(res).value();
    return Status::OK();
}

void PaimonNativeWriter::close(RuntimeState* runtime_state) noexcept {
    if (_file_store_write == nullptr) {
        return;
    }
    auto metrics = _file_store_write->GetMetrics();
    // todo: update metrics
    auto st = _file_store_write->Close();
    if (!st.ok()) {
        LOG(ERROR) << "Close paimon write failed, reason: " << st.message();
    }
}

void PaimonNativeWriter::set_output_expr(std::vector<ExprContext*> output_expr) {
    _output_expr = output_expr;
}

std::string PaimonNativeWriter::get_commit_message() {
    return commit_message;
}

StatusOr<ArrowSchema> PaimonNativeWriter::get_arrow_schema() {
    DCHECK(_output_expr.size() == _data_column_names.size());
    DCHECK(_data_column_names.size() == _data_column_types.size());
    std::vector<std::shared_ptr<arrow::Field>> output_fields;
    output_fields.reserve(_data_column_names.size());
    for (int i = 0; i < _data_column_names.size(); ++i) {
        auto nullable = _output_expr[i]->root()->is_nullable();
        ASSIGN_OR_RETURN(auto data_type, starrocks_type_to_arrow(_output_expr[i]->root()->type()));
        auto field = std::make_shared<arrow::Field>(_data_column_names[i], std::move(data_type), nullable);
        output_fields.emplace_back(field);
    }
    _schema = std::make_shared<arrow::Schema>(std::move(output_fields));
    ::ArrowSchema arrow_schema;
    auto st = arrow::ExportSchema(*_schema, &arrow_schema);
    if (!st.ok()) {
        return Status::InternalError(st.message());
    }
    return std::move(arrow_schema);
}

Status PaimonNativeWriter::create_file_store_write(std::unique_ptr<paimon::WriteContext> context) {
    auto res = paimon::FileStoreWrite::Create(std::move(context));
    if (!res.ok()) {
        return Status::InternalError(res.status().message());
    }
    _file_store_write = std::move(res).value();
    return Status::OK();
}

StatusOr<std::shared_ptr<paimon::RecordBatch>> PaimonNativeWriter::convert_chunk_to_record_batch(
        const ChunkPtr& chunk) {
    SCOPED_TIMER(_convert_timer);
    std::shared_ptr<arrow::RecordBatch> record_batch;
    RETURN_IF_ERROR(convert_chunk_to_arrow_batch(chunk.get(), _output_expr, _schema, arrow::default_memory_pool(),
                                                 &record_batch));
    ::ArrowArray arrow_array;
    auto st = arrow::ExportRecordBatch(*record_batch, &arrow_array);
    if (!st.ok()) {
        return Status::InternalError(st.message());
    }
    paimon::RecordBatchBuilder builder(&arrow_array);
    auto res = builder.Finish();
    if (!res.ok()) {
        return Status::InternalError(res.status().message());
    }
    return std::move(res).value();
}

} // namespace starrocks
