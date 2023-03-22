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

#include "parquet_builder.h"

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/logging.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

ParquetBuilder::ParquetBuilder(std::unique_ptr<WritableFile> writable_file,
                               std::shared_ptr<::parquet::WriterProperties> properties,
                               std::shared_ptr<::parquet::schema::GroupNode> schema,
                               const std::vector<ExprContext*>& output_expr_ctxs, int64_t row_group_max_size) {
    _writer = std::make_unique<starrocks::parquet::SyncFileWriter>(std::move(writable_file), std::move(properties),
                                                                   std::move(schema), output_expr_ctxs);
    _writer->set_max_row_group_size(row_group_max_size);
    _writer->init();
}

std::shared_ptr<::parquet::WriterProperties> ParquetBuilder::get_properties(const ParquetBuilderOptions& options) {
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_0);
    options.use_dict ? builder.enable_dictionary() : builder.disable_dictionary();
    starrocks::parquet::ParquetBuildHelper::build_compression_type(builder, options.compression_type);
    return builder.build();
}

std::shared_ptr<::parquet::schema::GroupNode> ParquetBuilder::get_schema(
        const std::vector<std::string>& file_column_names, const std::vector<ExprContext*>& output_expr_ctxs) {
    ::parquet::schema::NodeVector fields;
    for (int i = 0; i < output_expr_ctxs.size(); i++) {
        ::parquet::Repetition::type parquet_repetition_type;
        ::parquet::Type::type parquet_data_type;
        auto column_expr = output_expr_ctxs[i]->root();
        starrocks::parquet::ParquetBuildHelper::build_file_data_type(parquet_data_type, column_expr->type().type);
        starrocks::parquet::ParquetBuildHelper::build_parquet_repetition_type(parquet_repetition_type,
                                                                              column_expr->is_nullable());
        ::parquet::schema::NodePtr nodePtr = ::parquet::schema::PrimitiveNode::Make(
                file_column_names[i], parquet_repetition_type, parquet_data_type);
        fields.push_back(nodePtr);
    }

    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, fields));
}

Status ParquetBuilder::add_chunk(Chunk* chunk) {
    return _writer->write(chunk);
}

Status ParquetBuilder::finish() {
    return _writer->close();
}

std::size_t ParquetBuilder::file_size() {
    return _writer->file_size();
}

} // namespace starrocks