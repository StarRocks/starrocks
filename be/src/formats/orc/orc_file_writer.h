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

#pragma once

#include <orc/Writer.hh>
#include <util/priority_thread_pool.hpp>

#include "exec/pipeline/sink/rolling_file_writer.h"
#include "orc_chunk_writer.h"

namespace starrocks {

class ORCFileWriter final : public pipeline::FileWriter {
public:
    class ORCWriterOptions : public FileWriterOptions {
        int64_t stripe_size = 1 << 27; // 128MB
    };

    static StatusOr<std::unique_ptr<orc::Type>> make_schema(const std::vector<std::string>& column_names,
                                                            const std::vector<TypeDescriptor>& type_descs);

    ORCFileWriter(std::unique_ptr<OrcOutputStream> output_stream, const std::vector<std::string>& column_names,
                  const std::vector<ExprContext*>& output_exprs, const std::shared_ptr<ORCWriterOptions>& options,
                  PriorityThreadPool* executors = nullptr);

    ~ORCFileWriter() override = default;

    Status init() override;

    int64_t get_written_bytes() override;

    std::future<Status> write(ChunkPtr chunk) override;

    std::future<CommitResult> commit() override;

private:
    static StatusOr<std::unique_ptr<orc::Type>> _make_schema_node(const TypeDescriptor& type_desc);

    StatusOr<std::unique_ptr<orc::ColumnVectorBatch>> _convert(ChunkPtr chunk);

    void _write_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type_desc);

    template <LogicalType Type, typename VectorBatchType>
    void _write_number(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_string(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_decimal(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision, int scale);

    template <LogicalType DecimalType, typename VectorBatchType, typename T>
    void _write_decimal32or64or128(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision, int scale);

    void _write_date(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_datetime(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_array_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type);

    void _write_struct_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type);

    void _write_map_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type);

    std::unique_ptr<OrcOutputStream> _output_stream;
    std::vector<std::string> _column_names;
    std::vector<ExprContext*> _output_exprs;
    std::vector<TypeDescriptor> _type_descs;
    std::shared_ptr<orc::Writer> _writer;
    std::shared_ptr<ORCWriterOptions> _writer_options;

    // If provided, submit task to executors and return future to the caller. Otherwise execute synchronously.
    PriorityThreadPool* _executors;
};

} // namespace starrocks
