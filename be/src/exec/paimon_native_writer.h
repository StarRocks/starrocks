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

#include <memory>
#include <vector>

#include "arrow/type.h"
#include "exec/paimon_writer.h"
#include "util/runtime_profile.h"

struct ArrowSchema;

namespace paimon {
class RecordBatch;
class WriteContext;
class FileStoreWrite;
} // namespace paimon

namespace starrocks {

class Status;

template <typename T>
class StatusOr;

class Chunk;
using ChunkPtr = std::shared_ptr<Chunk>;

class PaimonTableDescriptor;
class ExprContext;
class RuntimeState;

struct TypeDescriptor;

class PaimonNativeWriter : public PaimonWriter {
public:
    PaimonNativeWriter(PaimonTableDescriptor* paimon_table, std::vector<ExprContext*> partition_expr,
                       std::vector<ExprContext*> bucket_expr, std::vector<ExprContext*> output_expr,
                       std::vector<std::string> data_column_names, std::vector<std::string> data_column_types,
                       RuntimeProfile::Counter* _convert_timer, bool is_static_partition_sink,
                       std::vector<std::string> partition_column_names,
                       std::vector<std::string> partition_column_values);
    ~PaimonNativeWriter() override;

    Status do_init(RuntimeState* runtime_state) override;
    Status write(RuntimeState* runtime_state, const ChunkPtr& chunk) override;
    Status commit(RuntimeState* runtime_state) override;
    void close(RuntimeState* runtime_state) noexcept override;

    void set_output_expr(std::vector<ExprContext*> output_expr) override;
    std::string get_commit_message() override;

private:
    StatusOr<std::unique_ptr<paimon::RecordBatch>> convert_chunk_to_record_batch(const ChunkPtr& chunk);
    StatusOr<std::map<std::string, std::string>> extract_partition_values(const ChunkPtr& chunk);
    Status get_arrow_schema();

    Status create_file_store_write(std::unique_ptr<paimon::WriteContext> context);

    StatusOr<std::vector<int32_t>> calculate_bucket_ids(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                                        int32_t bucket_num);

    std::string commit_message;

    PaimonTableDescriptor* _paimon_table;
    std::vector<ExprContext*> _output_expr;
    std::vector<ExprContext*> _partition_expr;
    std::vector<ExprContext*> _bucket_expr;
    std::vector<std::string> _data_column_names;
    std::vector<std::string> _data_column_types;

    RuntimeProfile::Counter* _convert_timer;

    bool _is_static_partition_sink = false;
    std::vector<std::string> _partition_column_names;
    std::vector<std::string> _partition_column_values;

    std::shared_ptr<arrow::Schema> _schema;

    std::unique_ptr<paimon::FileStoreWrite> _file_store_write;
};

} // namespace starrocks
