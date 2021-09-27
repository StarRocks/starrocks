// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/parquet_writer.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <cstdint>
#include <map>
#include <string>

#include "common/status.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class ExprContext;
class FileWriter;
class RowBatch;

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

class ParquetOutputStream : public arrow::io::OutputStream {
public:
    ParquetOutputStream(FileWriter* file_writer);
    ~ParquetOutputStream() override;

    arrow::Status Write(const void* data, int64_t nbytes) override;
    // return the current write position of the stream
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;

    bool closed() const override { return _is_closed; }

private:
    FileWriter* _file_writer; // not owned
    int64_t _cur_pos = 0;     // current write position
    bool _is_closed = false;
};

// a wrapper of parquet output stream
class ParquetWriterWrapper {
public:
    ParquetWriterWrapper(FileWriter* file_writer, const std::vector<ExprContext*>& output_expr_ctxs);
    virtual ~ParquetWriterWrapper();

    Status write(const RowBatch& row_batch);

    void close();

private:
    ParquetOutputStream* _outstream;
    const std::vector<ExprContext*>& _output_expr_ctxs;
};

} // namespace starrocks
