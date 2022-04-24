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

#include <cstdint>
#include <map>
#include <string>

#include "common/status.h"
#include "exec/file_builder.h"

namespace starrocks {

class ExprContext;
class ParquetOutputStream;

// a wrapper of parquet output stream
class ParquetBuilder : public FileBuilder {
public:
    ParquetBuilder(std::unique_ptr<WritableFile> writable_file, const std::vector<ExprContext*>& output_expr_ctxs);
    ~ParquetBuilder() override;

    Status add_chunk(vectorized::Chunk* chunk) override;

    std::size_t file_size() override { return 0; }

    Status finish() override;

private:
    ParquetOutputStream* _outstream;
    std::unique_ptr<WritableFile> _writable_file;
    const std::vector<ExprContext*>& _output_expr_ctxs;
};

} // namespace starrocks
