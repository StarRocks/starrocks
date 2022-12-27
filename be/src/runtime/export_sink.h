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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/export_sink.h

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

#include <vector>

#include "exec/data_sink.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream_file.h"
#include "fs/fs.h"
#include "util/runtime_profile.h"

namespace starrocks {

class RowDescriptor;
class TExpr;
class RuntimeState;
class RuntimeProfile;
class ExprContext;
class MemTracker;
class FileWriter;
class Status;
class FileBuilder;

// This class is a sinker, which put export data to external storage by broker.
class ExportSink : public DataSink {
public:
    ExportSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs);

    ~ExportSink() override = default;

    Status init(const TDataSink& thrift_sink, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send_chunk(RuntimeState* state, Chunk* chunk) override;

    Status close(RuntimeState* state, Status exec_status) override;

    RuntimeProfile* profile() override { return _profile; }

    std::vector<TExpr> get_output_expr() const { return _t_output_expr; }

private:
    Status open_file_writer(int timeout_ms);
    Status gen_file_name(std::string* file_name);

    RuntimeState* _state;

    // owned by RuntimeState
    ObjectPool* _pool;
    const std::vector<TExpr>& _t_output_expr;

    std::vector<ExprContext*> _output_expr_ctxs;

    TExportSink _t_export_sink;

    RuntimeProfile* _profile;

    RuntimeProfile::Counter* _bytes_written_counter;
    RuntimeProfile::Counter* _rows_written_counter;
    RuntimeProfile::Counter* _write_timer;

    std::unique_ptr<FileBuilder> _file_builder;
    bool _closed = false;
};

} // end namespace starrocks
