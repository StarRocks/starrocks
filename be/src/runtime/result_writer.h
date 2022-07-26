// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/result_writer.h

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

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class Status;
class RuntimeState;

// abstract class of the result writer
class ResultWriter {
public:
    ResultWriter() = default;
    virtual ~ResultWriter() = default;

    virtual Status init(RuntimeState* state) = 0;

    virtual Status open(RuntimeState* state) { return Status::OK(); };

    // convert one chunk to mysql result and
    // append this chunk to the result sink
    virtual Status append_chunk(vectorized::Chunk* chunk) = 0;

    virtual Status close() = 0;

    int64_t get_written_rows() const { return _written_rows; }

protected:
    int64_t _written_rows = 0; // number of rows written
};

} // namespace starrocks
