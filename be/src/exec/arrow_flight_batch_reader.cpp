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
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/http_service.h

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

#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"

#include "arrow_flight_batch_reader.h"

namespace starrocks {

ArrowFlightBatchReader::ArrowFlightBatchReader(const TUniqueId& query_id)
: _query_id(std::move(query_id)) {
    _schema = ExecEnv::GetInstance()->result_mgr()->get_arrow_schema(query_id);
}

arrow::Status ArrowFlightBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    *out = nullptr;
    auto status = ExecEnv::GetInstance()->result_mgr()->fetch_arrow_data(_query_id, out);
    return arrow::Status::OK();
}

std::shared_ptr<arrow::Schema> ArrowFlightBatchReader::schema() const {
    return _schema;
}

}