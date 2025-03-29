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

#include "arrow_flight_batch_reader.h"

#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"

namespace starrocks {

ArrowFlightBatchReader::ArrowFlightBatchReader(const TUniqueId& query_id) : _query_id(std::move(query_id)) {
    _schema = ExecEnv::GetInstance()->result_mgr()->get_arrow_schema(query_id);
}

arrow::Status ArrowFlightBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    if (!_schema) {
        return arrow::Status::IOError("Failed to fetch schema for query ID ", print_id(_query_id));
    }

    *out = nullptr;
    auto status = ExecEnv::GetInstance()->result_mgr()->fetch_arrow_data(_query_id, out);
    if (!status.ok()) {
        return arrow::Status::IOError("Failed to fetch arrow data for query ID ", print_id(_query_id), ": ",
                                      status.to_string());
    }

    return arrow::Status::OK();
}

std::shared_ptr<arrow::Schema> ArrowFlightBatchReader::schema() const {
    return _schema;
}

} // namespace starrocks