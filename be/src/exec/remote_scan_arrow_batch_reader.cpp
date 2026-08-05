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

#include "exec/remote_scan_arrow_batch_reader.h"

#include <utility>

#include "base/uid_util.h"
#include "runtime/remote_arrow_queue_mgr.h"
#include "runtime/remote_scan_token_mgr.h"

namespace starrocks {

RemoteScanArrowBatchReader::RemoteScanArrowBatchReader(RemoteArrowQueueMgr* arrow_queue_mgr,
                                                       RemoteScanTokenMgr* token_mgr,
                                                       const TUniqueId& fragment_instance_id, std::string token)
        : _arrow_queue_mgr(arrow_queue_mgr),
          _token_mgr(token_mgr),
          _fragment_instance_id(fragment_instance_id),
          _token(std::move(token)) {}

arrow::Status RemoteScanArrowBatchReader::init() {
    _schema = _arrow_queue_mgr->get_arrow_schema(_fragment_instance_id);
    if (_schema == nullptr) {
        return arrow::Status::ExecutionError("Failed to fetch remote scan schema for fragment instance ID: ",
                                             print_id(_fragment_instance_id));
    }
    return arrow::Status::OK();
}

std::shared_ptr<arrow::Schema> RemoteScanArrowBatchReader::schema() const {
    return _schema;
}

void RemoteScanArrowBatchReader::cleanup() {
    WARN_IF_ERROR(_arrow_queue_mgr->cancel(_fragment_instance_id), "Failed to cancel remote scan arrow queue");
    WARN_IF_ERROR(_token_mgr->remove(_token), "Failed to remove remote scan token");
}

arrow::Status RemoteScanArrowBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    if (!_schema) {
        return arrow::Status::IOError("Failed to fetch schema for fragment instance ID: ",
                                      print_id(_fragment_instance_id));
    }

    bool eos = false;
    auto status = _arrow_queue_mgr->fetch_result(_fragment_instance_id, out, &eos);
    if (!status.ok()) {
        cleanup();
        return arrow::Status::IOError("Failed to fetch remote scan arrow data for fragment instance ID: ",
                                      print_id(_fragment_instance_id), ", error: ", status.to_string());
    }
    if (eos) {
        *out = nullptr;
        cleanup();
    }
    return arrow::Status::OK();
}

} // namespace starrocks
