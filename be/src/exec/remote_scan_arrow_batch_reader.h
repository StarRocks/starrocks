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

#include <string>

#include "arrow/record_batch.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class RemoteScanTokenMgr;
class RemoteArrowQueueMgr;

class RemoteScanArrowBatchReader final : public arrow::RecordBatchReader {
public:
    RemoteScanArrowBatchReader(RemoteArrowQueueMgr* arrow_queue_mgr, RemoteScanTokenMgr* token_mgr,
                               const TUniqueId& fragment_instance_id, std::string token);

    arrow::Status init();

    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override;
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

private:
    void cleanup();

    RemoteArrowQueueMgr* _arrow_queue_mgr = nullptr;
    RemoteScanTokenMgr* _token_mgr = nullptr;
    const TUniqueId _fragment_instance_id;
    const std::string _token;
    std::shared_ptr<arrow::Schema> _schema;
};

} // namespace starrocks
