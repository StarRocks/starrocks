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

#include "exec/write_combined_txn_log.h"

#include "runtime/exec_env.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::stream_load {

Status write_combined_txn_log(const CombinedTxnLogPB& logs) {
    auto tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    return tablet_mgr->put_combined_txn_log(logs);
}

} // namespace starrocks::stream_load