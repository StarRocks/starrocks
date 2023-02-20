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

#include "exec/spill/spilled_stream.h"

#include <glog/logging.h>

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <utility>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/sort_exec_exprs.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/common.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller_path_provider.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "runtime/sorted_chunks_merger.h"
#include "util/blocking_queue.hpp"

namespace starrocks {
// TODO: implements SpilledFileStream

StatusOr<InputStreamWithTasks> SpilledFileGroup::as_flat_stream(std::weak_ptr<SpillerFactory> factory) {
    return Status::NotSupported("TODO: implements");
}

StatusOr<InputStreamWithTasks> SpilledFileGroup::as_sorted_stream(std::weak_ptr<SpillerFactory> factory,
                                                                  RuntimeState* state, const SortExecExprs* sort_exprs,
                                                                  const SortDescs* descs) {
    return Status::NotSupported("TODO: implements");
}

} // namespace starrocks