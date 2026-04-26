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

#include "exec/spill/dir_manager.h"

namespace starrocks::spill {

StatusOr<DirPtr> DirManager::acquire_writable_dir(const AcquireDirOptions& opts) {
    // for the case of multiple dirs, we randomly select one as the start
    // and then try one by one until we find the first one that meets the capacity requirements.
    size_t start_idx = 0;
    if (_dirs.size() > 1) {
        std::lock_guard l(_mutex);
        start_idx = _rand.Next() % _dirs.size();
    }
    for (size_t i = 0; i < _dirs.size(); i++) {
        size_t idx = (start_idx + i) % _dirs.size();
        if (_dirs[idx]->inc_size(opts.data_size)) {
            return _dirs[idx];
        }
    }
    return Status::CapacityLimitExceed("no writable spill storage directories");
}

} // namespace starrocks::spill
