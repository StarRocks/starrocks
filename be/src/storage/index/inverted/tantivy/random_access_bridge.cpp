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

#include "storage/index/inverted/tantivy/random_access_bridge.h"

#include "fs/fs.h"

extern "C" int sr_random_access_read(void* handle, uint64_t offset, uint8_t* buf, size_t len) {
    if (handle == nullptr || buf == nullptr) {
        return -1;
    }
    auto* file = static_cast<starrocks::RandomAccessFile*>(handle);
    auto st = file->read_at_fully(static_cast<int64_t>(offset), buf, static_cast<int64_t>(len));
    return st.ok() ? 0 : -1;
}
