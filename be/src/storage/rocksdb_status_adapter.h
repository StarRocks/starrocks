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

#include <rocksdb/status.h>

namespace starrocks {

static inline Status to_status(const rocksdb::Status& from) {
    switch (from.code()) {
    case rocksdb::Status::kOk:
        return Status::OK();
    case rocksdb::Status::kNotFound:
        return Status::NotFound(from.ToString());
    case rocksdb::Status::kCorruption:
        return Status::Corruption(from.ToString());
    case rocksdb::Status::kNotSupported:
        return Status::NotSupported(from.ToString());
    case rocksdb::Status::kInvalidArgument:
        return Status::InvalidArgument(from.ToString());
    case rocksdb::Status::kIOError:
        return Status::IOError(from.ToString());
    default:
        return Status::InternalError(from.ToString());
    }
}

} // namespace starrocks
