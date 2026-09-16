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

#include <functional>
#include <string>

#include "common/status.h"
#include "formats/file_commit_result.h"

namespace starrocks::connector {

struct CommitResult {
    formats::FileCommitResult file_result;
    std::string partition_null_fingerprint;
    std::string referenced_data_file;

    CommitResult& set_partition_null_fingerprint(std::string fingerprint);
    CommitResult& set_referenced_data_file(std::string referenced_file);
};

using CommitFunc = std::function<void(const CommitResult& result)>;
using ErrorHandleFunc = std::function<void(const Status& status)>;

} // namespace starrocks::connector
