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

#include "connector/common/connector_sink_commit.h"

#include <utility>

namespace starrocks::connector {

CommitResult& CommitResult::set_partition_null_fingerprint(std::string fingerprint) {
    partition_null_fingerprint = std::move(fingerprint);
    return *this;
}

CommitResult& CommitResult::set_referenced_data_file(std::string referenced_file) {
    referenced_data_file = std::move(referenced_file);
    return *this;
}

} // namespace starrocks::connector
