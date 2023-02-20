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

#include "exec/spill/spiller_path_provider.h"

#include <fmt/core.h>

#include <memory>

#include "common/status.h"

namespace starrocks {
LocalPathProvider::~LocalPathProvider() noexcept {
    if (_del_path) {
        for (const auto& path : _paths) {
            WARN_IF_ERROR(_fs->delete_dir(path), "delete spilled path error:");
        }
    }
}

Status LocalPathProvider::open(RuntimeState* state) {
    for (const auto& path : _paths) {
        bool created;
        RETURN_IF_ERROR(_fs->create_dir_if_missing(path, &created));
    }
    return Status::OK();
}

// TODO: consider the disk size ?
StatusOr<SpillFilePtr> LocalPathProvider::get_file() {
    int32_t path = _next_id.fetch_add(1);
    const std::string& selected_path = _paths[path % _paths.size()];
    auto file_path = fmt::format("{}/{}-{}", selected_path, _prefix, path);
    return std::make_shared<SpillFile>(std::move(file_path), _fs);
}

} // namespace starrocks