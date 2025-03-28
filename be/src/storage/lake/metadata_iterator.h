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

#include <fmt/format.h>

#include <set>

#include "common/status.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

class TabletManager;

template <typename T>
class MetadataIterator {
public:
    explicit MetadataIterator(TabletManager* manager, int64_t tablet_id, std::set<std::string> files)
            : _manager(manager), _tablet_id(tablet_id), _files(std::move(files)), _iter(_files.begin()){};

    bool has_next() const { return _iter != _files.end(); }

    StatusOr<T> next() {
        if (has_next()) {
            return get_metadata_from_tablet_manager(*_iter++);
        } else {
            return Status::RuntimeError("no more element");
        }
    }

private:
    StatusOr<T> get_metadata_from_tablet_manager(const std::string& path);

    TabletManager* _manager;
    int64_t _tablet_id;
    // Use sorted set
    std::set<std::string> _files;
    std::set<std::string>::iterator _iter;
    int64_t _max_gtid = 0;
};

} // namespace starrocks::lake
