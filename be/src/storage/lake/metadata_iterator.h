// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <fmt/format.h>

#include <vector>

#include "common/status.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

class TabletManager;

template <typename T>
class MetadataIterator {
public:
    explicit MetadataIterator(TabletManager* manager, std::vector<std::string> files)
            : _manager(manager), _files(std::move(files)), _iter(_files.begin()){};

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
    std::vector<std::string> _files;
    std::vector<std::string>::iterator _iter;
};

} // namespace starrocks::lake
