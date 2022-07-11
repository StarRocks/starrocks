// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
            : _manager(manager), _files(std::move(files)), _pos(0){};

    bool has_next() { return _pos < _files.size(); }

    StatusOr<T> next() {
        if (_pos < _files.size()) {
            return get_metadata_from_tablet_manager(_files[_pos++]);
        } else {
            return Status::RuntimeError(fmt::format("Out of range pos {} size {}", _pos, _files.size()));
        }
    }

private:
    StatusOr<T> get_metadata_from_tablet_manager(const std::string& path);

    TabletManager* _manager;
    std::vector<std::string> _files;
    int _pos;
};

} // namespace starrocks::lake
