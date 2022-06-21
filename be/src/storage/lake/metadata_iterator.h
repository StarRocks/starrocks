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
    explicit MetadataIterator(TabletManager* manager, std::string group, std::vector<std::string> vec)
            : _manager(manager), _group(std::move(group)), _vec(std::move(vec)), _pos(0){};

    bool has_next() { return _pos < _vec.size(); }

    StatusOr<T> next() {
        if (_pos < _vec.size()) {
            return get_metadata_from_tablet_manager(_group, _vec[_pos++]);
        } else {
            return Status::RuntimeError(fmt::format("Out of range pos {} size {}", _pos, _vec.size()));
        }
    }

private:
    StatusOr<T> get_metadata_from_tablet_manager(const std::string& group, const std::string& path);

    TabletManager* _manager;
    std::string _group;
    std::vector<std::string> _vec;
    int _pos;
};
} // namespace starrocks::lake
