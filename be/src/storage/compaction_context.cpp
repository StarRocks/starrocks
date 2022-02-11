// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/compaction_context.h"

#include <sstream>

#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks {

std::string CompactionContext::to_string() const {
    std::stringstream ss;
    ss << "tablet id:" << tablet->tablet_id() << "\n";
    ss << "current level:" << (int32_t)current_level << "\n";
    ss << "level 0:";
    for (auto& rowset : rowset_levels[0]) {
        ss << rowset->version() << ";";
    }
    ss << "\n";
    ss << "level 1:";
    for (auto& rowset : rowset_levels[1]) {
        ss << rowset->version() << ";";
    }
    ss << "\n";
    ss << "level 2:";
    for (auto& rowset : rowset_levels[2]) {
        ss << rowset->version() << ";";
    }
    ss << "\n";
    return ss.str();
}

} // namespace starrocks
