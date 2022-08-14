// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/compaction_context.h"

#include <sstream>

#include "storage/rowset/rowset.h"

namespace starrocks {

std::string CompactionContext::to_string() const {
    std::stringstream ss;
    ss << "tablet id:" << tablet->tablet_id() << "\n";
    ss << "compaction type:" << chosen_compaction_type << "\n";
    ss << "cumulative score:" << cumulative_score << "\n";
    ss << "base score:" << base_score << "\n";
    ss << "cumulative rowset candidates:";
    for (auto& rowset : rowset_levels[0]) {
        ss << rowset->version() << ";";
    }
    ss << "\n";
    ss << "base rowset candidates:";
    for (auto& rowset : rowset_levels[1]) {
        ss << rowset->version() << ";";
    }
    ss << "\n";
    ss << "base rowset:";
    for (auto& rowset : rowset_levels[2]) {
        ss << rowset->version() << ";";
    }
    ss << "\n";
    return ss.str();
}

} // namespace starrocks
