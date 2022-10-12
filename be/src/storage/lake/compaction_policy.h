// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "common/statusor.h"

namespace starrocks::lake {

class Rowset;
using RowsetPtr = std::shared_ptr<Rowset>;
class Tablet;
using TabletPtr = std::shared_ptr<Tablet>;
class CompactionPolicy;
using CompactionPolicyPtr = std::shared_ptr<CompactionPolicy>;

// Compaction policy for lake tablet
class CompactionPolicy {
public:
    virtual ~CompactionPolicy() = default;
    virtual StatusOr<std::vector<RowsetPtr>> pick_rowsets(int64_t version) = 0;

    static StatusOr<CompactionPolicyPtr> create_compaction_policy(TabletPtr tablet);
};

} // namespace starrocks::lake
