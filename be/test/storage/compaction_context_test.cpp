// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/compaction_context.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>

#include "storage/rowset/rowset.h"
#include "storage/tablet_schema_helper.h"

namespace starrocks {

// NOLINTNEXTLINE
TEST(CompactionContextTest, test_rowset_comparator) {
    std::set<Rowset*, RowsetComparator> sorted_rowsets_set;
    RowsetId id;
    id.init(2, 3, 0, 0);

    std::vector<RowsetSharedPtr> rowsets;
    auto tablet_schema = TabletSchemaHelper::create_tablet_schema();

    auto base_rowset_meta_pb = std::make_unique<RowsetMetaPB>();
    base_rowset_meta_pb->set_rowset_id(id.to_string());
    base_rowset_meta_pb->set_start_version(0);
    base_rowset_meta_pb->set_end_version(9);
    RowsetMetaSharedPtr base_rowset_meta = std::make_shared<RowsetMeta>(base_rowset_meta_pb);
    RowsetSharedPtr base_rowset = std::make_shared<Rowset>(tablet_schema.get(), "./rowset_0", base_rowset_meta);
    rowsets.emplace_back(std::move(base_rowset));

    for (int i = 1; i <= 10; i++) {
        auto rowset_meta_pb = std::make_unique<RowsetMetaPB>();
        rowset_meta_pb->set_rowset_id(id.to_string());
        rowset_meta_pb->set_start_version(i * 10);
        rowset_meta_pb->set_end_version((i + 1) * 10 - 1);
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>(rowset_meta_pb);
        RowsetSharedPtr rowset =
                std::make_shared<Rowset>(tablet_schema.get(), "./rowset" + std::to_string(i), rowset_meta);
        rowsets.emplace_back(std::move(rowset));
    }

    for (int i = 110; i < 120; i++) {
        auto rowset_meta_pb = std::make_unique<RowsetMetaPB>();
        rowset_meta_pb->set_rowset_id(id.to_string());
        rowset_meta_pb->set_start_version(i);
        rowset_meta_pb->set_end_version(i);
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>(rowset_meta_pb);
        RowsetSharedPtr rowset =
                std::make_shared<Rowset>(tablet_schema.get(), "./rowset" + std::to_string(i), rowset_meta);
        rowsets.emplace_back(std::move(rowset));
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(rowsets.begin(), rowsets.end(), g);

    for (auto& rowset : rowsets) {
        sorted_rowsets_set.insert(rowset.get());
    }

    bool is_sorted = true;
    Rowset* pre_rowset = nullptr;
    for (auto& rowset : sorted_rowsets_set) {
        if (pre_rowset != nullptr) {
            if (!(rowset->start_version() == pre_rowset->end_version() + 1 &&
                  rowset->start_version() <= rowset->end_version())) {
                is_sorted = false;
                break;
            }
        }
        pre_rowset = rowset;
    }
    ASSERT_EQ(true, is_sorted);
}

} // namespace starrocks
