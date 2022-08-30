// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/base_and_cumulative_compaction_policy.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>

#include "storage/compaction_context.h"
#include "storage/compaction_utils.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/tablet.h"
#include "storage/tablet_schema_helper.h"
#include "storage/tablet_updates.h"

namespace starrocks {

TEST(BaseAndCumulativeCompactionPolicyTest, test_need_compaction) {
    TabletSharedPtr tablet = std::make_shared<Tablet>();
    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->set_tablet_id(100);
    tablet->set_tablet_meta(tablet_meta);
    std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
    compaction_context->tablet = tablet;

    std::vector<RowsetSharedPtr> rowsets;
    auto tablet_schema = TabletSchemaHelper::create_tablet_schema();

    int64_t base_time = UnixSeconds() - 100 * 60;
    RowsetMetaSharedPtr base_rowset_meta = std::make_shared<RowsetMeta>();
    base_rowset_meta->set_start_version(0);
    base_rowset_meta->set_end_version(9);
    base_rowset_meta->set_creation_time(base_time);
    base_rowset_meta->set_segments_overlap(NONOVERLAPPING);
    base_rowset_meta->set_num_segments(1);
    base_rowset_meta->set_total_disk_size(100 * 1024 * 1024);
    base_rowset_meta->set_empty(false);
    RowsetSharedPtr base_rowset = std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset_0", base_rowset_meta);
    compaction_context->rowset_levels[2].insert(base_rowset.get());
    rowsets.emplace_back(std::move(base_rowset));
    for (int i = 1; i <= 10; i++) {
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_start_version(i * 10);
        rowset_meta->set_end_version((i + 1) * 10 - 1);
        rowset_meta->set_creation_time(base_time + i);
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
        rowset_meta->set_num_segments(1);
        rowset_meta->set_total_disk_size(1024 * 1024);
        RowsetSharedPtr rowset =
                std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset" + std::to_string(i), rowset_meta);
        compaction_context->rowset_levels[1].insert(rowset.get());
        rowsets.emplace_back(std::move(rowset));
    }

    for (int i = 110; i < 120; i++) {
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_start_version(i);
        rowset_meta->set_end_version(i);
        rowset_meta->set_creation_time(base_time + i);
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
        rowset_meta->set_num_segments(1);
        rowset_meta->set_total_disk_size(1024 * 1024);
        RowsetSharedPtr rowset =
                std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset" + std::to_string(i), rowset_meta);
        compaction_context->rowset_levels[0].insert(rowset.get());
        rowsets.emplace_back(std::move(rowset));
    }
    BaseAndCumulativeCompactionPolicy policy(compaction_context.get());
    ASSERT_TRUE(policy.need_compaction());
}

TEST(BaseAndCumulativeCompactionPolicyTest, test_create_cumulative_compaction_with_recent_rowsets) {
    TabletSharedPtr tablet = std::make_shared<Tablet>();
    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->set_tablet_id(100);
    tablet->set_tablet_meta(tablet_meta);
    std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
    compaction_context->tablet = tablet;

    std::vector<RowsetSharedPtr> rowsets;
    auto tablet_schema = TabletSchemaHelper::create_tablet_schema();

    int64_t base_time = UnixSeconds() - 100 * 60;
    RowsetMetaSharedPtr base_rowset_meta = std::make_shared<RowsetMeta>();
    base_rowset_meta->set_start_version(0);
    base_rowset_meta->set_end_version(9);
    base_rowset_meta->set_creation_time(base_time);
    base_rowset_meta->set_segments_overlap(NONOVERLAPPING);
    base_rowset_meta->set_num_segments(1);
    base_rowset_meta->set_total_disk_size(100 * 1024 * 1024);
    base_rowset_meta->set_empty(false);
    RowsetSharedPtr base_rowset = std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset_0", base_rowset_meta);
    compaction_context->rowset_levels[2].insert(base_rowset.get());
    rowsets.emplace_back(std::move(base_rowset));
    for (int i = 10; i <= 14; i++) {
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_start_version(i);
        rowset_meta->set_end_version(i);
        if (i == 14) {
            rowset_meta->set_creation_time(UnixSeconds());
        } else {
            rowset_meta->set_creation_time(base_time + i);
        }
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
        rowset_meta->set_num_segments(1);
        rowset_meta->set_total_disk_size(1024 * 1024);
        RowsetSharedPtr rowset =
                std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset" + std::to_string(i), rowset_meta);
        compaction_context->rowset_levels[0].insert(rowset.get());
        rowsets.emplace_back(std::move(rowset));
    }
    compaction_context->chosen_compaction_type = CUMULATIVE_COMPACTION;
    BaseAndCumulativeCompactionPolicy policy(compaction_context.get());
    std::shared_ptr<CompactionTask> cumulative_task = policy.create_compaction();
    ASSERT_EQ(cumulative_task, nullptr);
}

TEST(BaseAndCumulativeCompactionPolicyTest, test_create_cumulative_compaction_with_missed_versions) {
    TabletSharedPtr tablet = std::make_shared<Tablet>();
    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->set_tablet_id(100);
    tablet->set_tablet_meta(tablet_meta);
    std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
    compaction_context->tablet = tablet;

    std::vector<RowsetSharedPtr> rowsets;
    auto tablet_schema = TabletSchemaHelper::create_tablet_schema();

    int64_t base_time = UnixSeconds() - 100 * 60;
    RowsetMetaSharedPtr base_rowset_meta = std::make_shared<RowsetMeta>();
    base_rowset_meta->set_start_version(0);
    base_rowset_meta->set_end_version(9);
    base_rowset_meta->set_creation_time(base_time);
    base_rowset_meta->set_segments_overlap(NONOVERLAPPING);
    base_rowset_meta->set_num_segments(1);
    base_rowset_meta->set_total_disk_size(100 * 1024 * 1024);
    base_rowset_meta->set_empty(false);
    RowsetSharedPtr base_rowset = std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset_0", base_rowset_meta);
    compaction_context->rowset_levels[2].insert(base_rowset.get());
    rowsets.emplace_back(std::move(base_rowset));
    for (int i = 10; i <= 20; i++) {
        if (i == 14) {
            // version 14 is missed
            continue;
        }
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_start_version(i);
        rowset_meta->set_end_version(i);
        rowset_meta->set_creation_time(base_time + i);
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
        rowset_meta->set_num_segments(1);
        rowset_meta->set_total_disk_size(1024 * 1024);
        RowsetSharedPtr rowset =
                std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset" + std::to_string(i), rowset_meta);
        compaction_context->rowset_levels[0].insert(rowset.get());
        rowsets.emplace_back(std::move(rowset));
    }
    compaction_context->chosen_compaction_type = CUMULATIVE_COMPACTION;
    BaseAndCumulativeCompactionPolicy policy(compaction_context.get());
    std::shared_ptr<CompactionTask> cumulative_task = policy.create_compaction();
    ASSERT_EQ(cumulative_task, nullptr);
}

TEST(BaseAndCumulativeCompactionPolicyTest, test_create_base_compaction_with_empty_base_rowset) {
    TabletSharedPtr tablet = std::make_shared<Tablet>();
    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->set_tablet_id(100);
    tablet->set_tablet_meta(tablet_meta);
    std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
    compaction_context->tablet = tablet;

    std::vector<RowsetSharedPtr> rowsets;
    auto tablet_schema = TabletSchemaHelper::create_tablet_schema();

    int64_t base_time = UnixSeconds() - 100 * 60;
    RowsetMetaSharedPtr base_rowset_meta = std::make_shared<RowsetMeta>();
    base_rowset_meta->set_start_version(0);
    base_rowset_meta->set_end_version(9);
    base_rowset_meta->set_creation_time(base_time);
    base_rowset_meta->set_total_disk_size(100 * 1024 * 1024);
    base_rowset_meta->set_empty(true);
    RowsetSharedPtr base_rowset = std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset_0", base_rowset_meta);
    compaction_context->rowset_levels[2].insert(base_rowset.get());
    rowsets.emplace_back(std::move(base_rowset));
    for (int i = 1; i <= 1; i++) {
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_start_version(i * 10);
        rowset_meta->set_end_version((i + 1) * 10 - 1);
        rowset_meta->set_creation_time(base_time + i);
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
        rowset_meta->set_num_segments(1);
        rowset_meta->set_total_disk_size(1024 * 1024);
        RowsetSharedPtr rowset =
                std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset" + std::to_string(i), rowset_meta);
        compaction_context->rowset_levels[1].insert(rowset.get());
        rowsets.emplace_back(std::move(rowset));
    }
    compaction_context->chosen_compaction_type = BASE_COMPACTION;
    BaseAndCumulativeCompactionPolicy policy(compaction_context.get());
    std::shared_ptr<CompactionTask> base_task = policy.create_compaction();
    ASSERT_EQ(base_task, nullptr);
}

TEST(BaseAndCumulativeCompactionPolicyTest, test_create_base_compaction_with_missed_versions) {
    TabletSharedPtr tablet = std::make_shared<Tablet>();
    TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->set_tablet_id(100);
    tablet->set_tablet_meta(tablet_meta);
    std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
    compaction_context->tablet = tablet;

    std::vector<RowsetSharedPtr> rowsets;
    auto tablet_schema = TabletSchemaHelper::create_tablet_schema();

    int64_t base_time = UnixSeconds() - 100 * 60;
    RowsetMetaSharedPtr base_rowset_meta = std::make_shared<RowsetMeta>();
    base_rowset_meta->set_start_version(0);
    base_rowset_meta->set_end_version(9);
    base_rowset_meta->set_creation_time(base_time);
    base_rowset_meta->set_total_disk_size(100 * 1024 * 1024);
    base_rowset_meta->set_empty(true);
    RowsetSharedPtr base_rowset = std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset_0", base_rowset_meta);
    compaction_context->rowset_levels[2].insert(base_rowset.get());
    rowsets.emplace_back(std::move(base_rowset));
    for (int i = 1; i <= 10; i++) {
        if (i == 5) {
            continue;
        }
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_start_version(i * 10);
        rowset_meta->set_end_version((i + 1) * 10 - 1);
        rowset_meta->set_creation_time(base_time + i);
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
        rowset_meta->set_num_segments(1);
        rowset_meta->set_total_disk_size(1024 * 1024);
        RowsetSharedPtr rowset =
                std::make_shared<BetaRowset>(tablet_schema.get(), "./rowset" + std::to_string(i), rowset_meta);
        compaction_context->rowset_levels[1].insert(rowset.get());
        rowsets.emplace_back(std::move(rowset));
    }
    compaction_context->chosen_compaction_type = BASE_COMPACTION;
    BaseAndCumulativeCompactionPolicy policy(compaction_context.get());
    std::shared_ptr<CompactionTask> base_task = policy.create_compaction();
    ASSERT_EQ(base_task, nullptr);
}

} // namespace starrocks
