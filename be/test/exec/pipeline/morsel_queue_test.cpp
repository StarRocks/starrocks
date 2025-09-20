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

#include <utility>

#include "exec/pipeline/scan/morsel.h"
#include "fs/fs_memory.h"
#include "gtest/gtest.h"
#include "storage/chunk_helper.h"
#include "storage/range.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/unique_rowset_id_generator.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

class PhysicalSplitMorselQueueV2Test : public ::testing::Test {
public:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(std::string(TEST_DIR)).ok());

        _fname = std::string(TEST_DIR) + "/test_flat_json_compact1.data";
    }

    void TearDown() override {}

protected:
    Morsels create_mock_morsels(int count) {
        Morsels morsels;
        for (int i = 0; i < count; ++i) {
            TScanRange scan_range;
            scan_range.__isset.internal_scan_range = true;
            scan_range.internal_scan_range.tablet_id = i + 1;
            scan_range.internal_scan_range.version = "1";
            scan_range.internal_scan_range.partition_id = i + 1;

            auto morsel = std::make_unique<ScanMorsel>(_plan_node_id, scan_range);
            morsels.push_back(std::move(morsel));
        }
        return morsels;
    }

    // Create real tablet objects with minimal required fields
    TabletSharedPtr create_real_tablet(int64_t tablet_id) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(::starrocks::KeysType::DUP_KEYS);
        schema_pb.add_column()->set_unique_id(1);

        auto schema = TabletSchema::create(schema_pb);
        auto meta = TabletMeta::create();
        meta->set_tablet_id(tablet_id);
        meta->set_tablet_schema(schema);
        return Tablet::create_tablet_from_meta(meta);
    }

    class MockRowset : public Rowset {
    public:
        MockRowset(RowsetId rowset_id, Version version, const TabletSchemaCSPtr& tablet_schema,
                   RowsetMetaSharedPtr meta)
                : Rowset(tablet_schema, "", std::move(meta)) {}

        static RowsetSharedPtr create(RowsetId rowset_id, Version version, int num_segments,
                                      const TabletSchemaCSPtr& tablet_schema) {
            RowsetMetaPB meta;
            meta.set_rowset_id(rowset_id.to_string());
            meta.set_start_version(version.first);
            meta.set_end_version(version.second);
            meta.set_num_segments(num_segments);
            auto rowset =
                    std::make_shared<MockRowset>(rowset_id, version, tablet_schema, std::make_shared<RowsetMeta>(meta));
            return rowset;
        }

        Status load() override { return {}; }

    private:
    };

    SegmentSharedPtr create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname,
                                          uint64_t segment_id) {
        auto res = std::make_shared<Segment>(fs, FileInfo{fname}, segment_id, _dummy_segment_schema, nullptr);
        res->set_num_rows(_segment_rows);
        return res;
    }

    std::vector<BaseRowsetSharedPtr> create_real_rowsets(const TabletSharedPtr& tablet, int num_rowsets) {
        std::vector<BaseRowsetSharedPtr> rowsets;

        for (int i = 0; i < num_rowsets; ++i) {
            RowsetId rowset_id = _rowset_id_generator.next_id();
            Version version(i + 1, i + 1);

            auto rowset = MockRowset::create(rowset_id, version, _num_segments_per_rowset, tablet->tablet_schema());
            auto st = tablet->add_rowset(rowset, false);
            CHECK(st.ok()) << st.to_string();

            for (int i = 0; i < _num_segments_per_rowset; i++) {
                auto segment = create_dummy_segment(_fs, _fname, i);
                rowset->segments().push_back(segment);
            }
            rowsets.push_back(rowset);
        }

        return rowsets;
    }

    static constexpr int32_t _plan_node_id = 1;
    static constexpr int64_t _degree_of_parallelism = 4;
    static constexpr int64_t _splitted_scan_rows = 1000;
    static constexpr int _segment_rows = 102400;
    static constexpr size_t _num_tablets = 3;
    static constexpr size_t _num_rowsets_per_tablet = 4;
    static constexpr size_t _num_segments_per_rowset = 5;

    std::vector<TabletSharedPtr> _tablets;

    static constexpr const char* TEST_DIR = "/morsel_queue_test";
    std::shared_ptr<FileSystem> _fs;
    std::string _fname;
    std::shared_ptr<TabletSchema> _dummy_segment_schema;
    UniqueRowsetIdGenerator _rowset_id_generator{UniqueId(0, 1)};
};

// Integration test: Test the complete workflow of PhysicalSplitMorselQueueV2
TEST_F(PhysicalSplitMorselQueueV2Test, IntegratedWorkflowTest) {
    // 1. Create queue
    auto morsels = create_mock_morsels(3);
    PhysicalSplitMorselQueueV2 queue(std::move(morsels), _degree_of_parallelism, _splitted_scan_rows);

    // Verify initial state
    EXPECT_EQ(queue.max_degree_of_parallelism(), _degree_of_parallelism);
    EXPECT_EQ(queue.type(), MorselQueue::PHYSICAL_SPLIT);
    EXPECT_EQ(queue.name(), "physical_split_morsel_queue_v2");

    // 2. Create real tablets and rowsets
    std::vector<BaseTabletSharedPtr> tablets;
    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets;
    for (int i = 0; i < _num_tablets; ++i) {
        auto tablet = create_real_tablet(i + 1);
        tablets.push_back(tablet);

        auto rowsets = create_real_rowsets(tablet, _num_rowsets_per_tablet);
        tablet_rowsets.push_back(rowsets);
    }
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    EXPECT_FALSE(queue.empty());

    // Test1: get all morsels without refinement
    std::set<uint64_t> segments;
    std::vector<std::tuple<RowsetId, uint64_t, bool>> rowid_ranges;
    for (int i = 0; i < _num_tablets * _num_rowsets_per_tablet * _num_segments_per_rowset; i++) {
        auto result = queue.try_get();
        EXPECT_TRUE(result.ok());
        EXPECT_TRUE(result.value() != nullptr);
        MorselPtr morsel = std::move(result.value());
        PhysicalSplitScanMorsel* physical_morsel = dynamic_cast<PhysicalSplitScanMorsel*>(morsel.get());
        EXPECT_TRUE(physical_morsel != nullptr);
        RowidRangeOptionPtr range = physical_morsel->get_rowid_range_option();
        auto [range_rowset, range_segment, range_split] = range->get_single_segment().value();

        rowid_ranges.emplace_back(range_rowset, range_segment, range_split.is_first_split_of_segment);

        segments.emplace(range_segment);
        EXPECT_TRUE(range_split.is_first_split_of_segment);
        EXPECT_EQ(range_split.row_id_range->span_size(), _splitted_scan_rows);
    }
    for (uint64_t i = 0; i < _num_segments_per_rowset; i++) {
        EXPECT_TRUE(segments.contains(i));
    }

    // Test2: refine the morsel
    for (int i = 0; i < rowid_ranges.size(); i++) {
        SparseRange<> sum_range;
        sum_range.add(Range<>{0, _segment_rows});

        // Filter the first half of rows in a segment
        auto rowid_range = std::make_shared<RowidRangeOption>();
        auto [rowset, segment, split] = rowid_ranges[i];
        size_t refined_rows = _segment_rows / 2;
        auto range = std::make_shared<SparseRange<>>(0, refined_rows);
        rowid_range->add(rowset, segment, range, split);
        queue.refine_scan_ranges(rowid_range);
        sum_range -= *range;

        // after refinement, we should be able to get the refined morsel
        auto refined_segment = queue.try_get();
        ASSERT_TRUE(refined_segment.ok());
        ASSERT_TRUE(refined_segment.value() != nullptr);
        PhysicalSplitScanMorsel* refined_physical_morsel =
                dynamic_cast<PhysicalSplitScanMorsel*>(refined_segment.value().get());
        ASSERT_TRUE(refined_physical_morsel != nullptr);
        RowidRangeOptionPtr refined_range = refined_physical_morsel->get_rowid_range_option();
        auto [rowset1, segment1, refined_split] = refined_range->get_single_segment().value();
        ASSERT_FALSE(refined_split.is_first_split_of_segment);
        ASSERT_EQ(refined_split.row_id_range->span_size(), _splitted_scan_rows);
        ASSERT_EQ(refined_rows, refined_split.row_id_range->begin());

        int64_t remain_rows = _segment_rows - _splitted_scan_rows - refined_rows;
        for (int j = 0; j < remain_rows / _splitted_scan_rows; j++) {
            auto refined_segment2 = queue.try_get();
            ASSERT_TRUE(refined_segment2.ok());
            ASSERT_TRUE(refined_segment2.value() != nullptr);
            PhysicalSplitScanMorsel* refined_physical_morsel =
                    dynamic_cast<PhysicalSplitScanMorsel*>(refined_segment2.value().get());
            ASSERT_TRUE(refined_physical_morsel != nullptr);
            RowidRangeOptionPtr refined_range = refined_physical_morsel->get_rowid_range_option();
            auto [rowset2, segment2, refined_split] = refined_range->get_single_segment().value();
            ASSERT_EQ(rowset1, rowset2);
            ASSERT_EQ(segment1, segment2);
            ASSERT_FALSE(refined_split.is_first_split_of_segment);
            ASSERT_GE(refined_split.row_id_range->span_size(), _splitted_scan_rows);
            ASSERT_EQ(refined_split.row_id_range->begin(), refined_rows + (j + 1) * _splitted_scan_rows);
            sum_range |= *refined_split.row_id_range;
        }

        ASSERT_EQ(_segment_rows - refined_rows, sum_range.span_size());
    }

    while (!queue.empty()) {
        auto x = queue.try_get();
        EXPECT_OK(x.status());
    }
}

} // namespace starrocks::pipeline
