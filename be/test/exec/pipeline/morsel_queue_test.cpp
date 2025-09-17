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
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
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
        // Initialize test data
        _plan_node_id = 1;
        _degree_of_parallelism = 4;
        _splitted_scan_rows = 1000;

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

    SegmentSharedPtr create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname) {
        auto res = std::make_shared<Segment>(fs, FileInfo{fname}, 1, _dummy_segment_schema, nullptr);
        res->set_num_rows(_segment_rows);
        return res;
    }

    std::vector<BaseRowsetSharedPtr> create_real_rowsets(const TabletSharedPtr& tablet, int num_rowsets) {
        std::vector<BaseRowsetSharedPtr> rowsets;

        for (int i = 0; i < num_rowsets; ++i) {
            const int num_segments = 3;
            RowsetId rowset_id;
            rowset_id.init(10000 + i);
            Version version(i + 1, i + 1);

            auto rowset = MockRowset::create(rowset_id, version, num_segments, tablet->tablet_schema());
            auto st = tablet->add_rowset(rowset, false);
            CHECK(st.ok()) << st.to_string();

            for (int i = 0; i < num_segments; i++) {
                auto segment = create_dummy_segment(_fs, _fname);
                rowset->segments().push_back(segment);
            }

            rowsets.push_back(rowset);
        }

        return rowsets;
    }

    int32_t _plan_node_id;
    int64_t _degree_of_parallelism;
    int64_t _splitted_scan_rows;
    std::vector<TabletSharedPtr> _tablets;

    static constexpr const char* TEST_DIR = "/morsel_queue_test";
    std::shared_ptr<FileSystem> _fs;
    std::string _fname;
    std::shared_ptr<TabletSchema> _dummy_segment_schema;
    static const int _segment_rows = 102400;
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

    for (int i = 0; i < 3; ++i) {
        auto tablet = create_real_tablet(i + 1);
        tablets.push_back(tablet);

        auto rowsets = create_real_rowsets(tablet, 2);
        tablet_rowsets.push_back(rowsets);
    }

    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);

    // 3. Set key ranges using TabletReaderParams
    // std::vector<OlapTuple> range_start_key;
    // std::vector<OlapTuple> range_end_key;
    // queue.set_key_ranges(TabletReaderParams::RangeStartOperation::GT, TabletReaderParams::RangeEndOperation::LT,
    //                      range_start_key, range_end_key);

    // 4. Test queue operations
    EXPECT_FALSE(queue.empty());
    auto result = queue.try_get();
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(result.value() != nullptr);
    MorselPtr morsel = std::move(result.value());
    PhysicalSplitScanMorsel* physical_morsel = dynamic_cast<PhysicalSplitScanMorsel*>(morsel.get());
    EXPECT_TRUE(physical_morsel != nullptr);
    RowidRangeOptionPtr range = physical_morsel->get_rowid_range_option();
    auto [range_rowset, range_segment, range_split] = range->get_single_segment().value();

    // 6. Test refine_scan_ranges method
    auto rowid_range = std::make_shared<RowidRangeOption>();
    rowid_range->add(range_rowset, range_segment, std::make_shared<SparseRange<>>(0, 1000), true);
    queue.refine_scan_ranges(rowid_range);

    // 7. Test multiple try_get calls
    for (int i = 0; i < 5; ++i) {
        auto morsel_result = queue.try_get();
        EXPECT_TRUE(morsel_result.ok());
        EXPECT_TRUE(morsel_result.value() != nullptr);
    }
}

} // namespace starrocks::pipeline
