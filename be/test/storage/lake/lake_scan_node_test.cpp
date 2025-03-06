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

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "connector/lake_connector.h"
#include "exec/connector_scan_node.h"
#include "storage/chunk_helper.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeScanNodeTest : public TestBase {
public:
    LakeScanNodeTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    void create_rowsets_for_testing(TabletMetadata* tablet_metadata, int64_t version) {
        std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22}; // 23 rows
        std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

        std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41}; // 12 rows
        std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        auto c3 = Int32Column::create();
        c0->append_numbers(k0.data(), k0.size() * sizeof(int));
        c1->append_numbers(v0.data(), v0.size() * sizeof(int));
        c2->append_numbers(k1.data(), k1.size() * sizeof(int));
        c3->append_numbers(v1.data(), v1.size() * sizeof(int));

        Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
        Chunk chunk1({std::move(c2), std::move(c3)}, _schema);

        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_metadata->id()));

        {
            int64_t txn_id = next_id();
            // write rowset 1 with 2 segments
            ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
            ASSERT_OK(writer->open());

            // write rowset data
            // segment #1
            ASSERT_OK(writer->write(chunk0));
            ASSERT_OK(writer->write(chunk1));
            ASSERT_OK(writer->finish());

            // segment #2
            ASSERT_OK(writer->write(chunk0));
            ASSERT_OK(writer->write(chunk1));
            ASSERT_OK(writer->finish());

            auto files = writer->files();
            ASSERT_EQ(2, files.size());

            // add rowset metadata
            auto* rowset = tablet_metadata->add_rowsets();
            rowset->set_overlapped(true);
            rowset->set_id(1);
            rowset->set_num_rows(k0.size() + k1.size());
            auto* segs = rowset->mutable_segments();
            for (auto& file : writer->files()) {
                segs->Add(std::move(file.path));
            }

            writer->close();
        }

        // write tablet metadata
        tablet_metadata->set_version(version);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*tablet_metadata));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_scan_node";

    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeScanNodeTest, test_could_split) {
    create_rowsets_for_testing(_tablet_metadata.get(), 2);

    std::shared_ptr<RuntimeState> runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);
    auto tnode = create_tplan_node_cloud();
    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(runtime_state->obj_pool(), *tnode, *descs);
    ASSERT_OK(scan_node->init(*tnode, runtime_state.get()));

    bool enable_tablet_internal_parallel = true;
    auto tablet_internal_parallel_mode = TTabletInternalParallelMode::type::AUTO;
    std::map<int32_t, std::vector<TScanRangeParams>> no_scan_ranges_per_driver_seq;

    auto data_source_provider = dynamic_cast<connector::LakeDataSourceProvider*>(scan_node->data_source_provider());
    data_source_provider->set_lake_tablet_manager(_tablet_mgr.get());

    config::tablet_internal_parallel_max_splitted_scan_bytes = 32;
    config::tablet_internal_parallel_min_splitted_scan_rows = 4;
    // dop is 1
    int pipeline_dop = 1;
    auto tablet_metas = std::vector<TabletMetadata*>();
    tablet_metas.emplace_back(_tablet_metadata.get());
    auto scan_ranges = create_scan_ranges_cloud(tablet_metas);
    ASSIGN_OR_ABORT(auto morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_FALSE(data_source_provider->could_split());
    ASSERT_FALSE(data_source_provider->could_split_physically());

    // dop is 100
    pipeline_dop = 100;
    config::tablet_internal_parallel_min_scan_dop = 10;
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_FALSE(data_source_provider->could_split());
    ASSERT_FALSE(data_source_provider->could_split_physically());

    // dop is 2
    pipeline_dop = 2;
    config::tablet_internal_parallel_min_scan_dop = 4;
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(data_source_provider->could_split());
    ASSERT_TRUE(data_source_provider->could_split_physically());
}

// test issue https://github.com/StarRocks/starrocks/pull/44386
TEST_F(LakeScanNodeTest, test_issue_44386) {
    auto new_tablet_metadata = std::make_unique<TabletMetadata>(*_tablet_metadata);
    new_tablet_metadata->set_id(next_id());

    CHECK_OK(_tablet_mgr->put_tablet_metadata(*new_tablet_metadata));
    create_rowsets_for_testing(new_tablet_metadata.get(), 3);

    std::shared_ptr<RuntimeState> runtime_state = create_runtime_state();
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    auto* descs = create_table_desc(runtime_state.get(), types);
    auto tnode = create_tplan_node_cloud();
    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(runtime_state->obj_pool(), *tnode, *descs);
    ASSERT_OK(scan_node->init(*tnode, runtime_state.get()));

    bool enable_tablet_internal_parallel = true;
    auto tablet_internal_parallel_mode = TTabletInternalParallelMode::type::AUTO;
    std::map<int32_t, std::vector<TScanRangeParams>> no_scan_ranges_per_driver_seq;

    auto data_source_provider = dynamic_cast<connector::LakeDataSourceProvider*>(scan_node->data_source_provider());
    data_source_provider->set_lake_tablet_manager(_tablet_mgr.get());

    config::tablet_internal_parallel_max_splitted_scan_bytes = 32;
    config::tablet_internal_parallel_min_splitted_scan_rows = 4;

    int pipeline_dop = 3;
    config::tablet_internal_parallel_min_scan_dop = 4;
    auto tablet_metas = std::vector<TabletMetadata*>{new_tablet_metadata.get(), _tablet_metadata.get()};
    auto scan_ranges = create_scan_ranges_cloud(tablet_metas);
    config::tablet_internal_parallel_min_scan_dop = 4;
    ASSIGN_OR_ABORT(auto morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop, false,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(data_source_provider->could_split());
    ASSERT_TRUE(data_source_provider->could_split_physically());
}

} // namespace starrocks::lake
