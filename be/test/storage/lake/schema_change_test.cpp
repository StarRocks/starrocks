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

#include "storage/lake/schema_change.h"

#include <gtest/gtest.h>

#include <thread>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/binary_column.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_reshard.h"
#include "storage/lake/test_util.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/metadata_util.h"

namespace starrocks::lake {

using namespace starrocks;

using VSchema = starrocks::Schema;
using VChunk = starrocks::Chunk;

struct SchemaChangeParam {
    KeysType keys_type = DUP_KEYS;
    int writes_before = 2;
    int writes_after = 0;
    int concurrency = 1;
};

std::ostream& operator<<(std::ostream& os, const SchemaChangeParam& param) {
    return os << param.keys_type << "_" << param.writes_before << "_" << param.writes_after << "_" << param.concurrency;
}

std::string to_string_param_name(const testing::TestParamInfo<SchemaChangeParam>& info) {
    std::stringstream ss;
    ss << info.param;
    return ss.str();
}

class SchemaChangeTest : public testing::Test, public testing::WithParamInterface<SchemaChangeParam> {
public:
    SchemaChangeTest(const std::string& test_dir) {
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _location_provider = std::make_unique<FixedLocationProvider>(test_dir);
        _update_manager = std::make_unique<UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);
    }

protected:
    static ChunkPtr read(const VersionedTablet& tablet, bool sorted = false) {
        auto metadata = tablet.metadata();
        auto schema = tablet.get_schema();
        auto reader = std::make_shared<TabletReader>(tablet.tablet_manager(), metadata, *(schema->schema()));
        CHECK_OK(reader->prepare());
        TabletReaderParams params;
        params.sorted_by_keys_per_tablet = sorted;
        CHECK_OK(reader->open(params));

        auto chunk = ChunkHelper::new_chunk(*(schema->schema()), 10);

        while (true) {
            auto tmp = ChunkHelper::new_chunk(*(schema->schema()), 10);
            auto st = reader->get_next(tmp.get());
            if (st.ok()) {
                chunk->append(*tmp);
            } else if (st.is_end_of_file()) {
                break;
            } else {
                LOG(FATAL) << st;
            }
        }
        return chunk;
    }

    Status publish_version_for_schema_change(int64_t tablet_id, int64_t new_version, int64_t txn_id) {
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_combined_txn_log(false);
        txn_info.set_commit_time(time(nullptr));
        return publish_version(_tablet_manager.get(), lake::PublishTabletInfo(tablet_id), 1, new_version,
                               std::span<const TxnInfoPB>(&txn_info, 1), false)
                .status();
    }

    std::unique_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;

    std::shared_ptr<TabletMetadata> _base_tablet_metadata;
    std::shared_ptr<TabletSchema> _base_tablet_schema;
    std::shared_ptr<VSchema> _base_schema;

    std::shared_ptr<TabletMetadata> _new_tablet_metadata;
    std::shared_ptr<TabletSchema> _new_tablet_schema;
    std::shared_ptr<VSchema> _new_schema;

    std::shared_ptr<TabletMetadata> _new_tablet_metadata_with_generated_column;
    std::shared_ptr<TabletSchema> _new_tablet_schema_with_generated_column;
    std::shared_ptr<VSchema> _new_schema_with_generated_column;

    RuntimeProfile _dummy_runtime_profile{"dummy"};

    int64_t _partition_id = 100;
};

class SchemaChangeAddColumnTest : public SchemaChangeTest {
public:
    SchemaChangeAddColumnTest() : SchemaChangeTest(kTestGroupPath) {
        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(GetParam().keys_type);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c1->set_aggregation("NONE");
            } else {
                c1->set_aggregation("REPLACE");
            }
        }

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL | DEFAULT |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |         |
        //  |   c1   |  INT | NO  |  NO  |         |
        //  |   c2   |  INT | NO  |  NO  |  10     |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(GetParam().keys_type);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 id should be same as base tablet schema
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c1->set_aggregation("NONE");
            } else {
                c1->set_aggregation("REPLACE");
            }
        }
        {
            auto c2 = new_schema->add_column();
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
            c2->set_default_value("10");
        }

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_new_tablet_schema));

        // new tablet with generated column
        _new_tablet_metadata_with_generated_column = std::make_shared<TabletMetadata>();
        _new_tablet_metadata_with_generated_column->set_id(next_id());
        _new_tablet_metadata_with_generated_column->set_version(1);
        //
        //  | column | type | KEY | NULL | DEFAULT |   EXPR  |
        //  +--------+------+-----+------+---------+---------+
        //  |   c0   |  INT | YES |  NO  |         |         |
        //  |   c1   |  INT | NO  |  NO  |         |         |
        //  |   c2   |  INT | NO  |  NO  |         |    c0   |
        auto new_schema_with_generated_column = _new_tablet_metadata_with_generated_column->mutable_schema();
        new_schema_with_generated_column->set_id(next_id());
        new_schema_with_generated_column->set_num_short_key_columns(1);
        new_schema_with_generated_column->set_keys_type(GetParam().keys_type);
        new_schema_with_generated_column->set_num_rows_per_row_block(65535);
        // c0 c1 id should be same as base tablet schema
        {
            auto c0 = new_schema_with_generated_column->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = new_schema_with_generated_column->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c1->set_aggregation("NONE");
            } else {
                c1->set_aggregation("REPLACE");
            }
        }
        {
            auto c2 = new_schema_with_generated_column->add_column();
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(true);
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }

        _new_tablet_schema_with_generated_column = TabletSchema::create(*new_schema_with_generated_column);
        _new_schema_with_generated_column =
                std::make_shared<VSchema>(ChunkHelper::convert_schema(_new_tablet_schema_with_generated_column));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_direct_schema_change_2";

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata_with_generated_column));
    }

    void TearDown() override { ASSERT_OK(fs::remove_all(kTestGroupPath)); }
};

TEST_P(SchemaChangeAddColumnTest, test_add_column) {
    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_id = _base_tablet_metadata->id();
    for (int i = 0; i < GetParam().writes_before; i++) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_datum(Datum(i * 1));
        c1->append_datum(Datum(i * 2));

        VChunk chunk0({std::move(c0), std::move(c1)}, _base_schema);
        uint32_t indexes[1] = {0};

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_base_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes, sizeof(indexes) / sizeof(indexes[0])));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id++;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata->id();
    auto alter_version = version;
    auto alter_txn_id = txn_id;
    {
        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = alter_version;
        request.txn_id = alter_txn_id;

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
        txn_id++;
    }

    // Simulate concurrent writes with schema change
    for (int i = 0; i < GetParam().writes_after; i++) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        c0->append_datum(Datum(i * 1));
        c1->append_datum(Datum(i * 4));
        c2->append_datum(Datum(i * 8));

        VChunk chunk1({std::move(c0), std::move(c1), std::move(c2)}, _new_schema);
        uint32_t indexes[1] = {0};

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(new_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_new_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes, sizeof(indexes) / sizeof(indexes[0])));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_log_version(_tablet_manager.get(), new_tablet_id, txn_id, version + 1));
        version++;
        txn_id++;
    }

    // publish schema change
    int concurrency = GetParam().concurrency;
    if (concurrency == 1) {
        ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    } else {
        std::vector<std::thread> threads;
        for (int i = 0; i < concurrency; i++) {
            threads.emplace_back(
                    [&]() { (void)publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id); });
        }
        for (auto& t : threads) {
            t.join();
        }
    }
    version++;
    txn_id++;

    // check new tablet data
    ASSIGN_OR_ABORT(auto new_tablet, _tablet_manager->get_tablet(new_tablet_id, version));

    auto chunk = read(new_tablet, /*sorted=*/GetParam().keys_type != DUP_KEYS);

    if (GetParam().keys_type == DUP_KEYS) {
        int expect_num_rows = GetParam().writes_before + GetParam().writes_after;
        ASSERT_EQ(expect_num_rows, chunk->num_rows());
        for (int i = 0, sz = chunk->num_rows(); i < sz; i++) {
            if (i < GetParam().writes_before) {
                EXPECT_EQ(i * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(i * 2, chunk->get(i)[1].get_int32());
                EXPECT_EQ(/*default=*/10, chunk->get(i)[2].get_int32());
            } else {
                int k = i - GetParam().writes_before;
                EXPECT_EQ(k * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(k * 4, chunk->get(i)[1].get_int32());
                EXPECT_EQ(k * 8, chunk->get(i)[2].get_int32());
            }
        }
    } else {
        int expect_num_rows = std::max(GetParam().writes_before, GetParam().writes_after);
        ASSERT_EQ(expect_num_rows, chunk->num_rows());
        for (int i = 0, sz = chunk->num_rows(); i < sz; i++) {
            if (i >= GetParam().writes_after) {
                EXPECT_EQ(i * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(i * 2, chunk->get(i)[1].get_int32());
                EXPECT_EQ(/*default=*/10, chunk->get(i)[2].get_int32());
            } else {
                EXPECT_EQ(i * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(i * 4, chunk->get(i)[1].get_int32());
                EXPECT_EQ(i * 8, chunk->get(i)[2].get_int32());
            }
        }
        chunk->reset();
    }
}

TEST_P(SchemaChangeAddColumnTest, test_add_generated_column) {
    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_id = _base_tablet_metadata->id();
    for (int i = 0; i < GetParam().writes_before; i++) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_datum(Datum(i * 1));
        c1->append_datum(Datum(i * 2));

        VChunk chunk0({std::move(c0), std::move(c1)}, _base_schema);
        uint32_t indexes[1] = {0};

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_base_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes, sizeof(indexes) / sizeof(indexes[0])));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id++;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata_with_generated_column->id();
    auto alter_version = version;
    auto alter_txn_id = txn_id;
    {
        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = alter_version;
        request.txn_id = alter_txn_id;

        TAlterTabletMaterializedColumnReq mc_request;

        std::vector<TExprNode> nodes;
        TExprNode node;
        node.node_type = TExprNodeType::SLOT_REF;
        node.type = gen_type_desc(TPrimitiveType::INT);
        node.num_children = 0;
        TSlotRef t_slot_ref = TSlotRef();
        t_slot_ref.slot_id = 0;
        t_slot_ref.tuple_id = 0;
        node.__set_slot_ref(t_slot_ref);
        node.is_nullable = true;
        nodes.emplace_back(node);

        TExpr t_expr;
        t_expr.nodes = nodes;

        std::map<int32_t, starrocks::TExpr> m_expr;
        m_expr.insert({2, t_expr});

        mc_request.__set_query_globals(TQueryGlobals());
        mc_request.__set_query_options(TQueryOptions());
        mc_request.__set_mc_exprs(m_expr);

        request.__set_materialized_column_req(mc_request);

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
        txn_id++;
    }

    // Simulate concurrent writes with schema change
    for (int i = 0; i < GetParam().writes_after; i++) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        c0->append_datum(Datum(i * 1));
        c1->append_datum(Datum(i * 4));
        c2->append_datum(Datum(i * 5 /* c0 + c1 */));

        VChunk chunk1({std::move(c0), std::move(c1), std::move(c2)}, _new_schema_with_generated_column);
        uint32_t indexes[1] = {0};

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(new_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_new_tablet_schema_with_generated_column->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes, sizeof(indexes) / sizeof(indexes[0])));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_log_version(_tablet_manager.get(), new_tablet_id, txn_id, version + 1));
        version++;
        txn_id++;
    }

    // publish schema change
    int concurrency = GetParam().concurrency;
    if (concurrency == 1) {
        ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    } else {
        std::vector<std::thread> threads;
        for (int i = 0; i < concurrency; i++) {
            threads.emplace_back(
                    [&]() { (void)publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id); });
        }
        for (auto& t : threads) {
            t.join();
        }
    }
    version++;
    txn_id++;

    // check new tablet data
    ASSIGN_OR_ABORT(auto new_tablet, _tablet_manager->get_tablet(new_tablet_id, version));

    auto chunk = read(new_tablet, /*sorted=*/GetParam().keys_type != DUP_KEYS);

    if (GetParam().keys_type == DUP_KEYS) {
        int expect_num_rows = GetParam().writes_before + GetParam().writes_after;
        ASSERT_EQ(expect_num_rows, chunk->num_rows());
        for (int i = 0, sz = chunk->num_rows(); i < sz; i++) {
            if (i < GetParam().writes_before) {
                EXPECT_EQ(i * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(i * 2, chunk->get(i)[1].get_int32());
                EXPECT_EQ(i * 1, chunk->get(i)[2].get_int32());
            } else {
                int k = i - GetParam().writes_before;
                EXPECT_EQ(k * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(k * 4, chunk->get(i)[1].get_int32());
                EXPECT_EQ(k * 5, chunk->get(i)[2].get_int32());
            }
        }
    } else {
        int expect_num_rows = std::max(GetParam().writes_before, GetParam().writes_after);
        ASSERT_EQ(expect_num_rows, chunk->num_rows());
        for (int i = 0, sz = chunk->num_rows(); i < sz; i++) {
            if (i >= GetParam().writes_after) {
                EXPECT_EQ(i * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(i * 2, chunk->get(i)[1].get_int32());
                EXPECT_EQ(i * 1, chunk->get(i)[2].get_int32());
            } else {
                EXPECT_EQ(i * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(i * 4, chunk->get(i)[1].get_int32());
                EXPECT_EQ(i * 5, chunk->get(i)[2].get_int32());
            }
        }
        chunk->reset();
    }
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(SchemaChangeAddColumnTest, SchemaChangeAddColumnTest,
                         ::testing::Values(SchemaChangeParam{DUP_KEYS, 0, 0},
                                           SchemaChangeParam{DUP_KEYS, 0, 2},
                                           SchemaChangeParam{DUP_KEYS, 2, 0},
                                           SchemaChangeParam{DUP_KEYS, 2, 2},
                                           SchemaChangeParam{AGG_KEYS, 0, 0},
                                           SchemaChangeParam{AGG_KEYS, 0, 2},
                                           SchemaChangeParam{AGG_KEYS, 2, 0},
                                           SchemaChangeParam{AGG_KEYS, 2, 1},
                                           SchemaChangeParam{AGG_KEYS, 2, 2},
                                           SchemaChangeParam{UNIQUE_KEYS, 0, 0},
                                           SchemaChangeParam{UNIQUE_KEYS, 0, 2},
                                           SchemaChangeParam{UNIQUE_KEYS, 2, 0},
                                           SchemaChangeParam{UNIQUE_KEYS, 2, 1},
                                           SchemaChangeParam{UNIQUE_KEYS, 2, 2},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 0, 3},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 2, 3},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 0, 3},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 1, 3},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 2, 3},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 4, 3}),
                         to_string_param_name);
// clang-format on

class SchemaChangeModifyColumnTypeTest : public SchemaChangeTest {
public:
    SchemaChangeModifyColumnTypeTest() : SchemaChangeTest(kTestGroupPath) {
        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(GetParam().keys_type);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c1->set_aggregation("NONE");
            } else {
                c1->set_aggregation("REPLACE");
            }
        }

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |BIGINT| NO  |  NO  |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(GetParam().keys_type);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 id should be same as base tablet schema
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("BIGINT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c1->set_aggregation("NONE");
            } else {
                c1->set_aggregation("REPLACE");
            }
        }

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_new_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_direct_schema_change";

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }
};

TEST_P(SchemaChangeModifyColumnTypeTest, test_alter_column_type) {
    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_id = _base_tablet_metadata->id();
    for (int i = 0; i < GetParam().writes_before; i++) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_datum(Datum(i * 1));
        c1->append_datum(Datum(i * 2));

        VChunk chunk0({std::move(c0), std::move(c1)}, _base_schema);
        uint32_t indexes[1] = {0};

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_base_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes, sizeof(indexes) / sizeof(indexes[0])));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id++;
    }

    auto new_tablet_id = _new_tablet_metadata->id();

    int64_t alter_txn_id = txn_id++;
    // do schema change
    {
        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = version;
        request.txn_id = alter_txn_id;

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
    }

    std::vector<int64_t> v1{3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36};

    // concurrent writes with schema change
    for (int i = 0; i < GetParam().writes_after; i++) {
        auto c0 = Int32Column::create();
        auto c1 = Int64Column::create();
        c0->append_datum(Datum((int32_t)(i * 1)));
        c1->append_datum(Datum((int64_t)(i * 4)));

        VChunk chunk1({std::move(c0), std::move(c1)}, _new_schema);
        uint32_t indexes[1] = {0};

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(new_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_new_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes, sizeof(indexes) / sizeof(indexes[0])));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_log_version(_tablet_manager.get(), new_tablet_id, txn_id, version + 1));
        version++;
        txn_id++;
    }

    int concurrency = GetParam().concurrency;
    if (concurrency == 1) {
        ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    } else {
        std::vector<std::thread> threads;
        for (int i = 0; i < concurrency; i++) {
            threads.emplace_back(
                    [&]() { (void)publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id); });
        }
        for (auto& t : threads) {
            t.join();
        }
    }

    version++;
    txn_id++;

    // check new tablet data
    ASSIGN_OR_ABORT(auto new_tablet, _tablet_manager->get_tablet(new_tablet_id, version));
    auto chunk = read(new_tablet, /*sorted=*/GetParam().keys_type != DUP_KEYS);

    if (GetParam().keys_type == DUP_KEYS) {
        int expect_num_rows = GetParam().writes_before + GetParam().writes_after;
        ASSERT_EQ(expect_num_rows, chunk->num_rows());
        for (int i = 0; i < expect_num_rows; i++) {
            if (i < GetParam().writes_before) {
                int k = i;
                EXPECT_EQ(k * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(k * 2, chunk->get(i)[1].get_int64());
            } else {
                int k = i - GetParam().writes_before;
                EXPECT_EQ(k * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(k * 4, chunk->get(i)[1].get_int64());
            }
        }
    } else {
        int expect_num_rows = std::max(GetParam().writes_before, GetParam().writes_after);
        ASSERT_EQ(expect_num_rows, chunk->num_rows());
        for (int i = 0; i < expect_num_rows; i++) {
            int k = i;
            if (i >= GetParam().writes_after) {
                EXPECT_EQ(k * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(k * 2, chunk->get(i)[1].get_int64());
            } else {
                EXPECT_EQ(k * 1, chunk->get(i)[0].get_int32());
                EXPECT_EQ(k * 4, chunk->get(i)[1].get_int64());
            }
        }
    }
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(SchemaChangeModifyColumnTypeTest, SchemaChangeModifyColumnTypeTest,
                         ::testing::Values(SchemaChangeParam{DUP_KEYS, 0, 0},
                                           SchemaChangeParam{DUP_KEYS, 0, 2},
                                           SchemaChangeParam{DUP_KEYS, 2, 0},
                                           SchemaChangeParam{DUP_KEYS, 2, 2},
                                           SchemaChangeParam{AGG_KEYS, 0, 0},
                                           SchemaChangeParam{AGG_KEYS, 0, 2},
                                           SchemaChangeParam{AGG_KEYS, 2, 0},
                                           SchemaChangeParam{AGG_KEYS, 2, 2},
                                           SchemaChangeParam{UNIQUE_KEYS, 0, 0},
                                           SchemaChangeParam{UNIQUE_KEYS, 0, 2},
                                           SchemaChangeParam{UNIQUE_KEYS, 2, 0},
                                           SchemaChangeParam{UNIQUE_KEYS, 2, 2},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 0},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 2, 3},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 0, 3},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 1, 3},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 2, 3},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 4, 3}),
                         to_string_param_name);
// clang-format on

class SchemaChangeModifyColumnOrderTest : public SchemaChangeTest {
public:
    SchemaChangeModifyColumnOrderTest() : SchemaChangeTest(kTestGroupPath) {
        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | YES |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(GetParam().keys_type);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        auto c2_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = base_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c1   |  INT | YES |  NO  |
        //  |   c0   |  INT | YES |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(GetParam().keys_type);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 c2 id should be same as base tablet schema
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c2 = new_schema->add_column();
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_default_value("10");
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_new_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_sorted_schema_change";

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }
};

TEST_P(SchemaChangeModifyColumnOrderTest, test_alter_key_order) {
    std::vector<int> k0{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4};
    std::vector<int> k1{1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24};

    auto ck0 = Int32Column::create();
    auto ck1 = Int32Column::create();
    auto cv0 = Int32Column::create();
    ck0->append_numbers(k0.data(), k0.size() * sizeof(int));
    ck1->append_numbers(k1.data(), k1.size() * sizeof(int));
    cv0->append_numbers(v0.data(), v0.size() * sizeof(int));

    VChunk chunk0({ck0, ck1, cv0}, _base_schema);

    auto indexes = std::vector<uint32_t>(k0.size());
    for (int i = 0; i < k0.size(); i++) {
        indexes[i] = i;
    }

    // write 2 rowsets
    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_id = _base_tablet_metadata->id();
    for (int i = 0; i < GetParam().writes_before; i++) {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_base_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id++;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata->id();
    auto alter_txn_id = txn_id++;
    {
        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = version;
        request.txn_id = alter_txn_id;

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
    }

    for (int i = 0; i < GetParam().writes_after; i++) {
        VChunk chunk1({ck1, ck0, cv0}, _new_schema);

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(new_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_new_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_log_version(_tablet_manager.get(), new_tablet_id, txn_id, version + 1));
        version++;
        txn_id++;
    }

    ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    version++;
    txn_id++;

    // check new tablet data
    std::vector<int> rc1_0{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
    std::vector<int> rc0_0{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
    std::vector<int> rc2_0{2, 8, 14, 20, 4, 10, 16, 22, 6, 12, 18, 24};

    ASSIGN_OR_ABORT(auto new_metadata, _tablet_manager->get_tablet_metadata(new_tablet_id, version));
    auto reader = std::make_shared<TabletReader>(_tablet_manager.get(), new_metadata, *_new_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*_new_schema, 1024);

    if (GetParam().keys_type == DUP_KEYS) {
        for (int n = 0; n < GetParam().writes_before + GetParam().writes_after; n++) {
            CHECK_OK(reader->get_next(chunk.get()));
            for (int i = 0, sz = k0.size(); i < sz; i++) {
                EXPECT_EQ(rc1_0[i], chunk->get(i)[0].get_int32());
                EXPECT_EQ(rc0_0[i], chunk->get(i)[1].get_int32());
                EXPECT_EQ(rc2_0[i], chunk->get(i)[2].get_int32());
            }
            chunk->reset();
        }
    } else if (GetParam().writes_before + GetParam().writes_after > 0) {
        CHECK_OK(reader->get_next(chunk.get()));
        for (int i = 0, sz = k0.size(); i < sz; i++) {
            EXPECT_EQ(rc1_0[i], chunk->get(i)[0].get_int32());
            EXPECT_EQ(rc0_0[i], chunk->get(i)[1].get_int32());
            EXPECT_EQ(rc2_0[i], chunk->get(i)[2].get_int32());
        }
        chunk->reset();
    }

    auto st = reader->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(SchemaChangeModifyColumnOrderTest, SchemaChangeModifyColumnOrderTest,
                         ::testing::Values(SchemaChangeParam{DUP_KEYS, 0, 0},
                                           SchemaChangeParam{DUP_KEYS, 0, 2},
                                           SchemaChangeParam{DUP_KEYS, 2, 0},
                                           SchemaChangeParam{DUP_KEYS, 2, 2},
                                           SchemaChangeParam{AGG_KEYS, 0, 0},
                                           SchemaChangeParam{AGG_KEYS, 0, 2},
                                           SchemaChangeParam{AGG_KEYS, 2, 0},
                                           SchemaChangeParam{AGG_KEYS, 2, 2},
                                           SchemaChangeParam{UNIQUE_KEYS, 0, 0},
                                           SchemaChangeParam{UNIQUE_KEYS, 0, 2},
                                           SchemaChangeParam{UNIQUE_KEYS, 2, 0},
                                           SchemaChangeParam{UNIQUE_KEYS, 2, 2},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 0},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 1},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 2},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 0},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 1},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 2},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 4}),
                         to_string_param_name);
// clang-format on

class SchemaChangeModifyColumnMultiSegmentOrderTest : public SchemaChangeTest {
public:
    SchemaChangeModifyColumnMultiSegmentOrderTest() : SchemaChangeTest(kTestGroupPath) {
        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | YES |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(GetParam().keys_type);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        auto c2_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = base_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | YES |  NO  |
        //  |   c2   |  BIGINT | NO  |  NO  |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(GetParam().keys_type);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 c2 id should be same as base tablet schema
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = new_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("BIGINT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_default_value("10");
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_new_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_multi_segment_sorted_schema_change";

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }
};

TEST_P(SchemaChangeModifyColumnMultiSegmentOrderTest, test_alter_table) {
    std::vector<int> k0{1, 3, 5};
    std::vector<int> k1{1, 3, 5};
    std::vector<int> v0{1, 3, 5};

    auto ck0 = Int32Column::create();
    auto ck1 = Int32Column::create();
    auto cv0 = Int32Column::create();
    ck0->append_numbers(k0.data(), k0.size() * sizeof(int));
    ck1->append_numbers(k1.data(), k1.size() * sizeof(int));
    cv0->append_numbers(v0.data(), v0.size() * sizeof(int));

    auto indexes = std::vector<uint32_t>(k0.size());
    for (int i = 0; i < k0.size(); i++) {
        indexes[i] = i;
    }

    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_id = _base_tablet_metadata->id();

    std::vector<int> k0_2{2, 4, 6};
    std::vector<int> k1_2{2, 4, 6};
    std::vector<int> v0_2{2, 4, 6};

    auto ck0_2 = Int32Column::create();
    auto ck1_2 = Int32Column::create();
    auto cv0_2 = Int32Column::create();
    ck0_2->append_numbers(k0_2.data(), k0_2.size() * sizeof(int));
    ck1_2->append_numbers(k1_2.data(), k1_2.size() * sizeof(int));
    cv0_2->append_numbers(v0_2.data(), v0_2.size() * sizeof(int));

    {
        // mutli segments in on rowset
        const int64_t old_size = config::write_buffer_size;
        config::write_buffer_size = 1;
        VChunk chunk0({std::move(ck0), std::move(ck1), std::move(cv0)}, _base_schema);
        VChunk chunk1({std::move(ck0_2), std::move(ck1_2), std::move(cv0_2)}, _base_schema);

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_base_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id++;
        config::write_buffer_size = old_size;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata->id();
    auto alter_txn_id = txn_id;
    {
        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = version;
        request.txn_id = alter_txn_id;

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
    }

    ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    version++;
    txn_id++;

    // check new tablet data
    std::vector<int> rc0{1, 2, 3, 4, 5, 6};
    std::vector<int> rc1{1, 2, 3, 4, 5, 6};
    std::vector<int> rc2{1, 2, 3, 4, 5, 6};

    ASSIGN_OR_ABORT(auto new_metadata, _tablet_manager->get_tablet_metadata(new_tablet_id, version));
    auto reader = std::make_shared<TabletReader>(_tablet_manager.get(), new_metadata, *_new_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*_new_schema, 1024);

    CHECK_OK(reader->get_next(chunk.get()));
    for (int i = 0; i < rc0.size(); i++) {
        EXPECT_EQ(rc0[i], chunk->get(i)[0].get_int32());
        EXPECT_EQ(rc1[i], chunk->get(i)[1].get_int32());
        EXPECT_EQ(rc2[i], chunk->get(i)[2].get_int64());
    }
    chunk->reset();

    auto st = reader->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(SchemaChangeModifyColumnMultiSegmentOrderTest, SchemaChangeModifyColumnMultiSegmentOrderTest,
                         ::testing::Values(SchemaChangeParam{PRIMARY_KEYS, 0, 0}, SchemaChangeParam{DUP_KEYS, 0, 0}),
                         to_string_param_name);
// clang-format on

class SchemaChangeSortKeyReorderTest1 : public SchemaChangeTest {
public:
    SchemaChangeSortKeyReorderTest1() : SchemaChangeTest(kTestGroupPath) {
        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL | SORTKEY |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |   NO    |
        //  |   c1   |  INT | NO  |  NO  |   YES   |
        //  |   c2   |  INT | NO  |  NO  |   YES   |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(GetParam().keys_type);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        auto c2_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = base_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }
        base_schema->add_sort_key_idxes(1);
        base_schema->add_sort_key_idxes(2);

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL | SORTKEY |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |   NO    |
        //  |   c1   |  INT | NO  |  NO  |   NO    |
        //  |   c2   |  INT | NO  |  NO  |   YES   |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(GetParam().keys_type);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 c2 id should be same as base tablet schema
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = new_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_default_value("10");
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }
        new_schema->add_sort_key_idxes(2);

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_new_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_sortkey_reorder_1";

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }
};

TEST_P(SchemaChangeSortKeyReorderTest1, test_alter_sortkey_reorder_1) {
    std::vector<int> k0{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4};
    std::vector<int> k1{1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24};

    auto ck0 = Int32Column::create();
    auto ck1 = Int32Column::create();
    auto cv0 = Int32Column::create();
    ck0->append_numbers(k0.data(), k0.size() * sizeof(int));
    ck1->append_numbers(k1.data(), k1.size() * sizeof(int));
    cv0->append_numbers(v0.data(), v0.size() * sizeof(int));

    VChunk chunk0({ck0, ck1, cv0}, _base_schema);

    auto indexes = std::vector<uint32_t>(k0.size());
    for (int i = 0; i < k0.size(); i++) {
        indexes[i] = i;
    }

    int64_t version = 1;
    int64_t txn_id = next_id();
    auto base_tablet_id = _base_tablet_metadata->id();
    for (int i = 0; i < GetParam().writes_before; i++) {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_base_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id++;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata->id();
    auto alter_txn_id = txn_id++;
    {
        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = version;
        request.txn_id = alter_txn_id;

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
    }

    for (int i = 0; i < GetParam().writes_after; i++) {
        VChunk chunk1({ck0, ck1, cv0}, _new_schema);

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(new_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_new_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_log_version(_tablet_manager.get(), new_tablet_id, txn_id, version + 1));
        version++;
        txn_id++;
    }

    ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    version++;
    txn_id++;

    // check new tablet data
    std::vector<int> rc0_0{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4};
    std::vector<int> rc1_0{1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
    std::vector<int> rc2_0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24};

    ASSIGN_OR_ABORT(auto new_metadata, _tablet_manager->get_tablet_metadata(new_tablet_id, version));
    auto reader = std::make_shared<TabletReader>(_tablet_manager.get(), new_metadata, *_new_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*_new_schema, 1024);

    if (GetParam().writes_before + GetParam().writes_after > 0) {
        CHECK_OK(reader->get_next(chunk.get()));
        for (int i = 0, sz = k0.size(); i < sz; i++) {
            EXPECT_EQ(rc0_0[i], chunk->get(i)[0].get_int32());
            EXPECT_EQ(rc1_0[i], chunk->get(i)[1].get_int32());
            EXPECT_EQ(rc2_0[i], chunk->get(i)[2].get_int32());
        }
        chunk->reset();
    }

    auto st = reader->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(SchemaChangeSortKeyReorderTest1, SchemaChangeSortKeyReorderTest1,
                         ::testing::Values(SchemaChangeParam{PRIMARY_KEYS, 0, 1},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 2},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 1},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 4}),
                         to_string_param_name);
// clang-format on

class SchemaChangeSortKeyReorderTest2 : public SchemaChangeTest {
public:
    SchemaChangeSortKeyReorderTest2() : SchemaChangeTest(kTestGroupPath) {
        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL | SORTKEY |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |   NO    |
        //  |   c1   |  INT | NO  |  NO  |   YES   |
        //  |   c2   |  INT | NO  |  NO  |   YES   |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(GetParam().keys_type);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        auto c2_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = base_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }
        base_schema->add_sort_key_idxes(1);
        base_schema->add_sort_key_idxes(2);

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL | SORTKEY |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |   YES   |
        //  |   c1   |  INT | NO  |  NO  |   YES   |
        //  |   c2   |  INT | NO  |  NO  |   YES   |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(GetParam().keys_type);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 c2 id should be same as base tablet schema
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c1_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c0_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = new_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_default_value("10");
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }
        new_schema->add_sort_key_idxes(0);
        new_schema->add_sort_key_idxes(1);
        new_schema->add_sort_key_idxes(2);

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_new_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_sortkey_reorder_2";

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }
};

TEST_P(SchemaChangeSortKeyReorderTest2, test_alter_sortkey_reorder2) {
    std::vector<int> k0{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4};
    std::vector<int> k1{1, 2, 3, 3, 2, 1, 1, 2, 3, 1, 2, 3};
    std::vector<int> v0{2, 4, 6, 12, 10, 8, 14, 16, 18, 20, 22, 24};

    auto ck0 = Int32Column::create();
    auto ck1 = Int32Column::create();
    auto cv0 = Int32Column::create();
    ck0->append_numbers(k0.data(), k0.size() * sizeof(int));
    ck1->append_numbers(k1.data(), k1.size() * sizeof(int));
    cv0->append_numbers(v0.data(), v0.size() * sizeof(int));

    VChunk chunk0({ck0, ck1, cv0}, _base_schema);

    auto indexes = std::vector<uint32_t>(k0.size());
    for (int i = 0; i < k0.size(); i++) {
        indexes[i] = i;
    }

    int64_t version = 1;
    int64_t txn_id = next_id();
    auto base_tablet_id = _base_tablet_metadata->id();
    for (int i = 0; i < GetParam().writes_before; i++) {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_base_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id++;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata->id();
    auto alter_txn_id = txn_id++;
    {
        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = version;
        request.txn_id = alter_txn_id;

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
    }

    for (int i = 0; i < GetParam().writes_after; i++) {
        VChunk chunk1({ck0, ck1, cv0}, _new_schema);

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(new_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_new_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_log_version(_tablet_manager.get(), new_tablet_id, txn_id, version + 1));
        version++;
        txn_id++;
    }

    ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    version++;
    txn_id++;

    // check new tablet data
    std::vector<int> rc0_0{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4};
    std::vector<int> rc1_0{1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
    std::vector<int> rc2_0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24};

    ASSIGN_OR_ABORT(auto new_metadata, _tablet_manager->get_tablet_metadata(new_tablet_id, version));
    auto reader = std::make_shared<TabletReader>(_tablet_manager.get(), new_metadata, *_new_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*_new_schema, 1024);

    if (GetParam().writes_before + GetParam().writes_after > 0) {
        CHECK_OK(reader->get_next(chunk.get()));
        for (int i = 0, sz = k0.size(); i < sz; i++) {
            EXPECT_EQ(rc0_0[i], chunk->get(i)[0].get_int32());
            EXPECT_EQ(rc1_0[i], chunk->get(i)[1].get_int32());
            EXPECT_EQ(rc2_0[i], chunk->get(i)[2].get_int32());
        }
        chunk->reset();
    }

    auto st = reader->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(SchemaChangeSortKeyReorderTest2, SchemaChangeSortKeyReorderTest2,
                         ::testing::Values(SchemaChangeParam{PRIMARY_KEYS, 0, 1},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 2},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 1},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 4}),
                         to_string_param_name);
// clang-format on

class SchemaChangeSortKeyReorderTest3 : public SchemaChangeTest {
public:
    SchemaChangeSortKeyReorderTest3() : SchemaChangeTest(kTestGroupPath) {
        // base tablet
        _base_tablet_metadata = std::make_shared<TabletMetadata>();
        _base_tablet_metadata->set_id(next_id());
        _base_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL | SORTKEY |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |   NO    |
        //  |   c1   |  INT | NO  |  NO  |   YES   |
        //  |   c2   |  INT | NO  |  NO  |   YES   |
        auto base_schema = _base_tablet_metadata->mutable_schema();
        base_schema->set_id(next_id());
        base_schema->set_num_short_key_columns(1);
        base_schema->set_keys_type(GetParam().keys_type);
        base_schema->set_num_rows_per_row_block(65535);
        auto c0_id = next_id();
        auto c1_id = next_id();
        auto c2_id = next_id();
        {
            auto c0 = base_schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = base_schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = base_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }
        base_schema->add_sort_key_idxes(1);
        base_schema->add_sort_key_idxes(2);

        _base_tablet_schema = TabletSchema::create(*base_schema);
        _base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_base_tablet_schema));

        // new tablet
        _new_tablet_metadata = std::make_shared<TabletMetadata>();
        _new_tablet_metadata->set_id(next_id());
        _new_tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL | SORTKEY |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |   NO    |
        //  |   c1   |  INT | NO  |  NO  |   YES   |
        //  |   c2   |  INT | NO  |  NO  |   NO    |
        auto new_schema = _new_tablet_metadata->mutable_schema();
        new_schema->set_id(next_id());
        new_schema->set_num_short_key_columns(1);
        new_schema->set_keys_type(GetParam().keys_type);
        new_schema->set_num_rows_per_row_block(65535);
        // c0 c1 c2 id should be same as base tablet schema
        {
            auto c0 = new_schema->add_column();
            c0->set_unique_id(c1_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = new_schema->add_column();
            c1->set_unique_id(c0_id);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        {
            auto c2 = new_schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_default_value("10");
            if (GetParam().keys_type == DUP_KEYS) {
                c2->set_aggregation("NONE");
            } else {
                c2->set_aggregation("REPLACE");
            }
        }
        new_schema->add_sort_key_idxes(1);

        _new_tablet_schema = TabletSchema::create(*new_schema);
        _new_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_new_tablet_schema));
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_sortkey_reorder_3";

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_base_tablet_metadata));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_new_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }
};

TEST_P(SchemaChangeSortKeyReorderTest3, test_alter_sortkey_reorder3) {
    std::vector<int> k0{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4};
    std::vector<int> k1{1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24};

    auto ck0 = Int32Column::create();
    auto ck1 = Int32Column::create();
    auto cv0 = Int32Column::create();
    ck0->append_numbers(k0.data(), k0.size() * sizeof(int));
    ck1->append_numbers(k1.data(), k1.size() * sizeof(int));
    cv0->append_numbers(v0.data(), v0.size() * sizeof(int));

    VChunk chunk0({ck0, ck1, cv0}, _base_schema);

    auto indexes = std::vector<uint32_t>(k0.size());
    for (int i = 0; i < k0.size(); i++) {
        indexes[i] = i;
    }

    int64_t version = 1;
    int64_t txn_id = next_id();
    auto base_tablet_id = _base_tablet_metadata->id();
    for (int i = 0; i < GetParam().writes_before; i++) {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_base_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id++;
    }

    // do schema change
    auto new_tablet_id = _new_tablet_metadata->id();
    auto alter_txn_id = txn_id++;
    {
        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = version;
        request.txn_id = alter_txn_id;

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
    }

    for (int i = 0; i < GetParam().writes_after; i++) {
        VChunk chunk1({ck0, ck1, cv0}, _new_schema);

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(new_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_new_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_log_version(_tablet_manager.get(), new_tablet_id, txn_id, version + 1));
        version++;
        txn_id++;
    }

    ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    version++;
    txn_id++;

    // check new tablet data
    std::vector<int> rc0_0{1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
    std::vector<int> rc1_0{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
    std::vector<int> rc2_0{2, 8, 14, 20, 4, 10, 16, 22, 6, 12, 18, 24};

    ASSIGN_OR_ABORT(auto new_metadata, _tablet_manager->get_tablet_metadata(new_tablet_id, version));
    auto reader = std::make_shared<TabletReader>(_tablet_manager.get(), new_metadata, *_new_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*_new_schema, 1024);

    if (GetParam().writes_before + GetParam().writes_after > 0) {
        CHECK_OK(reader->get_next(chunk.get()));
        for (int i = 0, sz = k0.size(); i < sz; i++) {
            EXPECT_EQ(rc0_0[i], chunk->get(i)[0].get_int32());
            EXPECT_EQ(rc1_0[i], chunk->get(i)[1].get_int32());
            EXPECT_EQ(rc2_0[i], chunk->get(i)[2].get_int32());
        }
        chunk->reset();
    }

    auto st = reader->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(SchemaChangeSortKeyReorderTest3, SchemaChangeSortKeyReorderTest3,
                         ::testing::Values(SchemaChangeParam{PRIMARY_KEYS, 0, 1},
                                           SchemaChangeParam{PRIMARY_KEYS, 0, 2},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 1},
                                           SchemaChangeParam{PRIMARY_KEYS, 2, 4}),
                         to_string_param_name);
// clang-format on

class SchemaChangeBaseTabletReadSchemaTest : public testing::Test {
public:
    SchemaChangeBaseTabletReadSchemaTest() {
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _location_provider = std::make_unique<FixedLocationProvider>(_test_dir);
        _update_manager = std::make_unique<UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);
    }

protected:
    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kTxnLogDirectoryName)));
    }

    void TearDown() override { ASSERT_OK(fs::remove_all(_test_dir)); }

    Status publish_version_for_schema_change(int64_t tablet_id, int64_t new_version, int64_t txn_id) {
        TxnInfoPB txn_info;
        txn_info.set_txn_id(txn_id);
        txn_info.set_combined_txn_log(false);
        txn_info.set_commit_time(time(nullptr));
        return publish_version(_tablet_manager.get(), tablet_id, 1, new_version,
                               std::span<const TxnInfoPB>(&txn_info, 1), false)
                .status();
    }

    // Create base tablet metadata schema (tablet metadata schema)
    // c0 (INT, KEY), c1 (VARCHAR(20), KEY), c2 (INT), c3 (INT), c4 (INT)
    std::shared_ptr<TabletMetadata> create_base_tablet_metadata() {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(next_id());
        metadata->set_version(1);

        auto schema = metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1); // stops before varchar key column c1
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        // Use deterministic column unique ids so the FE catalog schema (base_tablet_read_schema) can
        // reliably reference historical data by unique id in tests.
        constexpr int32_t c0_id = 1;
        constexpr int32_t c1_id = 2;
        constexpr int32_t c2_id = 3;
        constexpr int32_t c3_id = 4;
        constexpr int32_t c4_id = 5;

        // c0 (INT, KEY)
        {
            auto c0 = schema->add_column();
            c0->set_unique_id(c0_id);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }

        // c1 (VARCHAR(20), KEY)
        {
            auto c1 = schema->add_column();
            c1->set_unique_id(c1_id);
            c1->set_name("c1");
            c1->set_type("VARCHAR");
            c1->set_length(20);
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }

        // c2 (INT)
        {
            auto c2 = schema->add_column();
            c2->set_unique_id(c2_id);
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_aggregation("NONE");
        }

        // c3 (INT) - will be dropped
        {
            auto c3 = schema->add_column();
            c3->set_unique_id(c3_id);
            c3->set_name("c3");
            c3->set_type("INT");
            c3->set_is_key(false);
            c3->set_is_nullable(false);
            c3->set_aggregation("NONE");
        }

        // c4 (INT) - will be dropped and re-added with different type/unique_id
        {
            auto c4 = schema->add_column();
            c4->set_unique_id(c4_id);
            c4->set_name("c4");
            c4->set_type("INT");
            c4->set_is_key(false);
            c4->set_is_nullable(false);
            c4->set_aggregation("NONE");
        }

        return metadata;
    }

    // Create base_tablet_read_schema
    // c0 (INT, KEY), c1 (VARCHAR(20), KEY), c6 (INT, KEY), c2 (BIGINT), c5 (BIGINT), c4 (VARCHAR(20))
    TTabletSchema create_base_tablet_read_schema(int64_t schema_id) {
        TTabletSchema t_schema;
        t_schema.__set_id(schema_id);
        t_schema.__set_short_key_column_count(1); // stops before varchar key column c1
        t_schema.__set_keys_type(TKeysType::DUP_KEYS);

        // c0 (INT, KEY, unique_id=1)
        {
            TColumn c0;
            c0.__set_column_name("c0");
            TTypeNode type0;
            type0.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar0;
            scalar0.__set_type(TPrimitiveType::INT);
            type0.__set_scalar_type(scalar0);
            c0.__set_type_desc(TTypeDesc());
            c0.type_desc.__set_types({type0});
            c0.__set_col_unique_id(1);
            c0.__set_is_key(true);
            c0.__set_is_allow_null(false);
            t_schema.columns.push_back(c0);
        }

        // c1 (VARCHAR(20), KEY, unique_id=2)
        {
            TColumn c1;
            c1.__set_column_name("c1");
            TTypeNode type1;
            type1.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar1;
            scalar1.__set_type(TPrimitiveType::VARCHAR);
            scalar1.__set_len(20);
            type1.__set_scalar_type(scalar1);
            c1.__set_type_desc(TTypeDesc());
            c1.type_desc.__set_types({type1});
            c1.__set_col_unique_id(2);
            c1.__set_is_key(true);
            c1.__set_is_allow_null(false);
            c1.__set_index_len(20);
            t_schema.columns.push_back(c1);
        }

        // c6 (INT, KEY, unique_id=7) - new key column
        {
            TColumn c6;
            c6.__set_column_name("c6");
            TTypeNode type6;
            type6.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar6;
            scalar6.__set_type(TPrimitiveType::INT);
            type6.__set_scalar_type(scalar6);
            c6.__set_type_desc(TTypeDesc());
            c6.type_desc.__set_types({type6});
            c6.__set_col_unique_id(7);
            c6.__set_is_key(true);
            c6.__set_is_allow_null(true);
            t_schema.columns.push_back(c6);
        }

        // c2 (BIGINT, unique_id=3) - type changed from INT to BIGINT
        {
            TColumn c2;
            c2.__set_column_name("c2");
            TTypeNode type2;
            type2.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar2;
            scalar2.__set_type(TPrimitiveType::BIGINT);
            type2.__set_scalar_type(scalar2);
            c2.__set_type_desc(TTypeDesc());
            c2.type_desc.__set_types({type2});
            c2.__set_col_unique_id(3);
            c2.__set_is_key(false);
            c2.__set_is_allow_null(true);
            c2.__set_aggregation_type(TAggregationType::NONE);
            t_schema.columns.push_back(c2);
        }

        // c5 (BIGINT, unique_id=6) - new column
        {
            TColumn c5;
            c5.__set_column_name("c5");
            TTypeNode type5;
            type5.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar5;
            scalar5.__set_type(TPrimitiveType::BIGINT);
            type5.__set_scalar_type(scalar5);
            c5.__set_type_desc(TTypeDesc());
            c5.type_desc.__set_types({type5});
            c5.__set_col_unique_id(6);
            c5.__set_is_key(false);
            c5.__set_is_allow_null(true);
            c5.__set_aggregation_type(TAggregationType::NONE);
            t_schema.columns.push_back(c5);
        }

        // c4 (VARCHAR(20), unique_id=8) - dropped and re-added with different type/unique_id
        {
            TColumn c4;
            c4.__set_column_name("c4");
            TTypeNode type4;
            type4.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar4;
            scalar4.__set_type(TPrimitiveType::VARCHAR);
            scalar4.__set_len(20);
            type4.__set_scalar_type(scalar4);
            c4.__set_type_desc(TTypeDesc());
            c4.type_desc.__set_types({type4});
            c4.__set_col_unique_id(8); // different unique_id from tablet metadata schema
            c4.__set_is_key(false);
            c4.__set_is_allow_null(true);
            c4.__set_aggregation_type(TAggregationType::NONE);
            c4.__set_index_len(20);
            t_schema.columns.push_back(c4);
        }

        return t_schema;
    }

    std::unique_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;

    constexpr static const char* const _test_dir = "test_lake_base_tablet_read_schema";
    int64_t _partition_id = 100;
};

TEST_F(SchemaChangeBaseTabletReadSchemaTest, test_fallback_to_tablet_metadata_schema) {
    // TC-4: Test fallback to tablet metadata schema when base_tablet_read_schema is not provided
    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));

    // Create new tablet metadata (simple add column)
    auto new_metadata = std::make_shared<TabletMetadata>();
    new_metadata->set_id(next_id());
    new_metadata->set_version(1);
    auto new_schema = new_metadata->mutable_schema();
    new_schema->CopyFrom(base_metadata->schema());
    auto c5 = new_schema->add_column();
    c5->set_unique_id(6);
    c5->set_name("c5");
    c5->set_type("INT");
    c5->set_is_key(false);
    c5->set_is_nullable(false);
    c5->set_aggregation("NONE");
    CHECK_OK(_tablet_manager->put_tablet_metadata(*new_metadata));

    // Write test data
    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_schema = TabletSchema::create(base_metadata->schema());
    auto base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(base_tablet_schema));

    auto c0 = Int32Column::create();
    auto c1 = BinaryColumn::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    auto c4 = Int32Column::create();
    c0->append_datum(Datum(1));
    c1->append_datum(Datum("test"));
    c2->append_datum(Datum(100));
    c3->append_datum(Datum(200));
    c4->append_datum(Datum(300));

    VChunk chunk0({std::move(c0), std::move(c1), std::move(c2), std::move(c3), std::move(c4)}, base_schema);
    uint32_t indexes[1] = {0};

    ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_manager.get())
                                               .set_tablet_id(base_tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(base_tablet_schema->id())
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(chunk0, indexes, sizeof(indexes) / sizeof(indexes[0])));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();
    ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
    version++;

    // Do schema change without base_tablet_read_schema (should fallback to tablet metadata schema)
    auto new_tablet_id = new_metadata->id();
    auto alter_txn_id = txn_id + 1;
    {
        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = version;
        request.txn_id = alter_txn_id;
        // base_tablet_read_schema is not set, should fallback to tablet metadata schema

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
    }

    // Publish schema change
    ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
}

TEST_F(SchemaChangeBaseTabletReadSchemaTest, test_materialized_view) {
    // base_tablet_read_schema differs from tablet metadata schema, with materialized view
    // This triggers SortedSchemaChange because keys_type changes from DUP_KEYS to AGG_KEYS
    // Materialized view DDL:
    //   CREATE MATERIALIZED VIEW mv1 AS SELECT c0, c1, c6, c4, SUM(c2) AS c2_sum FROM base_table GROUP BY c0, c1, c6, c4;

    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));

    // Create new tablet metadata for materialized view with AGG_KEYS
    // New schema: c0 (INT, KEY), c1 (VARCHAR(20), KEY), c6 (INT, KEY), c4 (VARCHAR(20), KEY), c2_sum (BIGINT, SUM)
    auto new_metadata = std::make_shared<TabletMetadata>();
    new_metadata->set_id(next_id());
    new_metadata->set_version(1);
    auto new_schema = new_metadata->mutable_schema();
    new_schema->set_id(next_id());
    new_schema->set_num_short_key_columns(1);
    new_schema->set_keys_type(AGG_KEYS); // Changed from DUP_KEYS to AGG_KEYS for materialized view
    new_schema->set_num_rows_per_row_block(65535);

    // c0 (INT, KEY, unique_id=1)
    {
        auto c0 = new_schema->add_column();
        c0->set_unique_id(1);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    // c1 (VARCHAR(20), KEY, unique_id=2)
    {
        auto c1 = new_schema->add_column();
        c1->set_unique_id(2);
        c1->set_name("c1");
        c1->set_type("VARCHAR");
        c1->set_length(20);
        c1->set_is_key(true);
        c1->set_is_nullable(false);
    }
    // c6 (INT, KEY, unique_id=7) - new column for materialized view key
    {
        auto c6 = new_schema->add_column();
        c6->set_unique_id(7);
        c6->set_name("c6");
        c6->set_type("INT");
        // For AGG_KEYS, all key columns must appear before value columns.
        // Otherwise ChunkAggregator will treat a non-key column as key and DCHECK.
        c6->set_is_key(true);
        c6->set_is_nullable(true);
    }
    // c4 (VARCHAR(20), KEY, unique_id=8) - dropped and re-added
    {
        auto c4 = new_schema->add_column();
        c4->set_unique_id(8);
        c4->set_name("c4");
        c4->set_type("VARCHAR");
        c4->set_length(20);
        c4->set_is_key(true);
        c4->set_is_nullable(true);
    }
    // c2_sum (BIGINT, unique_id=9, SUM aggregation)
    {
        auto c2_sum = new_schema->add_column();
        c2_sum->set_unique_id(9);
        c2_sum->set_name("c2_sum");
        c2_sum->set_type("BIGINT");
        c2_sum->set_is_key(false);
        c2_sum->set_is_nullable(true);
        c2_sum->set_aggregation("SUM");
    }
    CHECK_OK(_tablet_manager->put_tablet_metadata(*new_metadata));

    // Write test data using tablet metadata schema
    int64_t version = 1;
    int64_t txn_id = next_id();
    auto base_tablet_schema = TabletSchema::create(base_metadata->schema());
    auto base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(base_tablet_schema));

    // Write multiple rows with same c0, c1 to test aggregation
    for (int i = 0; i < 3; i++) {
        auto col_c0 = Int32Column::create();
        auto col_c1 = BinaryColumn::create();
        auto col_c2 = Int32Column::create();
        auto col_c3 = Int32Column::create();
        auto col_c4 = Int32Column::create();

        col_c0->append_datum(Datum(i + 1));
        col_c1->append_datum(Datum(Slice(fmt::format("row{}", i))));
        col_c2->append_datum(Datum((i + 1) * 100));
        col_c3->append_datum(Datum((i + 1) * 1000));
        col_c4->append_datum(Datum((i + 1) * 10));

        VChunk chunk0({std::move(col_c0), std::move(col_c1), std::move(col_c2), std::move(col_c3), std::move(col_c4)},
                      base_schema);
        uint32_t indexes[1] = {0};

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(base_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes, sizeof(indexes) / sizeof(indexes[0])));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id = next_id();
    }

    // Do schema change with base_tablet_read_schema (this should trigger SortedSchemaChange)
    auto new_tablet_id = new_metadata->id();
    auto alter_txn_id = next_id();
    {
        // Create base_tablet_read_schema
        auto t_read_schema = create_base_tablet_read_schema(next_id());

        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = version;
        request.txn_id = alter_txn_id;

        // Set base_tablet_read_schema
        request.__set_base_tablet_read_schema(t_read_schema);

        // Set base_table_column_names - these are the columns we need to read for MV
        request.__set_base_table_column_names({"c0", "c1", "c6", "c2", "c4"});

        // Describe how to build MV columns. For this test we only need to map c2_sum to base column c2;
        // the AGG_KEYS/SUM behavior is driven by the new tablet schema.
        TAlterMaterializedViewParam mv_param;
        mv_param.__set_column_name("c2_sum");
        mv_param.__set_origin_column_name("c2");
        request.__set_materialized_view_params({mv_param});

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
    }

    // Publish schema change
    ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    version++;

    // Verify new tablet data
    auto new_tablet_schema = TabletSchema::create(new_metadata->schema());
    auto new_schema_for_read = std::make_shared<VSchema>(ChunkHelper::convert_schema(new_tablet_schema));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_manager->get_tablet_metadata(new_tablet_id, version));
    auto reader = std::make_shared<TabletReader>(_tablet_manager.get(), new_tablet_metadata, *new_schema_for_read);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*new_schema_for_read, 1024);
    int row_count = 0;
    while (true) {
        auto st = reader->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        CHECK_OK(st);
        for (int i = 0; i < chunk->num_rows(); i++) {
            EXPECT_EQ(row_count + 1, chunk->get(i)[0].get_int32());                               // c0
            EXPECT_EQ(fmt::format("row{}", row_count), chunk->get(i)[1].get_slice().to_string()); // c1
            EXPECT_TRUE(chunk->get(i)[2].is_null());                                              // c6 (new, NULL)
            EXPECT_TRUE(chunk->get(i)[3].is_null());                        // c4 (new unique_id, NULL)
            EXPECT_EQ((row_count + 1) * 100, chunk->get(i)[4].get_int64()); // c2_sum
            row_count++;
        }
        chunk->reset();
    }
    ASSERT_EQ(3, row_count);
}

TEST_F(SchemaChangeBaseTabletReadSchemaTest, test_generated_column) {
    // base_tablet_read_schema differs from tablet metadata schema, with generated column
    // This triggers DirectSchemaChange

    auto base_metadata = create_base_tablet_metadata();
    auto base_tablet_id = base_metadata->id();
    CHECK_OK(_tablet_manager->put_tablet_metadata(*base_metadata));

    // Create new tablet metadata with generated column
    // New schema: c0 (INT, KEY), c1 (VARCHAR(20), KEY), c6 (INT, KEY), c2 (BIGINT), c5 (BIGINT), c4 (VARCHAR(20)),
    //             c7 (BIGINT, generated = c2 + c5)
    auto new_metadata = std::make_shared<TabletMetadata>();
    new_metadata->set_id(next_id());
    new_metadata->set_version(1);
    auto new_schema = new_metadata->mutable_schema();
    new_schema->set_id(next_id());
    new_schema->set_num_short_key_columns(1);
    new_schema->set_keys_type(DUP_KEYS);
    new_schema->set_num_rows_per_row_block(65535);

    // c0 (INT, KEY, unique_id=1)
    {
        auto c0 = new_schema->add_column();
        c0->set_unique_id(1);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    // c1 (VARCHAR(20), KEY, unique_id=2)
    {
        auto c1 = new_schema->add_column();
        c1->set_unique_id(2);
        c1->set_name("c1");
        c1->set_type("VARCHAR");
        c1->set_length(20);
        c1->set_is_key(true);
        c1->set_is_nullable(false);
    }
    // c6 (INT, KEY, unique_id=7)
    {
        auto c6 = new_schema->add_column();
        c6->set_unique_id(7);
        c6->set_name("c6");
        c6->set_type("INT");
        // Keep keys unchanged (c0,c1) so generated-column schema change can run in DirectSchemaChange mode.
        c6->set_is_key(false);
        c6->set_is_nullable(true);
        c6->set_aggregation("NONE");
    }
    // c2 (BIGINT, unique_id=3)
    {
        auto c2 = new_schema->add_column();
        c2->set_unique_id(3);
        c2->set_name("c2");
        c2->set_type("BIGINT");
        c2->set_is_key(false);
        c2->set_is_nullable(true);
        c2->set_aggregation("NONE");
    }
    // c5 (BIGINT, unique_id=6)
    {
        auto c5 = new_schema->add_column();
        c5->set_unique_id(6);
        c5->set_name("c5");
        c5->set_type("BIGINT");
        c5->set_is_key(false);
        c5->set_is_nullable(true);
        c5->set_aggregation("NONE");
    }
    // c4 (VARCHAR(20), unique_id=8)
    {
        auto c4 = new_schema->add_column();
        c4->set_unique_id(8);
        c4->set_name("c4");
        c4->set_type("VARCHAR");
        c4->set_length(20);
        c4->set_is_key(false);
        c4->set_is_nullable(true);
        c4->set_aggregation("NONE");
    }
    // c7 (INT, unique_id=10, generated column = c0 * 2)
    {
        auto c7 = new_schema->add_column();
        c7->set_unique_id(10);
        c7->set_name("c7");
        c7->set_type("INT");
        c7->set_is_key(false);
        c7->set_is_nullable(true);
        c7->set_aggregation("NONE");
    }
    CHECK_OK(_tablet_manager->put_tablet_metadata(*new_metadata));

    // Write test data using tablet metadata schema
    int64_t version = 1;
    int64_t txn_id = 1000;
    auto base_tablet_schema = TabletSchema::create(base_metadata->schema());
    auto base_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(base_tablet_schema));

    for (int i = 0; i < 3; i++) {
        auto col_c0 = Int32Column::create();
        auto col_c1 = BinaryColumn::create();
        auto col_c2 = Int32Column::create();
        auto col_c3 = Int32Column::create();
        auto col_c4 = Int32Column::create();

        col_c0->append_datum(Datum(i + 1));
        col_c1->append_datum(Datum(Slice(fmt::format("row{}", i))));
        col_c2->append_datum(Datum((i + 1) * 100));
        col_c3->append_datum(Datum((i + 1) * 1000));
        col_c4->append_datum(Datum((i + 1) * 10));

        VChunk chunk0({std::move(col_c0), std::move(col_c1), std::move(col_c2), std::move(col_c3), std::move(col_c4)},
                      base_schema);
        uint32_t indexes[1] = {0};

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_manager.get())
                                                   .set_tablet_id(base_tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(base_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes, sizeof(indexes) / sizeof(indexes[0])));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(TEST_publish_single_version(_tablet_manager.get(), base_tablet_id, version + 1, txn_id).status());
        version++;
        txn_id++;
    }

    // Do schema change with base_tablet_read_schema and generated column expression
    auto new_tablet_id = new_metadata->id();
    auto alter_txn_id = txn_id++;
    {
        // Create base_tablet_read_schema
        auto t_read_schema = create_base_tablet_read_schema(next_id());
        // Keep keys unchanged (c0,c1) so schema change doesn't require sorting.
        ASSERT_GE(t_read_schema.columns.size(), 3);
        t_read_schema.columns[2].__set_is_key(false); // c6

        TAlterTabletReqV2 request;
        request.base_tablet_id = base_tablet_id;
        request.new_tablet_id = new_tablet_id;
        request.alter_version = version;
        request.txn_id = alter_txn_id;

        // Set base_tablet_read_schema
        request.__set_base_tablet_read_schema(t_read_schema);

        // Set base_table_column_names
        request.__set_base_table_column_names({"c0", "c1", "c6", "c2", "c5", "c4"});

        // Set generated column expression for c7 = c0 * 2
        TAlterTabletMaterializedColumnReq mc_request;

        std::vector<TExprNode> nodes;
        TExprNode node;
        node.node_type = TExprNodeType::SLOT_REF;
        node.type = gen_type_desc(TPrimitiveType::INT);
        node.num_children = 0;
        TSlotRef t_slot_ref = TSlotRef();
        t_slot_ref.slot_id = 0; // c0
        t_slot_ref.tuple_id = 0;
        node.__set_slot_ref(t_slot_ref);
        node.is_nullable = true;
        nodes.emplace_back(node);

        TExpr t_expr;
        t_expr.nodes = nodes;

        std::map<int32_t, starrocks::TExpr> m_expr;
        m_expr.insert({6, t_expr}); // c7 is at index 6 in new schema

        mc_request.__set_query_globals(TQueryGlobals());
        mc_request.__set_query_options(TQueryOptions());
        mc_request.__set_mc_exprs(m_expr);

        request.__set_materialized_column_req(mc_request);

        SchemaChangeHandler handler(_tablet_manager.get());
        ASSERT_OK(handler.process_alter_tablet(request));
    }

    // Publish schema change
    ASSERT_OK(publish_version_for_schema_change(new_tablet_id, version + 1, alter_txn_id));
    version++;

    // Verify new tablet data
    auto new_tablet_schema = TabletSchema::create(new_metadata->schema());
    auto new_schema_for_read = std::make_shared<VSchema>(ChunkHelper::convert_schema(new_tablet_schema));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_manager->get_tablet_metadata(new_tablet_id, version));
    auto reader = std::make_shared<TabletReader>(_tablet_manager.get(), new_tablet_metadata, *new_schema_for_read);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkHelper::new_chunk(*new_schema_for_read, 1024);
    int row_count = 0;
    while (true) {
        auto st = reader->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        CHECK_OK(st);
        for (int i = 0; i < chunk->num_rows(); i++) {
            EXPECT_EQ(row_count + 1, chunk->get(i)[0].get_int32());                               // c0
            EXPECT_EQ(fmt::format("row{}", row_count), chunk->get(i)[1].get_slice().to_string()); // c1
            EXPECT_TRUE(chunk->get(i)[2].is_null());                                              // c6 (new, NULL)
            EXPECT_EQ((row_count + 1) * 100, chunk->get(i)[3].get_int64());                       // c2 (INT->BIGINT)
            EXPECT_TRUE(chunk->get(i)[4].is_null());                                              // c5 (new, NULL)
            EXPECT_TRUE(chunk->get(i)[5].is_null());                  // c4 (new unique_id, NULL)
            EXPECT_EQ((row_count + 1), chunk->get(i)[6].get_int32()); // c7 (generated = c0)
            row_count++;
        }
        chunk->reset();
    }
    ASSERT_EQ(3, row_count);
}

} // namespace starrocks::lake
