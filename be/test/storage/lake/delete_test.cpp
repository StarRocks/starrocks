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

#include <set>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gen_cpp/types.pb.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/tablet_schema.h"
#include "test_util.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeTabletDeleteDataTest : public TestBase {
public:
    LakeTabletDeleteDataTest() : TestBase(kTestDirectory) {
        auto mutable_metadata = std::make_shared<TabletMetadata>();
        mutable_metadata->set_id(next_id());
        mutable_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c1   |  INT | YES |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
        auto schema = mutable_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(true);
            c1->set_is_nullable(false);
        }
        auto c2 = schema->add_column();
        {
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
        }

        _tablet_metadata = std::const_pointer_cast<const TabletMetadata>(mutable_metadata);
        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    void create_rowsets_with_data() {
        // Create data: c1 values 1-10, c2 values 1-10 (some rows have c2=3)
        std::vector<int> c1_values{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        std::vector<int> c2_values{1, 2, 3, 3, 5, 6, 3, 8, 9, 10}; // rows with c2=3: indices 2, 3, 6

        auto c1_col = Int32Column::create();
        auto c2_col = Int32Column::create();
        c1_col->append_numbers(c1_values.data(), c1_values.size() * sizeof(int));
        c2_col->append_numbers(c2_values.data(), c2_values.size() * sizeof(int));

        Chunk chunk({std::move(c1_col), std::move(c2_col)}, _schema);

        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

        int64_t txn_id1, txn_id2;

        // Create a mutable copy for adding rowsets
        auto mutable_metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);

        // Write rowset 1
        {
            txn_id1 = next_id();
            ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id1));
            ASSERT_OK(writer->open());
            ASSERT_OK(writer->write(chunk));
            ASSERT_OK(writer->finish());

            // Create txn_log for txn_id1
            auto txn_log1 = std::make_shared<TxnLog>();
            txn_log1->set_tablet_id(_tablet_metadata->id());
            txn_log1->set_txn_id(txn_id1);
            auto op_write1 = txn_log1->mutable_op_write();
            for (const auto& f : writer->segments()) {
                op_write1->mutable_rowset()->add_segments(f.path);
            }
            op_write1->mutable_rowset()->set_num_rows(writer->num_rows());
            op_write1->mutable_rowset()->set_data_size(writer->data_size());
            op_write1->mutable_rowset()->set_overlapped(true);
            ASSERT_OK(_tablet_mgr->put_txn_log(txn_log1));

            writer->close();

            auto* rowset = mutable_metadata->add_rowsets();
            rowset->set_overlapped(true);
            rowset->set_id(1);
            rowset->set_num_rows(chunk.num_rows());
            auto* segs = rowset->mutable_segments();
            for (const auto& file : writer->segments()) {
                segs->Add()->assign(file.path);
            }
        }

        // Write rowset 2
        {
            std::vector<int> c1_values2{11, 12, 13, 14, 15};
            std::vector<int> c2_values2{3, 12, 13, 3, 15}; // rows with c2=3: indices 0, 3

            auto c1_col2 = Int32Column::create();
            auto c2_col2 = Int32Column::create();
            c1_col2->append_numbers(c1_values2.data(), c1_values2.size() * sizeof(int));
            c2_col2->append_numbers(c2_values2.data(), c2_values2.size() * sizeof(int));

            Chunk chunk2({std::move(c1_col2), std::move(c2_col2)}, _schema);

            txn_id2 = next_id();
            ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id2));
            ASSERT_OK(writer->open());
            ASSERT_OK(writer->write(chunk2));
            ASSERT_OK(writer->finish());

            // Create txn_log for txn_id2
            auto txn_log2 = std::make_shared<TxnLog>();
            txn_log2->set_tablet_id(_tablet_metadata->id());
            txn_log2->set_txn_id(txn_id2);
            auto op_write2 = txn_log2->mutable_op_write();
            for (const auto& f : writer->segments()) {
                op_write2->mutable_rowset()->add_segments(f.path);
            }
            op_write2->mutable_rowset()->set_num_rows(writer->num_rows());
            op_write2->mutable_rowset()->set_data_size(writer->data_size());
            op_write2->mutable_rowset()->set_overlapped(true);
            ASSERT_OK(_tablet_mgr->put_txn_log(txn_log2));

            writer->close();

            auto* rowset = mutable_metadata->add_rowsets();
            rowset->set_overlapped(true);
            rowset->set_id(2);
            rowset->set_num_rows(chunk2.num_rows());
            auto* segs = rowset->mutable_segments();
            for (const auto& file : writer->segments()) {
                segs->Add()->assign(file.path);
            }
        }

        // Publish version 3 with both txn_ids
        // For batch_publish, new_version must equal base_version + txn_ids.size()
        std::vector<int64_t> txn_ids{txn_id1, txn_id2};
        ASSIGN_OR_ABORT(auto metadata_v3, batch_publish(_tablet_metadata->id(), 1, 3, txn_ids));
        _tablet_metadata = metadata_v3;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_tablet_delete_data";

    TabletMetadataPtr _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeTabletDeleteDataTest, test_delete_data_without_schema_key) {
    create_rowsets_with_data();

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

    // Create delete predicate: c2 = 3
    DeletePredicatePB delete_predicate;
    delete_predicate.set_version(-1);
    auto* binary_predicate = delete_predicate.add_binary_predicates();
    binary_predicate->set_column_name("c2");
    binary_predicate->set_op("=");
    binary_predicate->set_value("3");

    // Execute delete_data without schema_key
    int64_t txn_id = next_id();
    ASSERT_OK(tablet.delete_data(txn_id, delete_predicate, nullptr));

    // Verify txn log was created
    ASSIGN_OR_ABORT(auto txn_log, tablet.get_txn_log(txn_id));
    ASSERT_TRUE(txn_log->has_op_write());
    ASSERT_TRUE(txn_log->op_write().has_rowset());
    ASSERT_TRUE(txn_log->op_write().rowset().has_delete_predicate());
    ASSERT_FALSE(txn_log->op_write().has_schema_key());

    // Publish version 4
    ASSIGN_OR_ABORT(auto metadata_v4, publish_single_version(_tablet_metadata->id(), 4, txn_id));

    // Verify delete predicate is in the rowset
    ASSERT_EQ(3, metadata_v4->rowsets_size());
    const auto& delete_rowset = metadata_v4->rowsets(2);
    ASSERT_TRUE(delete_rowset.has_delete_predicate());
    ASSERT_EQ(1, delete_rowset.delete_predicate().binary_predicates_size());
    ASSERT_EQ("c2", delete_rowset.delete_predicate().binary_predicates(0).column_name());
    ASSERT_EQ("=", delete_rowset.delete_predicate().binary_predicates(0).op());
    ASSERT_EQ("3", delete_rowset.delete_predicate().binary_predicates(0).value());

    // Read data and verify deletion
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata_v4, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    int total_rows = 0;
    std::set<int> remaining_c1_values;
    while (true) {
        read_chunk_ptr->reset();
        auto st = reader->get_next(read_chunk_ptr.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_OK(st);
        total_rows += read_chunk_ptr->num_rows();
        for (int i = 0; i < read_chunk_ptr->num_rows(); i++) {
            int c1_val = read_chunk_ptr->get(i)[0].get_int32();
            int c2_val = read_chunk_ptr->get(i)[1].get_int32();
            remaining_c1_values.insert(c1_val);
            // Verify no rows with c2=3 remain
            ASSERT_NE(3, c2_val) << "Found row with c2=3: c1=" << c1_val;
        }
    }

    // Original data: 10 rows in rowset1 + 5 rows in rowset2 = 15 rows
    // Rows with c2=3: c1=3,4,7 (rowset1) and c1=11,14 (rowset2) = 5 rows
    // Expected remaining: 15 - 5 = 10 rows
    ASSERT_EQ(10, total_rows);
    // Verify specific rows remain
    ASSERT_TRUE(remaining_c1_values.find(1) != remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(2) != remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(5) != remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(6) != remaining_c1_values.end());
    // Verify deleted rows are gone
    ASSERT_TRUE(remaining_c1_values.find(3) == remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(4) == remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(7) == remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(11) == remaining_c1_values.end());
    ASSERT_TRUE(remaining_c1_values.find(14) == remaining_c1_values.end());

    reader->close();
}

TEST_F(LakeTabletDeleteDataTest, test_delete_data_with_schema_key) {
    create_rowsets_with_data();

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

    // Create extended schema with c3 int default 5
    // The schema_key points to a schema that extends the tablet schema with c3
    int64_t schema_id = next_id();
    TableSchemaKeyPB schema_key;
    schema_key.set_db_id(next_id());
    schema_key.set_table_id(next_id());
    schema_key.set_schema_id(schema_id);

    // Create extended schema: tablet schema + c3 int default 5
    TabletSchemaPB extended_schema_pb;
    extended_schema_pb.CopyFrom(_tablet_metadata->schema());
    extended_schema_pb.set_id(schema_id);
    extended_schema_pb.set_schema_version(_tablet_metadata->schema().schema_version() + 1);

    // Add c3 column with default value 5
    auto c3 = extended_schema_pb.add_column();
    c3->set_unique_id(next_id());
    c3->set_name("c3");
    c3->set_type("INT");
    c3->set_is_key(false);
    c3->set_is_nullable(false);
    // Set default value to "5"
    c3->set_default_value("5");

    // Cache the extended schema before publish
    auto extended_schema = TabletSchema::create(extended_schema_pb);
    _tablet_mgr->cache_schema(extended_schema);

    // Create delete predicate: c3 = 5
    // Since c3 has default value 5 for all existing rows in the extended schema,
    // this condition will match and delete all rows.
    DeletePredicatePB delete_predicate;
    delete_predicate.set_version(-1);
    auto* binary_predicate = delete_predicate.add_binary_predicates();
    binary_predicate->set_column_name("c3");
    binary_predicate->set_op("=");
    binary_predicate->set_value("5");

    int64_t txn_id = next_id();
    ASSERT_OK(tablet.delete_data(txn_id, delete_predicate, &schema_key));

    // Verify txn log was created with schema_key
    ASSIGN_OR_ABORT(auto txn_log, tablet.get_txn_log(txn_id));
    ASSERT_TRUE(txn_log->op_write().has_schema_key());
    ASSERT_EQ(schema_id, txn_log->op_write().schema_key().schema_id());

    // Publish version 4
    ASSIGN_OR_ABORT(auto metadata_v4, publish_single_version(_tablet_metadata->id(), 4, txn_id));

    // 1. Verify that the schema ID in metadata_v4 is the same as the extended_schema
    ASSERT_EQ(schema_id, metadata_v4->schema().id());

    // Verify delete predicate is in the rowset
    ASSERT_EQ(3, metadata_v4->rowsets_size());
    const auto& delete_rowset = metadata_v4->rowsets(2);
    ASSERT_TRUE(delete_rowset.has_delete_predicate());
    ASSERT_EQ(1, delete_rowset.delete_predicate().binary_predicates_size());
    ASSERT_EQ("c3", delete_rowset.delete_predicate().binary_predicates(0).column_name());
    ASSERT_EQ("5", delete_rowset.delete_predicate().binary_predicates(0).value());

    // 2. Build TabletReader according to extended_schema for data read verification
    // All rows should be deleted because c3 defaults to 5.
    auto extended_schema_obj = std::make_shared<Schema>(ChunkHelper::convert_schema(extended_schema));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata_v4, *extended_schema_obj);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*extended_schema_obj, 1024);
    int total_rows = 0;
    while (true) {
        read_chunk_ptr->reset();
        auto st = reader->get_next(read_chunk_ptr.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_OK(st);
        total_rows += read_chunk_ptr->num_rows();
        for (int i = 0; i < read_chunk_ptr->num_rows(); i++) {
            int c1_val = read_chunk_ptr->get(i)[0].get_int32();
            int c3_val = read_chunk_ptr->get(i)[2].get_int32();
            // Verify rows with c3=5 are deleted.
            ASSERT_NE(5, c3_val) << "Found row with c3=5: c1=" << c1_val;
        }
    }

    // Since c3 defaults to 5 for all rows and we have a delete predicate c3=5,
    // all rows should be deleted.
    ASSERT_EQ(0, total_rows);
    reader->close();
}

} // namespace starrocks::lake
