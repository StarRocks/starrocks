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
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "column/vectorized_schema.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks::vectorized;

using VSchema = starrocks::VectorizedSchema;
using VChunk = starrocks::Chunk;

class PrimaryKeyTest : public testing::Test {
public:
    PrimaryKeyTest() {
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _update_manager = std::make_unique<UpdateManager>(_location_provider.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider.get(), _update_manager.get(), 0);
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
        schema->set_keys_type(PRIMARY_KEYS);
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
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }

protected:
    constexpr static const char* const kTestGroupPath = "test_primary_key";

    std::unique_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::unique_ptr<UpdateManager> _update_manager;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
    int64_t _txnlog{1};
};

TEST_F(PrimaryKeyTest, test_write_success) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    VChunk chunk0({c0, c1}, _schema);
    auto rowset_txn_meta = std::make_unique<RowsetTxnMetaPB>();

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
    std::shared_ptr<const TabletSchema> const_schema = _tablet_schema;
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(rowset_txn_meta.get(), const_schema));
    ASSERT_OK(writer->open());

    // write segment #1
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->finish());

    // write txnlog
    int64_t logs[1];
    logs[0] = _txnlog;
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_metadata->id());
    txn_log->set_txn_id(_txnlog++);
    auto op_write = txn_log->mutable_op_write();
    for (auto& f : writer->files()) {
        op_write->mutable_rowset()->add_segments(std::move(f));
    }
    op_write->mutable_rowset()->set_num_rows(writer->num_rows());
    op_write->mutable_rowset()->set_data_size(writer->data_size());
    op_write->mutable_rowset()->set_overlapped(false);

    ASSERT_OK(_tablet_manager->put_txn_log(txn_log));

    writer->close();

    ASSIGN_OR_ABORT(auto score, _tablet_manager->publish_version(_tablet_metadata->id(), 1, 2, logs, 1));
    EXPECT_TRUE(score > 0.0);

    // read at version 2
    ASSIGN_OR_ABORT(auto reader, tablet.new_reader(2, *_schema));
    ASSERT_OK(reader->prepare());
    vectorized::TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
    ASSERT_EQ(k0.size(), read_chunk_ptr->num_rows());

    for (int i = 0, sz = k0.size(); i < sz; i++) {
        EXPECT_EQ(k0[i], read_chunk_ptr->get(i)[0].get_int32());
        EXPECT_EQ(v0[i], read_chunk_ptr->get(i)[1].get_int32());
    }
}

} // namespace starrocks::lake
