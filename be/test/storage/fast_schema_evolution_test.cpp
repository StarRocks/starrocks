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

#include <cstdlib>

#include "fs/fs_memory.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/persistent_index_compaction_manager.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset_update_state.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace starrocks {

class FastSchemaEvolutionTest : public testing::Test {
public:
    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.__set_id(1);
        request.tablet_schema.__set_schema_version(1);
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                  Column* one_delete = nullptr) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        size_t size = keys.size();
        for (size_t i = 0; i < size; i++) {
            cols[0]->append_datum(Datum(keys[i]));
            cols[1]->append_datum(Datum((int16_t)(keys[i] % size + 1)));
            cols[2]->append_datum(Datum((int32_t)(keys[i] % size + 2)));
        }
        if (one_delete == nullptr && !keys.empty()) {
            CHECK_OK(writer->flush_chunk(*chunk));
        } else if (one_delete == nullptr) {
            CHECK_OK(writer->flush());
        } else if (one_delete != nullptr) {
            CHECK_OK(writer->flush_chunk_with_deletes(*chunk, *one_delete));
        }
        return *writer->build();
    }
};

TEST_F(FastSchemaEvolutionTest, update_schema_id_test) {
    TabletSharedPtr tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    const size_t N = 100;
    std::vector<int64_t> keys(N);
    std::vector<Slice> key_slices;
    key_slices.reserve(N);
    for (int64_t i = 0; i < N; ++i) {
        keys[i] = i;
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(uint64_t));
    }
    TabletSchemaCSPtr ori_tablet_schema = tablet->tablet_schema();
    RowsetSharedPtr rowset = create_rowset(tablet, keys);
    EXPECT_EQ(ori_tablet_schema->id(), 1);
    EXPECT_EQ(tablet->tablet_schema().get(), rowset->tablet_schema().get());
    EXPECT_EQ(ori_tablet_schema.get(), tablet->tablet_schema().get());

    TabletSchemaSPtr new_schema = std::make_shared<TabletSchema>();
    new_schema->copy_from(ori_tablet_schema);
    new_schema->set_schema_version(ori_tablet_schema->schema_version() + 1);
    new_schema->set_id(100);
    tablet->update_max_version_schema(new_schema);

    RowsetSharedPtr rowset2 = create_rowset(tablet, keys);

    EXPECT_EQ(tablet->tablet_schema().get(), rowset2->tablet_schema().get());
    EXPECT_NE(rowset->tablet_schema().get(), rowset2->tablet_schema().get());
    EXPECT_EQ(ori_tablet_schema.get(), rowset->tablet_schema().get());
}

} // namespace starrocks