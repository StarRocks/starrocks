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

#include "storage/dictionary_cache_manager.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>

#include "exec/tablet_info.h"
#include "runtime/descriptor_helper.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks {

class DictionaryCacheManagerTest : public testing::Test {
public:
    ~DictionaryCacheManagerTest() override = default;

    void SetUp() override {
        if (dictionary_cache_manager == nullptr) {
            dictionary_cache_manager = new starrocks::DictionaryCacheManager();
        }
        if (test_tablet == nullptr) {
            test_tablet = _create_tablet();
        }
        create_new_dictionary_cache(dictionary_cache_manager, 1, 1, test_tablet);
    }

    void TearDown() override {
        if (test_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(test_tablet->tablet_id());
            test_tablet.reset();
            test_tablet = nullptr;
        }
        if (dictionary_cache_manager) {
            delete dictionary_cache_manager;
            dictionary_cache_manager = nullptr;
        }
    }

    static TCreateTabletReq get_create_tablet_request(int64_t tablet_id, int64_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "k1";
        k1.__set_is_key(true);
        k1.__set_default_value("1");
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "k2";
        k2.__set_default_value("2");
        k2.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "k3";
        k3.__set_default_value("3");
        k3.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k3);

        return request;
    }

    static void create_new_dictionary_cache(starrocks::DictionaryCacheManager* dictionary_cache_manager, int64_t dict,
                                            int64_t txn_id, TabletSharedPtr tablet) {
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, 0);
        for (size_t i = 0; i < chunk->num_columns(); ++i) {
            chunk->set_slot_id_to_index(i, i);
            chunk->get_column_by_index(i)->append_default();
        }

        std::unique_ptr<ChunkPB> pchunk = std::make_unique<ChunkPB>();
        DictionaryCacheWriter::ChunkUtil::compress_and_serialize_chunk(chunk.get(), pchunk.get());

        TOlapTableSchemaParam tschema;
        tschema.db_id = 1;
        tschema.table_id = 1;
        tschema.version = 0;

        // descriptor
        {
            TDescriptorTableBuilder dtb;
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k1").column_pos(1).build());
            tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k1").column_pos(2).build());
            tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k1").column_pos(3).build());

            tuple_builder.build(&dtb);

            auto desc_tbl = dtb.desc_tbl();
            tschema.slot_descs = desc_tbl.slotDescriptors;
            tschema.tuple_desc = desc_tbl.tupleDescriptors[0];
        }
        // index
        tschema.indexes.resize(1);
        tschema.indexes[0].id = 4;
        tschema.indexes[0].columns = {"k1", "k2", "k3"};
        auto req = get_create_tablet_request(0, 0);
        TOlapTableColumnParam column_param;
        column_param.columns = (req.tablet_schema).columns;
        tschema.indexes[0].__set_column_param(column_param);

        OlapTableSchemaParam olap_schema;
        olap_schema.init(tschema);

        PRefreshDictionaryCacheRequest request;
        request.set_allocated_chunk(pchunk.get());
        request.set_dictionary_id(dict);
        request.set_txn_id(txn_id);
        request.set_allocated_schema(olap_schema.to_protobuf());
        request.set_memory_limit(1024 * 1024);
        request.set_key_size(1);

        dictionary_cache_manager->begin(dict, txn_id);
        dictionary_cache_manager->refresh(&request);
        dictionary_cache_manager->commit(dict, txn_id);

        request.release_chunk();
        request.release_schema();
    }

    starrocks::DictionaryCacheManager* dictionary_cache_manager = nullptr;
    TabletSharedPtr test_tablet = nullptr;

private:
    TabletSharedPtr _create_tablet() {
        auto st = StorageEngine::instance()->create_tablet(get_create_tablet_request(_tablet_id, _schema_hash));
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id, false);
    }

    const int64_t _tablet_id = 9143;
    const int64_t _schema_hash = 6542;
};

// NOLINTNEXTLINE
TEST_F(DictionaryCacheManagerTest, concurrent_refresh) {
    int N = 100;
    std::vector<std::thread> pool;
    for (int i = 0; i < N; ++i) {
        pool.emplace_back(create_new_dictionary_cache, dictionary_cache_manager, i + N, 1, test_tablet);
    }

    for (int i = 0; i < N; ++i) {
        pool[i].join();
    }
}

} // namespace starrocks
