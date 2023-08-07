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

#include "storage/binlog_test_base.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"

namespace starrocks {

class TabletBinlogTest : public BinlogTestBase {
public:
    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::DUP_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TBinlogConfig binlog_config;
        binlog_config.version = 1;
        binlog_config.binlog_enable = true;
        binlog_config.binlog_ttl_second = 30 * 60;
        binlog_config.binlog_max_size = INT64_MAX;
        request.__set_binlog_config(binlog_config);

        TColumn k1;
        k1.column_name = "k1";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k1);

        TColumn v1;
        v1.column_name = "v1";
        v1.__set_is_key(false);
        v1.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(v1);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    void create_rowset(TabletSharedPtr tablet, std::vector<int32_t> num_rows_per_segment, RowsetSharedPtr* rowset) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 5;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, 10);
        for (int32_t i = 0, total_rows = 0; i < num_rows_per_segment.size(); i++) {
            int32_t num_rows = num_rows_per_segment[i];
            chunk->reset();
            auto& cols = chunk->columns();
            for (int32_t j = total_rows; j < total_rows + num_rows; j++) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(j)));
                cols[1]->append_datum(Datum(static_cast<int32_t>(j + 1)));
            }
            total_rows += num_rows;
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        auto status_or = writer->build();
        ASSERT_OK(status_or.status());
        *rowset = status_or.value();
    }

    void SetUp() override {
        srand(GetCurrentTimeMicros());
        _tablet = create_tablet(rand(), rand());
    }

    void TearDown() override {
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

protected:
    void ingest_random_binlog(TabletSharedPtr tablet, int64_t start_version, int64_t num_version,
                              std::vector<DupKeyVersionInfo>* version_infos);

    TabletSharedPtr _tablet;
};

void TabletBinlogTest::ingest_random_binlog(TabletSharedPtr tablet, int64_t start_version, int64_t num_version,
                                            std::vector<DupKeyVersionInfo>* version_infos) {
    for (int32_t version = start_version; version < start_version + num_version; version++) {
        int32_t num_segments = std::rand() % 5;
        int32_t num_rows_per_segment = std::rand() % 100 + 1;
        std::vector<int32_t> segment_rows;
        for (int i = 0; i < num_segments; i++) {
            segment_rows.push_back(num_rows_per_segment);
        }
        RowsetSharedPtr rowset;
        create_rowset(tablet, segment_rows, &rowset);
        ASSERT_OK(tablet->add_inc_rowset(rowset, version));
        int64_t timestamp = rowset->creation_time() * 1000000;
        version_infos->push_back(DupKeyVersionInfo(version, num_segments, num_rows_per_segment, timestamp));
    }
}

TEST_F(TabletBinlogTest, test_config_binlog) {
    std::shared_ptr<BinlogConfig> binlog_config = _tablet->tablet_meta()->get_binlog_config();
    ASSERT_EQ(1, binlog_config->version);
    ASSERT_TRUE(binlog_config->binlog_enable);
    ASSERT_EQ(30 * 60, binlog_config->binlog_ttl_second);
    ASSERT_EQ(INT64_MAX, binlog_config->binlog_max_size);

    // higher version will override the configuration
    BinlogConfig binlog_config_3;
    binlog_config_3.update(3, true, 823, 984);
    _tablet->update_binlog_config(binlog_config_3);
    binlog_config = _tablet->tablet_meta()->get_binlog_config();
    ASSERT_EQ(3, binlog_config->version);
    ASSERT_TRUE(binlog_config->binlog_enable);
    ASSERT_EQ(823, binlog_config->binlog_ttl_second);
    ASSERT_EQ(984, binlog_config->binlog_max_size);

    // lower version would not override the configuration
    BinlogConfig binlog_config_2;
    binlog_config_2.update(2, true, 323, 475);
    _tablet->update_binlog_config(binlog_config_2);
    binlog_config = _tablet->tablet_meta()->get_binlog_config();
    ASSERT_EQ(3, binlog_config->version);
    ASSERT_TRUE(binlog_config->binlog_enable);
    ASSERT_EQ(823, binlog_config->binlog_ttl_second);
    ASSERT_EQ(984, binlog_config->binlog_max_size);
}

TEST_F(TabletBinlogTest, test_generate_binlog) {
    std::vector<DupKeyVersionInfo> version_infos;
    ingest_random_binlog(_tablet, 2, 100, &version_infos);
    BinlogManager* binlog_manager = _tablet->binlog_manager();
    std::map<BinlogLsn, BinlogFilePtr>& lsn_map = binlog_manager->alive_binlog_files();
    std::vector<BinlogFileMetaPBPtr> file_metas;
    for (auto it : lsn_map) {
        file_metas.push_back(it.second->file_meta());
    }
    verify_dup_key_multiple_versions(version_infos, _tablet->schema_hash_path(), file_metas);
}

TEST_F(TabletBinlogTest, test_publish_out_of_order) {
    std::vector<DupKeyVersionInfo> version_infos;
    for (int32_t version = 2; version < 100; version += 2) {
        for (int32_t k = 1; k >= 0; k--) {
            int32_t sub_version = version + k;
            int32_t num_segments = std::rand() % 5;
            int32_t num_rows_per_segment = std::rand() % 100 + 1;
            std::vector<int32_t> segment_rows;
            for (int i = 0; i < num_segments; i++) {
                segment_rows.push_back(num_rows_per_segment);
            }
            RowsetSharedPtr rowset;
            create_rowset(_tablet, segment_rows, &rowset);
            ASSERT_OK(_tablet->add_inc_rowset(rowset, sub_version));
            int64_t timestamp = rowset->creation_time() * 1000000;
            if (k == 1) {
                version_infos.push_back(DupKeyVersionInfo(sub_version, num_segments, num_rows_per_segment, timestamp));
            }
        }
    }

    BinlogManager* binlog_manager = _tablet->binlog_manager();
    std::map<BinlogLsn, BinlogFilePtr>& lsn_map = binlog_manager->alive_binlog_files();
    std::vector<BinlogFileMetaPBPtr> file_metas;
    for (auto it : lsn_map) {
        file_metas.push_back(it.second->file_meta());
    }
    verify_dup_key_multiple_versions(version_infos, _tablet->schema_hash_path(), file_metas);
}

TEST_F(TabletBinlogTest, test_load) {
    // verify load empty rowsets
    ASSERT_OK(_tablet->finish_load_rowsets());
    std::vector<DupKeyVersionInfo> version_infos;
    ingest_random_binlog(_tablet, 2, 50, &version_infos);
    // simulate to checkpoint tablet meta
    std::shared_ptr<TabletMetaPB> meta_checkpoint = std::make_shared<TabletMetaPB>();
    _tablet->tablet_meta()->to_meta_pb(meta_checkpoint.get());
    ingest_random_binlog(_tablet, 52, 50, &version_infos);

    // simulate the process of loading tablet as DataDir#load
    TabletMetaSharedPtr load_tablet_meta = TabletMeta::create();
    load_tablet_meta->init_from_pb(meta_checkpoint.get());
    auto load_tablet = Tablet::create_tablet_from_meta(load_tablet_meta, _tablet->data_dir());
    ASSERT_OK(load_tablet->init());
    for (int64_t version = 52; version < 102; version++) {
        ASSERT_OK(load_tablet->load_rowset(_tablet->get_inc_rowset_by_version(Version(version, version))));
    }
    ASSERT_OK(load_tablet->finish_load_rowsets());

    for (int64_t version = 2; version < 102; version++) {
        RowsetSharedPtr raw_rowset = _tablet->get_inc_rowset_by_version(Version(version, version));
        RowsetSharedPtr load_rowset = load_tablet->get_inc_rowset_by_version(Version(version, version));
        ASSERT_TRUE(raw_rowset != nullptr);
        ASSERT_TRUE(load_rowset != nullptr);
        ASSERT_EQ(raw_rowset->rowset_id(), load_rowset->rowset_id());
    }

    BinlogManager* binlog_manager = load_tablet->binlog_manager();
    std::map<BinlogLsn, BinlogFilePtr>& lsn_map = binlog_manager->alive_binlog_files();
    std::vector<BinlogFileMetaPBPtr> file_metas;
    for (auto it : lsn_map) {
        file_metas.push_back(it.second->file_meta());
    }
    verify_dup_key_multiple_versions(version_infos, load_tablet->schema_hash_path(), file_metas);
}

} // namespace starrocks
