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

#ifndef BE_TEST
#define BE_TEST
#endif

#include "exec/cache_stats_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config_exec_fwd.h"
#include "common/status.h"
#include "connector/cache_stats_connector.h"
#include "fs/fs_util.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/storage_env.h"

namespace starrocks {

class CacheStatsScannerTest : public ::testing::Test {
public:
    CacheStatsScannerTest() : _tablet_id(next_id()) {
        _location_provider = std::make_shared<lake::FixedLocationProvider>(kRootLocation);
        _tablet_mgr = StorageEnv::GetInstance()->lake_tablet_manager();
        _backup_location_provider = _tablet_mgr->TEST_set_location_provider(_location_provider);

        CHECK(FileSystem::Default()
                      ->create_dir_recursive(lake::join_path(kRootLocation, lake::kSegmentDirectoryName))
                      .ok());
        CHECK(FileSystem::Default()
                      ->create_dir_recursive(lake::join_path(kRootLocation, lake::kMetadataDirectoryName))
                      .ok());
        CHECK(FileSystem::Default()
                      ->create_dir_recursive(lake::join_path(kRootLocation, lake::kTxnLogDirectoryName))
                      .ok());

        TUniqueId query_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        TUniqueId fragment_id;
        auto* exec_env = ExecEnv::GetInstance();
        _state = _pool.add(new RuntimeState(query_id, fragment_id, query_options, query_globals,
                                            &exec_env->query_execution_services(), exec_env));
        _state->init_mem_trackers(query_id);
    }

    ~CacheStatsScannerTest() override {
        (void)_tablet_mgr->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kRootLocation);
    }

protected:
    std::shared_ptr<TabletMetadata> create_base_metadata(int64_t version) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(_tablet_id);
        metadata->set_version(version);

        auto schema = metadata->mutable_schema();
        schema->set_id(10);
        schema->set_schema_version(1);
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        auto c0 = schema->add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        return metadata;
    }

    void write_dummy_file(const std::string& path, int64_t size) {
        std::string content(size, 'x');
        ASSIGN_OR_ABORT(auto file, fs::new_writable_file(path));
        CHECK(file->append(Slice(content)).ok());
        CHECK(file->close().ok());
    }

    void create_tablet_with_segment(int64_t version, const std::string& segment_name, int64_t segment_size) {
        auto metadata = create_base_metadata(version);

        auto rowset = metadata->add_rowsets();
        auto* segment_meta = rowset->add_segment_metas();
        segment_meta->set_filename(segment_name);
        segment_meta->set_size(segment_size);

        CHECK(_tablet_mgr->put_tablet_metadata(*metadata).ok());

        write_dummy_file(_tablet_mgr->segment_location(_tablet_id, segment_name), segment_size);
    }

    void build_desc_tbl(bool with_unknown_slot = false) {
        TDescriptorTableBuilder table_desc_builder;
        TTupleDescriptorBuilder tuple_desc_builder;

        auto slot0 = TSlotDescriptorBuilder()
                             .type(LogicalType::TYPE_BIGINT)
                             .column_name("tablet_id")
                             .column_pos(0)
                             .nullable(false)
                             .build();
        auto slot1 = TSlotDescriptorBuilder()
                             .type(LogicalType::TYPE_BIGINT)
                             .column_name("cached_bytes")
                             .column_pos(1)
                             .nullable(false)
                             .build();
        auto slot2 = TSlotDescriptorBuilder()
                             .type(LogicalType::TYPE_BIGINT)
                             .column_name("total_bytes")
                             .column_pos(2)
                             .nullable(false)
                             .build();

        tuple_desc_builder.add_slot(slot0);
        tuple_desc_builder.add_slot(slot1);
        tuple_desc_builder.add_slot(slot2);
        if (with_unknown_slot) {
            auto slot3 = TSlotDescriptorBuilder()
                                 .type(LogicalType::TYPE_BIGINT)
                                 .column_name("unknown_col")
                                 .column_pos(3)
                                 .nullable(true)
                                 .build();
            tuple_desc_builder.add_slot(slot3);
        }
        tuple_desc_builder.build(&table_desc_builder);

        CHECK(DescriptorTbl::create(_state, &_pool, table_desc_builder.desc_tbl(), &_desc_tbl,
                                    config::vector_chunk_size)
                      .ok());
        _state->set_desc_tbl(_desc_tbl);
    }

    const TupleDescriptor* tuple_desc() { return _desc_tbl->get_tuple_descriptor(0); }

    constexpr static const char* const kRootLocation = "./CacheStatsScannerTest";
    lake::TabletManager* _tablet_mgr;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::shared_ptr<lake::LocationProvider> _backup_location_provider;
    int64_t _tablet_id;

    ObjectPool _pool;
    RuntimeState* _state = nullptr;
    DescriptorTbl* _desc_tbl = nullptr;
};

TEST_F(CacheStatsScannerTest, test_init_invalid_version) {
    build_desc_tbl();
    CacheStatsScanner scanner(tuple_desc());

    TInternalScanRange scan_range;
    scan_range.tablet_id = _tablet_id;
    scan_range.version = "abc";

    auto st = scanner.init(nullptr, scan_range);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.message().find("Invalid") != std::string::npos) << st.message();
}

TEST_F(CacheStatsScannerTest, test_get_chunk_metadata_not_found) {
    build_desc_tbl();
    CacheStatsScanner scanner(tuple_desc());

    TInternalScanRange scan_range;
    scan_range.tablet_id = _tablet_id;
    scan_range.version = "99";

    ASSERT_TRUE(scanner.init(nullptr, scan_range).ok());
    ASSERT_TRUE(scanner.open(nullptr).ok());

    ChunkPtr chunk;
    bool eos = false;
    auto st = scanner.get_chunk(nullptr, &chunk, &eos);
    ASSERT_FALSE(st.ok());
}

TEST_F(CacheStatsScannerTest, test_basic) {
    const int64_t version = 2;
    const int64_t seg_size = 512;
    const int64_t seg_without_size = 64;
    const int64_t seg_with_offset = 128;
    const int64_t delvec_size = 128;
    const int64_t sst_size = 256;
    const int64_t dcg_size = 200;
    const std::string delvec_name = lake::gen_delvec_filename(1);
    const std::string sst_name = lake::gen_sst_filename();
    const std::string dcg_name = lake::gen_cols_filename(1);

    auto metadata = create_base_metadata(version);

    auto rowset1 = metadata->add_rowsets();
    auto* segment_with_size = rowset1->add_segment_metas();
    segment_with_size->set_filename("seg_001.dat");
    segment_with_size->set_size(seg_size);

    auto rowset2 = metadata->add_rowsets();
    rowset2->add_segment_metas()->set_filename("seg_without_size.dat");

    auto rowset3 = metadata->add_rowsets();
    auto* segment_with_offset = rowset3->add_segment_metas();
    segment_with_offset->set_filename("seg_with_offset.dat");
    segment_with_offset->set_size(seg_with_offset);
    segment_with_offset->set_bundle_file_offset(64);

    auto delvec_meta = metadata->mutable_delvec_meta();
    auto& file_meta = (*delvec_meta->mutable_version_to_file())[1];
    file_meta.set_name(delvec_name);
    file_meta.set_size(delvec_size);

    auto sst_meta = metadata->mutable_sstable_meta();
    auto sst = sst_meta->add_sstables();
    sst->set_filename(sst_name);
    sst->set_filesize(sst_size);

    auto dcg_meta = metadata->mutable_dcg_meta();
    auto& dcg_ver = (*dcg_meta->mutable_dcgs())[0];
    dcg_ver.add_column_files(dcg_name);

    CHECK(_tablet_mgr->put_tablet_metadata(*metadata).ok());

    write_dummy_file(_tablet_mgr->segment_location(_tablet_id, "seg_001.dat"), seg_size);
    write_dummy_file(_tablet_mgr->segment_location(_tablet_id, "seg_without_size.dat"), seg_without_size);
    write_dummy_file(_tablet_mgr->segment_location(_tablet_id, "seg_with_offset.dat"), seg_with_offset);
    write_dummy_file(_tablet_mgr->delvec_location(_tablet_id, delvec_name), delvec_size);
    write_dummy_file(_tablet_mgr->sst_location(_tablet_id, sst_name), sst_size);
    write_dummy_file(_tablet_mgr->segment_location(_tablet_id, dcg_name), dcg_size);

    build_desc_tbl(true);
    CacheStatsScanner scanner(tuple_desc());

    TInternalScanRange scan_range;
    scan_range.tablet_id = _tablet_id;
    scan_range.version = std::to_string(version);

    ASSERT_TRUE(scanner.init(nullptr, scan_range).ok());
    ASSERT_TRUE(scanner.open(nullptr).ok());

    ChunkPtr chunk;
    bool eos = false;
    ASSERT_TRUE(scanner.get_chunk(nullptr, &chunk, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(chunk->num_rows(), 1);

    int64_t expected_total = seg_size + seg_without_size + seg_with_offset + delvec_size + sst_size + dcg_size;
    ASSERT_EQ(chunk->get_column_by_index(0)->get(0).get_int64(), _tablet_id);
    ASSERT_EQ(chunk->get_column_by_index(1)->get(0).get_int64(), expected_total);
    ASSERT_EQ(chunk->get_column_by_index(2)->get(0).get_int64(), expected_total);
    ASSERT_TRUE(chunk->get_column_by_index(3)->is_null(0));

    ASSERT_TRUE(scanner.get_chunk(nullptr, &chunk, &eos).ok());
    ASSERT_TRUE(eos);

    scanner.close(nullptr);

    CacheStatsScanner invalid_version_scanner(tuple_desc());
    scan_range.version = "0";
    ASSERT_TRUE(invalid_version_scanner.init(nullptr, scan_range).ok());
    eos = false;
    auto st = invalid_version_scanner.get_chunk(nullptr, &chunk, &eos);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.message().find("Invalid cache stats scan range version") != std::string::npos) << st.message();
}

TEST_F(CacheStatsScannerTest, test_connector_data_source) {
    const int64_t version = 4;
    const int64_t seg_size = 256;
    create_tablet_with_segment(version, "connector_seg.dat", seg_size);
    build_desc_tbl();

    connector::CacheStatsConnector connector;
    ASSERT_EQ(connector.connector_type(), connector::ConnectorType::CACHE_STATS);

    TCacheStatsScanNode scan_node;
    scan_node.__set_tuple_id(0);

    TPlanNode plan_node;
    plan_node.__set_cache_stats_scan_node(scan_node);

    auto provider = connector.create_data_source_provider(nullptr, plan_node);
    ASSERT_NE(provider, nullptr);
    ASSERT_FALSE(provider->insert_local_exchange_operator());
    ASSERT_FALSE(provider->accept_empty_scan_ranges());
    ASSERT_EQ(provider->tuple_descriptor(_state), _state->desc_tbl().get_tuple_descriptor(0));

    TInternalScanRange internal_scan_range;
    internal_scan_range.tablet_id = _tablet_id;
    internal_scan_range.version = std::to_string(version);

    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_scan_range);

    auto data_source = provider->create_data_source(scan_range);
    ASSERT_NE(data_source, nullptr);
    ASSERT_EQ(data_source->name(), "CacheStatsDataSource");
    ASSERT_TRUE(data_source->open(_state).ok());

    ChunkPtr chunk;
    auto st = data_source->get_next(_state, &chunk);
    ASSERT_TRUE(st.ok()) << st.message();
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->num_rows(), 1);
    ASSERT_EQ(data_source->raw_rows_read(), 1);
    ASSERT_EQ(data_source->num_rows_read(), 1);
    ASSERT_EQ(data_source->num_bytes_read(), chunk->bytes_usage());
    ASSERT_GT(data_source->num_bytes_read(), 0);
    ASSERT_EQ(data_source->cpu_time_spent(), 0);

    st = data_source->get_next(_state, &chunk);
    ASSERT_TRUE(st.is_end_of_file()) << st.to_string();

    data_source->close(_state);
}

} // namespace starrocks
