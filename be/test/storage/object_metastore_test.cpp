// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/object_metastore.h"

#include <aws/core/Aws.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "storage/tablet_meta.h"
#include "testutil/assert.h"

namespace starrocks {

// NOTE: The bucket must be created before running this test.
constexpr static const char* kBucketName = "starrocks-env-s3-unit-test";
constexpr static const std::string_view kTabletObjectMetastore = "/tablet_object_metastore_test";

class S3ObjectMetastoreTest : public testing::Test {
public:
    S3ObjectMetastoreTest() = default;
    ~S3ObjectMetastoreTest() override = default;
    void SetUp() override { Aws::InitAPI(_options); }
    void TearDown() override { Aws::ShutdownAPI(_options); }

    std::string S3Path(std::string_view path) {
        return fmt::format("s3://{}.{}{}", kBucketName, config::object_storage_endpoint, path);
    }

private:
    Aws::SDKOptions _options;
};

TEST_F(S3ObjectMetastoreTest, test_tablet_meta) {
    // Create a new TabletMeta
    TCreateTabletReq request;
    request.__set_tablet_id(1000001);
    request.__set_partition_id(1);
    request.__set_tablet_type(TTabletType::TABLET_TYPE_DISK);
    request.__set_tablet_schema(TTabletSchema());

    TTabletSchema& schema = request.tablet_schema;
    schema.__set_schema_hash(12345);
    schema.__set_is_in_memory(false);
    schema.__set_keys_type(TKeysType::DUP_KEYS);
    schema.__set_short_key_column_count(1);

    // c0 int key
    schema.columns.emplace_back();
    {
        TTypeNode type;
        type.__set_type(TTypeNodeType::SCALAR);
        type.__set_scalar_type(TScalarType());
        type.scalar_type.__set_type(TPrimitiveType::INT);

        schema.columns.back().__set_column_name("c0");
        schema.columns.back().__set_is_key(true);
        schema.columns.back().__set_index_len(sizeof(int32_t));
        schema.columns.back().__set_aggregation_type(TAggregationType::NONE);
        schema.columns.back().__set_is_allow_null(true);
        schema.columns.back().__set_type_desc(TTypeDesc());
        schema.columns.back().type_desc.__set_types({type});
    }

    std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
    col_ordinal_to_unique_id[0] = 0;

    std::unique_ptr<MemTracker> mem_tracker = std::make_unique<MemTracker>(-1);
    TabletMetaSharedPtr tablet_meta;
    Status st = TabletMeta::create(mem_tracker.get(), request, TabletUid(1, 1), 2,1, col_ordinal_to_unique_id,
                                   RowsetTypePB::BETA_ROWSET, &tablet_meta);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(tablet_meta != nullptr);
    tablet_meta->set_tablet_state(TabletState::TABLET_RUNNING);

    // Test: add and get tablet meta
    // s3://starrocks-env-s3-unit-test/tablet_object_metastore_test/tabletmeta_1000001
    auto metastore = new_object_metastore(S3Path(kTabletObjectMetastore));
    ASSERT_OK(metastore->add_tablet_meta(*tablet_meta));
    ASSIGN_OR_ABORT(auto new_tablet_meta,
                    metastore->get_tablet_meta(tablet_meta->tablet_id(), tablet_meta->schema_hash()));
    ASSERT_TRUE(new_tablet_meta != nullptr);
    ASSERT_EQ(TabletState::TABLET_RUNNING, new_tablet_meta->tablet_state());

    // Test: update and get tablet meta
    tablet_meta->set_tablet_state(TabletState::TABLET_SHUTDOWN);
    ASSERT_OK(metastore->update_tablet_meta(*tablet_meta));
    ASSIGN_OR_ABORT(auto new_tablet_meta2,
                    metastore->get_tablet_meta(tablet_meta->tablet_id(), tablet_meta->schema_hash()));
    ASSERT_TRUE(new_tablet_meta2 != nullptr);
    ASSERT_EQ(TabletState::TABLET_SHUTDOWN, new_tablet_meta2->tablet_state());

    // Test: remove tablet meta
    ASSERT_OK(metastore->remove_tablet_meta(tablet_meta->tablet_id(), tablet_meta->schema_hash()));
    ASSERT_FALSE(metastore->get_tablet_meta(tablet_meta->tablet_id(), tablet_meta->schema_hash()).ok());
}

TEST_F(S3ObjectMetastoreTest, test_rowset_meta) {
    // Create a new RowsetMeta
    RowsetMeta rowset_meta1;
    rowset_meta1.set_tablet_uid(TabletUid(1, 1));
    rowset_meta1.set_start_version(1);
    rowset_meta1.set_end_version(1);
    rowset_meta1.set_num_rows(1000);
    rowset_meta1.set_num_segments(1);
    rowset_meta1.set_data_disk_size(1024);
    rowset_meta1.set_rowset_seg_id(1);
    RowsetId rowsetId1;
    rowsetId1.init(2, 1, 0, 0);
    rowset_meta1.set_rowset_id(rowsetId1);
    rowset_meta1.set_rowset_state(RowsetStatePB::COMMITTED);

    // Test: add and get rowset meta
    // s3://starrocks-env-s3-unit-test/tablet_object_metastore_test/rst_00...01-00...01_020000000000000100...
    auto metastore = new_object_metastore(S3Path(kTabletObjectMetastore));
    ASSERT_OK(metastore->add_rowset_meta(rowset_meta1));
    ASSIGN_OR_ABORT(auto new_rowset_meta1,
                    metastore->get_rowset_meta(rowset_meta1.tablet_uid(), rowset_meta1.rowset_id()));
    ASSERT_TRUE(new_rowset_meta1 != nullptr);
    ASSERT_EQ(RowsetStatePB::COMMITTED, new_rowset_meta1->rowset_state());

    // Test: update and get rowset meta
    rowset_meta1.set_rowset_state(RowsetStatePB::VISIBLE);
    ASSERT_OK(metastore->update_rowset_meta(rowset_meta1));
    ASSIGN_OR_ABORT(auto new_rowset_meta2,
                    metastore->get_rowset_meta(rowset_meta1.tablet_uid(), rowset_meta1.rowset_id()));
    ASSERT_TRUE(new_rowset_meta2 != nullptr);
    ASSERT_EQ(RowsetStatePB::VISIBLE, new_rowset_meta2->rowset_state());

    // Create another new RowsetMeta
    // s3://starrocks-env-s3-unit-test/tablet_object_metastore_test/rst_00...01-00...01_020000000000000200...
    RowsetMeta rowset_meta2;
    rowset_meta2.set_tablet_uid(TabletUid(1, 1));
    rowset_meta2.set_start_version(2);
    rowset_meta2.set_end_version(2);
    rowset_meta2.set_num_rows(1000);
    rowset_meta2.set_num_segments(1);
    rowset_meta2.set_data_disk_size(1024);
    rowset_meta2.set_rowset_seg_id(1);
    RowsetId rowsetId2;
    rowsetId2.init(2, 2, 0, 0);
    rowset_meta2.set_rowset_id(rowsetId2);
    rowset_meta2.set_rowset_state(RowsetStatePB::COMMITTED);

    // Test: add and get rowset metas
    ASSERT_OK(metastore->add_rowset_meta(rowset_meta2));
    ASSIGN_OR_ABORT(auto new_rowset_metas,
                    metastore->get_rowset_metas(rowset_meta2.tablet_uid()));
    ASSERT_EQ(2, new_rowset_metas.size());
    ASSERT_EQ(RowsetStatePB::VISIBLE, new_rowset_metas[0]->rowset_state());
    ASSERT_EQ(RowsetStatePB::COMMITTED, new_rowset_metas[1]->rowset_state());

    // Test: remove rowset meta
    ASSERT_OK(metastore->remove_rowset_meta(rowset_meta1.tablet_uid(), rowset_meta1.rowset_id()));
    ASSERT_OK(metastore->remove_rowset_meta(rowset_meta2.tablet_uid(), rowset_meta2.rowset_id()));
    ASSIGN_OR_ABORT(auto new_rowset_metas2,
                    metastore->get_rowset_metas(rowset_meta2.tablet_uid()));
    ASSERT_EQ(0, new_rowset_metas2.size());
}

} // namespace starrocks