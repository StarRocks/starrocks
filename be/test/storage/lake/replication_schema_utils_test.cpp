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

#include "storage/lake/replication_schema_utils.h"

#include <gtest/gtest.h>

#include <climits>

#include "base/testutil/assert.h"

namespace starrocks::lake {
namespace {
ColumnPB* add_column(TabletSchemaPB* schema, int uid, const std::string& name, bool key = false) {
    auto* column = schema->add_column();
    column->set_unique_id(uid);
    column->set_name(name);
    column->set_type("INT");
    column->set_is_key(key);
    column->set_is_nullable(!key);
    return column;
}

TabletMetadataPB source_metadata() {
    TabletMetadataPB source;
    source.set_id(100);
    auto* current = source.mutable_schema();
    current->set_id(103);
    current->set_keys_type(DUP_KEYS);
    current->set_num_short_key_columns(1);
    add_column(current, 0, "k", true);
    add_column(current, 2, "stable");
    add_column(current, 4, "added");
    auto& old = (*source.mutable_historical_schemas())[101];
    old.CopyFrom(*current);
    old.set_id(101);
    old.mutable_column(2)->set_unique_id(1); // Dropped and re-added under the same name.
    add_column(&old, 3, "dropped");
    source.add_rowsets()->set_id(1);
    source.add_rowsets()->set_id(2);
    (*source.mutable_rowset_to_schema())[1] = 101;
    (*source.mutable_rowset_to_schema())[2] = 103;
    return source;
}

TabletMetadataPB target_metadata(const TabletMetadataPB& source) {
    TabletMetadataPB target;
    target.set_id(200);
    target.mutable_schema()->CopyFrom(source.schema());
    target.mutable_schema()->set_id(201);
    target.mutable_schema()->mutable_column(1)->set_unique_id(1);
    target.mutable_schema()->mutable_column(2)->set_unique_id(2);
    return target;
}
} // namespace

class ReplicationSchemaUtilsTest : public testing::Test {
protected:
    TabletMetadataPB source = source_metadata();
    TabletMetadataPB target = target_metadata(source);
    std::unordered_map<uint32_t, uint32_t> mapping;
    std::unordered_set<uint32_t> dropped_uids;
    Status build() { return build_light_schema_change_uid_map(source, target, &mapping, &dropped_uids); }
    TabletSchemaPB* history() { return &(*source.mutable_historical_schemas())[101]; }
};

TEST_F(ReplicationSchemaUtilsTest, FreshTargetAcceptsAddDropAndSameNameReadd) {
    ASSERT_TRUE(target.historical_schemas().empty());
    ASSERT_OK(build());
    EXPECT_EQ(0, mapping.at(0));
    EXPECT_EQ(1, mapping.at(2));
    EXPECT_EQ(2, mapping.at(4));
    EXPECT_FALSE(mapping.contains(1));
    EXPECT_FALSE(mapping.contains(3));
    EXPECT_EQ((std::unordered_set<uint32_t>{1, 3}), dropped_uids);
    const auto first = mapping;
    ASSERT_OK(build());
    EXPECT_EQ(first, mapping);
}

TEST_F(ReplicationSchemaUtilsTest, IdentityCurrentIdsDoNotDisableRetiredColumnIsolation) {
    target.mutable_schema()->mutable_column(1)->set_unique_id(2);
    target.mutable_schema()->mutable_column(2)->set_unique_id(4);
    ASSERT_OK(build());
    EXPECT_EQ(2, mapping.at(2));
    EXPECT_EQ(4, mapping.at(4));
    EXPECT_EQ(3, mapping.size());
    EXPECT_EQ((std::unordered_set<uint32_t>{1, 3}), dropped_uids);
}

TEST_F(ReplicationSchemaUtilsTest, DroppedColumnsDoNotAllocateIdsFromTargetHistory) {
    auto* reserved = (*target.mutable_historical_schemas())[199].add_column();
    reserved->set_unique_id(INT_MAX - 1);
    ASSERT_OK(build());
    EXPECT_FALSE(mapping.contains(1));
    EXPECT_FALSE(mapping.contains(3));
    EXPECT_EQ((std::unordered_set<uint32_t>{1, 3}), dropped_uids);
}

TEST_F(ReplicationSchemaUtilsTest, NonNullAddedColumnRequiresDefault) {
    source.mutable_schema()->mutable_column(2)->set_is_nullable(false);
    target.mutable_schema()->mutable_column(2)->set_is_nullable(false);
    EXPECT_TRUE(build().is_not_supported());
    source.mutable_schema()->mutable_column(2)->set_default_value("7");
    target.mutable_schema()->mutable_column(2)->set_default_value("7");
    ASSERT_OK(build());
}

TEST_F(ReplicationSchemaUtilsTest, ScalarStorageTypeNamesIncludeDecimalV2AndVarbinary) {
    for (const auto* type : {"BOOLEAN", "TINYINT", "SMALLINT",   "INT",       "BIGINT",    "LARGEINT",   "FLOAT",
                             "DOUBLE",  "DECIMAL", "DECIMAL_V2", "DECIMAL32", "DECIMAL64", "DECIMAL128", "CHAR",
                             "VARCHAR", "DATE",    "DATE_V2",    "DATETIME",  "TIMESTAMP", "VARBINARY"}) {
        source.mutable_schema()->mutable_column(2)->set_type(type);
        target.mutable_schema()->mutable_column(2)->set_type(type);
        ASSERT_OK(build());
    }
}

TEST_F(ReplicationSchemaUtilsTest, AllKeyModelsKeepSameContract) {
    for (auto keys : {DUP_KEYS, PRIMARY_KEYS, AGG_KEYS, UNIQUE_KEYS}) {
        source.mutable_schema()->set_keys_type(keys);
        history()->set_keys_type(keys);
        target.mutable_schema()->set_keys_type(keys);
        ASSERT_OK(build());
    }
}

TEST_F(ReplicationSchemaUtilsTest, MissingOrMismatchedReferencedSchemaIsCorruption) {
    history()->set_id(999);
    EXPECT_TRUE(build().is_corruption());
    source.mutable_historical_schemas()->erase(101);
    EXPECT_TRUE(build().is_corruption());
}

TEST_F(ReplicationSchemaUtilsTest, UnreferencedHistoryAndMappingsAreIgnored) {
    (*source.mutable_rowset_to_schema())[999] = 888;
    (*source.mutable_historical_schemas())[888];
    ASSERT_OK(build());
}

TEST_F(ReplicationSchemaUtilsTest, CurrentSchemaHistoryConflictIsCorruption) {
    (*source.mutable_historical_schemas())[103].CopyFrom(source.schema());
    (*source.mutable_historical_schemas())[103].mutable_column(1)->set_type("BIGINT");
    EXPECT_TRUE(build().is_corruption());
}

TEST_F(ReplicationSchemaUtilsTest, DuplicateColumnIdentityIsCorruption) {
    history()->mutable_column(3)->set_unique_id(2);
    EXPECT_TRUE(build().is_corruption());
    history()->mutable_column(3)->set_unique_id(3);
    history()->mutable_column(3)->set_name("stable");
    EXPECT_TRUE(build().is_corruption());
}

TEST_F(ReplicationSchemaUtilsTest, MissingRowsetIdentityOrEmptyColumnTypeIsCorruption) {
    source.mutable_rowsets(0)->clear_id();
    EXPECT_TRUE(build().is_corruption());
    source.mutable_rowsets(0)->set_id(1);
    source.mutable_schema()->mutable_column(1)->set_type("");
    target.mutable_schema()->mutable_column(1)->set_type("");
    EXPECT_TRUE(build().is_corruption());
}

TEST_F(ReplicationSchemaUtilsTest, DuplicateRowsetsAreCorruption) {
    source.mutable_rowsets(1)->set_id(1);
    EXPECT_TRUE(build().is_corruption());
}

TEST_F(ReplicationSchemaUtilsTest, TypeChangeIsRejected) {
    history()->mutable_column(1)->set_type("BIGINT");
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, DefaultChangeIsRejected) {
    history()->mutable_column(1)->set_default_value("7");
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, NullableChangeIsRejected) {
    history()->mutable_column(1)->set_is_nullable(false);
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, RenameWithSameUidIsRejected) {
    history()->mutable_column(1)->set_name("old_stable");
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, KeyDropOrReaddIsRejected) {
    history()->mutable_column(0)->set_unique_id(9);
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, SortKeyChangeIsRejected) {
    history()->add_sort_key_unique_ids(2);
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, SourceTargetCurrentMismatchIsRejected) {
    target.mutable_schema()->mutable_column(1)->set_type("BIGINT");
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, MultipleHistoryGenerationsAndUidConflicts) {
    auto& older = (*source.mutable_historical_schemas())[99];
    older.CopyFrom(*history());
    older.set_id(99);
    source.add_rowsets()->set_id(3);
    (*source.mutable_rowset_to_schema())[3] = 99;
    ASSERT_OK(build());
    older.mutable_column(3)->set_type("BIGINT");
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, UnchangedComplexColumnIsAllowedButNestedUidRemapIsNot) {
    for (auto* schema : {source.mutable_schema(), target.mutable_schema(), history()}) {
        auto* column = schema->mutable_column(1);
        column->set_type("STRUCT");
        auto* child = column->add_children_columns();
        child->set_unique_id(10);
        child->set_name("x");
        child->set_type("INT");
    }
    ASSERT_OK(build());
    target.mutable_schema()->mutable_column(1)->mutable_children_columns(0)->set_unique_id(11);
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, ComplexColumnAddDropIsRejected) {
    history()->mutable_column(3)->set_type("STRUCT");
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, UnknownSchemaFieldIsRejected) {
    history()->GetReflection()->MutableUnknownFields(history())->AddVarint(777, 1);
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, ExternalIndexesAndBundledSegmentsAreRejected) {
    auto* index = source.mutable_schema()->add_table_indices();
    index->set_index_type(VECTOR);
    index->add_col_unique_id(2);
    EXPECT_TRUE(build().is_not_supported());
    source.mutable_schema()->clear_table_indices();
    source.mutable_rowsets(0)->add_segment_metas()->set_bundle_file_offset(0);
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, StableEmbeddedIndexDoesNotRequireSameIndexId) {
    int id = 1;
    for (auto* schema : {source.mutable_schema(), target.mutable_schema(), history()}) {
        auto* index = schema->add_table_indices();
        index->set_index_id(id++);
        index->set_index_name("idx_stable");
        index->set_index_type(BITMAP);
        index->add_col_unique_id(schema->column(1).unique_id());
    }
    ASSERT_OK(build());
}

TEST_F(ReplicationSchemaUtilsTest, DeletePredicateMustReferenceStableColumnIdentity) {
    auto* predicate = source.mutable_rowsets(0)->mutable_delete_predicate();
    predicate->set_version(1);
    auto* binary = predicate->add_binary_predicates();
    binary->set_column_name("stable");
    binary->set_op("=");
    binary->set_value("5");
    ASSERT_OK(build());
    binary->set_column_name("added");
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, NormalizationDropsDestinationHistoryAndPinsTargetSchema) {
    target.add_rowsets()->set_id(1);
    target.add_rowsets()->set_id(2);
    (*target.mutable_rowset_to_schema())[999] = 500;
    (*target.mutable_historical_schemas())[500].CopyFrom(*history());
    normalize_replication_rowset_schemas(&target);
    EXPECT_EQ(2, target.rowset_to_schema_size());
    EXPECT_EQ(1, target.historical_schemas_size());
    EXPECT_EQ(target.schema().id(), target.rowset_to_schema().at(1));
    TxnLogPB::OpReplication op;
    op.set_normalize_rowset_schema(true);
    op.mutable_tablet_metadata()->CopyFrom(target);
    ASSERT_OK(validate_normalized_replication_schema(op, target));
    target.mutable_schema()->set_schema_version(100);
    EXPECT_TRUE(validate_normalized_replication_schema(op, target).is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, MalformedNormalizedLogFailsButLegacyLogIsUnchanged) {
    TxnLogPB::OpReplication op;
    ASSERT_OK(validate_normalized_replication_schema(op, target));
    op.set_normalize_rowset_schema(true);
    EXPECT_TRUE(validate_normalized_replication_schema(op, target).is_corruption());
    op.mutable_tablet_metadata()->CopyFrom(target);
    op.mutable_tablet_metadata()->add_rowsets()->set_id(1);
    normalize_replication_rowset_schemas(op.mutable_tablet_metadata());
    ASSERT_OK(validate_normalized_replication_schema(op, target));
    (*op.mutable_tablet_metadata()->mutable_rowset_to_schema())[1] = 777;
    EXPECT_TRUE(validate_normalized_replication_schema(op, target).is_corruption());
}

TEST_F(ReplicationSchemaUtilsTest, EmptySnapshotHasNoDanglingHistory) {
    normalize_replication_rowset_schemas(&target);
    EXPECT_TRUE(target.historical_schemas().empty());
    EXPECT_TRUE(target.rowset_to_schema().empty());
}
TEST_F(ReplicationSchemaUtilsTest, IndexDeltaGroupsAreRejectedEvenWhenCurrentIdsMatch) {
    source.mutable_idg_meta()->mutable_idgs()->operator[](1).add_entries()->set_index_file("external.idx");
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, DroppedIndexDefinitionsAreRejected) {
    history()->add_dropped_table_indices()->set_index_type(BITMAP);
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, SharedAndRangeSegmentsAreRejected) {
    auto* segment = source.mutable_rowsets(0)->add_segment_metas();
    segment->set_filename("shared.dat");
    segment->set_shared(true);
    EXPECT_TRUE(build().is_not_supported());
    segment->clear_shared();
    source.mutable_rowsets(0)->mutable_range();
    EXPECT_TRUE(build().is_not_supported());
    source.mutable_rowsets(0)->clear_range();
    source.mutable_range();
    EXPECT_TRUE(build().is_not_supported());
    source.clear_range();
    target.mutable_range();
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, CanonicalBundleOffsetZeroAndSyntheticOffsetsAreRejected) {
    auto* segment = source.mutable_rowsets(0)->add_segment_metas();
    segment->set_bundle_file_offset(0);
    EXPECT_TRUE(build().is_not_supported());
    segment->set_synthetic_bundle_file_offset(true);
    EXPECT_TRUE(build().is_not_supported());
}

TEST_F(ReplicationSchemaUtilsTest, SegmentVectorIndexFilesAreRejected) {
    source.mutable_rowsets(0)->add_segment_metas()->add_vector_index_ids(123);
    EXPECT_TRUE(build().is_not_supported());
}

} // namespace starrocks::lake
