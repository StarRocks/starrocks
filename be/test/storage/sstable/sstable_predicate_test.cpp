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

#include "storage/sstable/sstable_predicate.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/record_predicate/column_hash_is_congruent.h"
#include "storage/record_predicate/record_predicate_helper.h"
#include "storage/tablet_schema.h"

namespace starrocks {

namespace sstable {
class SstablePredicateTest : public ::testing::Test {
public:
    void SetUp() override {
        _schema_pb_1.set_keys_type(PRIMARY_KEYS);
        _schema_pb_1.set_num_short_key_columns(1);
        _schema_pb_1.set_num_rows_per_row_block(5);
        _schema_pb_1.set_next_column_unique_id(2);

        ColumnPB& col = *(_schema_pb_1.add_column());
        col.set_unique_id(1);
        col.set_name("col1");
        col.set_type("INT");
        col.set_is_key(true);
        col.set_is_nullable(false);
        col.set_default_value("1");

        ColumnPB& col2 = *(_schema_pb_1.add_column());
        col2.set_unique_id(2);
        col2.set_name("col2");
        col2.set_type("INT");
        col2.set_is_key(false);
        col2.set_is_nullable(false);
        col2.set_default_value("2");

        _schema_pb_2.set_keys_type(PRIMARY_KEYS);
        _schema_pb_2.set_num_short_key_columns(1);
        _schema_pb_2.set_num_rows_per_row_block(5);
        _schema_pb_2.set_next_column_unique_id(2);

        ColumnPB& col3 = *(_schema_pb_2.add_column());
        col3.set_unique_id(1);
        col3.set_name("col1");
        col3.set_type("INT");
        col3.set_is_key(true);
        col3.set_is_nullable(false);
        col3.set_default_value("1");

        ColumnPB& col4 = *(_schema_pb_2.add_column());
        col4.set_unique_id(2);
        col4.set_name("col2");
        col4.set_type("INT");
        col4.set_is_key(true);
        col4.set_is_nullable(false);
        col4.set_default_value("2");
    }

private:
    TabletSchemaPB _schema_pb_1;
    TabletSchemaPB _schema_pb_2;
};

TEST_F(SstablePredicateTest, basicSstablePredicateTest) {
    {
        PersistentIndexSstablePredicatePB sstable_predicate_pb;
        auto record_predicate_pb = sstable_predicate_pb.mutable_record_predicate();
        record_predicate_pb->set_type(RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT);
        auto column_hash_is_congruent_pb = record_predicate_pb->mutable_column_hash_is_congruent();
        column_hash_is_congruent_pb->set_modulus(2);
        column_hash_is_congruent_pb->set_remainder(0);
        column_hash_is_congruent_pb->add_column_names("col1");
        ASSIGN_OR_ABORT(auto sstable_predicate, SstablePredicate::create(_schema_pb_1, sstable_predicate_pb));

        std::shared_ptr<TabletSchema> tablet_schema_1 = std::make_shared<TabletSchema>(_schema_pb_1);
        ASSERT_EQ(tablet_schema_1->num_key_columns(), 1);
        std::vector<ColumnId> pk_columns(tablet_schema_1->num_key_columns());
        for (auto i = 0; i < tablet_schema_1->num_key_columns(); i++) {
            pk_columns[i] = (ColumnId)i;
        }
        auto pkey_schema = ChunkHelper::convert_schema(tablet_schema_1, pk_columns);
        auto pk_chunk = ChunkHelper::new_chunk(pkey_schema, 1);
        pk_chunk->get_column_raw_ptr_by_name("col1")->append_datum(Datum(12345));
        ASSERT_EQ(pk_chunk->num_rows(), 1);

        uint32_t hashes = 0;
        pk_chunk->get_column_by_name("col1")->crc32_hash(&(hashes), 0, 1);
        bool expected = (hashes % 2 == 0);

        MutableColumnPtr encoded_columns;
        ASSERT_OK(PrimaryKeyEncoder::create_column(pkey_schema, &encoded_columns));
        PrimaryKeyEncoder::encode(pkey_schema, *pk_chunk, 0, 1, encoded_columns.get());
        auto key_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);
        Slice key(encoded_columns->continuous_data(), key_size);
        std::string row = key.to_string();

        uint8_t selection;
        ASSERT_OK(sstable_predicate->evaluate(row, &selection));
        ASSERT_EQ(selection, expected);
    }

    {
        PersistentIndexSstablePredicatePB sstable_predicate_pb;
        auto record_predicate_pb = sstable_predicate_pb.mutable_record_predicate();
        record_predicate_pb->set_type(RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT);
        auto column_hash_is_congruent_pb = record_predicate_pb->mutable_column_hash_is_congruent();
        column_hash_is_congruent_pb->set_modulus(2);
        column_hash_is_congruent_pb->set_remainder(0);
        column_hash_is_congruent_pb->add_column_names("col1");
        column_hash_is_congruent_pb->add_column_names("col2");
        ASSIGN_OR_ABORT(auto sstable_predicate, SstablePredicate::create(_schema_pb_2, sstable_predicate_pb));

        std::shared_ptr<TabletSchema> tablet_schema_2 = std::make_shared<TabletSchema>(_schema_pb_2);
        ASSERT_EQ(tablet_schema_2->num_key_columns(), 2);
        std::vector<ColumnId> pk_columns(tablet_schema_2->num_key_columns());
        for (auto i = 0; i < tablet_schema_2->num_key_columns(); i++) {
            pk_columns[i] = (ColumnId)i;
        }
        auto pkey_schema = ChunkHelper::convert_schema(tablet_schema_2, pk_columns);
        auto pk_chunk = ChunkHelper::new_chunk(pkey_schema, 1);
        pk_chunk->get_column_raw_ptr_by_name("col1")->append_datum(Datum(12345));
        pk_chunk->get_column_raw_ptr_by_name("col2")->append_datum(Datum(66666));
        ASSERT_EQ(pk_chunk->num_rows(), 1);

        uint32_t hashes = 0;
        pk_chunk->get_column_by_name("col1")->crc32_hash(&(hashes), 0, 1);
        pk_chunk->get_column_by_name("col2")->crc32_hash(&(hashes), 0, 1);
        bool expected = (hashes % 2 == 0);

        MutableColumnPtr encoded_columns;
        ASSERT_OK(PrimaryKeyEncoder::create_column(pkey_schema, &encoded_columns));
        PrimaryKeyEncoder::encode(pkey_schema, *pk_chunk, 0, 1, encoded_columns.get());
        ASSERT_TRUE(encoded_columns->is_binary() || encoded_columns->is_large_binary());
        std::string row = reinterpret_cast<const Slice*>(encoded_columns->raw_data())->to_string();

        uint8_t selection;
        ASSERT_OK(sstable_predicate->evaluate(row, &selection));
        ASSERT_EQ(selection, expected);
    }
}

} // namespace sstable
} // namespace starrocks
