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

#include "storage/record_predicate/record_predicate_helper.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "storage/chunk_helper.h"
#include "storage/tablet_schema.h"

namespace starrocks {
class RecordPredicateHelperTest : public ::testing::Test {
public:
    void SetUp() override {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(PRIMARY_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(5);
        schema_pb.set_next_column_unique_id(2);

        ColumnPB& col = *(schema_pb.add_column());
        col.set_unique_id(1);
        col.set_name("col1");
        col.set_type("INT");
        col.set_is_key(true);
        col.set_is_nullable(false);
        col.set_default_value("1");

        ColumnPB& col2 = *(schema_pb.add_column());
        col2.set_unique_id(2);
        col2.set_name("col2");
        col2.set_type("INT");
        col2.set_is_key(false);
        col2.set_is_nullable(false);
        col2.set_default_value("2");

        _table_schema = std::make_shared<const TabletSchema>(schema_pb);
    }

private:
    std::shared_ptr<const TabletSchema> _table_schema;
};

TEST_F(RecordPredicateHelperTest, basicRecordPredicateHelper) {
    auto chunk = ChunkHelper::new_chunk(*_table_schema->schema(), 2);
    RecordPredicatePB record_predicate_pb;

    record_predicate_pb.set_type(RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT);
    auto column_hash_is_congruent_pb = record_predicate_pb.mutable_column_hash_is_congruent();
    column_hash_is_congruent_pb->add_column_names("col1");
    column_hash_is_congruent_pb->set_modulus(2);
    column_hash_is_congruent_pb->set_remainder(0);
    ASSIGN_OR_ABORT(auto predicate, RecordPredicateHelper::create(record_predicate_pb));

    std::set<ColumnId> column_ids;
    ASSERT_OK(RecordPredicateHelper::check_valid_schema(*predicate, *_table_schema->schema()));

    column_ids.clear();
    ASSERT_OK(RecordPredicateHelper::get_column_ids(*predicate, _table_schema, &column_ids));
    ASSERT_TRUE(column_ids.size() == 1);

    column_ids.clear();
    ASSERT_OK(RecordPredicateHelper::get_column_ids(*predicate, *_table_schema->schema(), &column_ids));
    ASSERT_TRUE(column_ids.size() == 1);
}

} // namespace starrocks
