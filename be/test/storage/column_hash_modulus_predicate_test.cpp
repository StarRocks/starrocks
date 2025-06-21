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

#include "storage/column_hash_modulus_predicate.h"

#include <gtest/gtest.h>

#include "storage/chunk_helper.h"
#include "testutil/assert.h"

namespace starrocks {
class ColumnHashModulusPredicateTest : public ::testing::Test {
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

        ColumnPB& col2 = *(schema_pb.add_column());
        col2.set_unique_id(2);
        col2.set_name("col2");
        col2.set_type("INT");
        col2.set_is_key(false);
        col2.set_is_nullable(false);

        _table_schema = std::make_shared<TabletSchema>(schema_pb);
    }

private:
    std::shared_ptr<TabletSchema> _table_schema;
};

TEST_F(ColumnHashModulusPredicateTest, basic) {
    auto chunk = ChunkHelper::new_chunk(*_table_schema->schema(), 2);
    ColumnHashModulusPredicate pred;
    ASSERT_ERROR(pred.evaluate(chunk.get(), nullptr, 0, 0));

    ColumnHashModulusPredicatePB column_hash_modulus_predicate_pb;
    column_hash_modulus_predicate_pb.set_type(ColumnHashModulusPredicatePB::UNKNOWN);
    ASSERT_ERROR(ColumnHashModulusPredicate::create(column_hash_modulus_predicate_pb));

    column_hash_modulus_predicate_pb.set_type(ColumnHashModulusPredicatePB::CRC32);
    ASSERT_ERROR(ColumnHashModulusPredicate::create(column_hash_modulus_predicate_pb));

    column_hash_modulus_predicate_pb.add_hash_column_names("");
    ASSERT_ERROR(ColumnHashModulusPredicate::create(column_hash_modulus_predicate_pb));

    column_hash_modulus_predicate_pb.clear_hash_column_names();
    column_hash_modulus_predicate_pb.add_hash_column_names("not_existed_col_name");
    column_hash_modulus_predicate_pb.set_modulus(-1);
    column_hash_modulus_predicate_pb.set_remainder(-1);
    ASSERT_ERROR(ColumnHashModulusPredicate::create(column_hash_modulus_predicate_pb));
    column_hash_modulus_predicate_pb.set_modulus(2);
    column_hash_modulus_predicate_pb.set_remainder(0);

    ASSIGN_OR_ABORT(auto predicate, ColumnHashModulusPredicate::create(column_hash_modulus_predicate_pb));
    ASSERT_ERROR(predicate.evaluate(chunk.get(), nullptr, 0, 0));
}

} // namespace starrocks
