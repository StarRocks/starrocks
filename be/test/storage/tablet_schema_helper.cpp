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

#include "storage/tablet_schema_helper.h"

namespace starrocks {

std::unique_ptr<TabletSchema> TabletSchemaHelper::create_tablet_schema(const std::vector<ColumnPB>& columns,
                                                                       int num_short_key_columns) {
    TabletSchemaPB schema_pb;

    int num_key_columns = 0;
    for (auto& col : columns) {
        if (col.is_key()) {
            num_key_columns++;
        }
        auto* col_pb = schema_pb.add_column();
        *col_pb = col;
    }

    if (num_short_key_columns == -1) {
        schema_pb.set_num_short_key_columns(num_key_columns);
    } else {
        schema_pb.set_num_short_key_columns(num_short_key_columns);
    }
    return std::make_unique<TabletSchema>(schema_pb);
}

std::shared_ptr<TabletSchema> TabletSchemaHelper::create_tablet_schema() {
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_keys_type(DUP_KEYS);
    tablet_schema_pb.set_num_short_key_columns(2);
    tablet_schema_pb.set_num_rows_per_row_block(1024);
    tablet_schema_pb.set_next_column_unique_id(4);

    ColumnPB* column_1 = tablet_schema_pb.add_column();
    column_1->set_unique_id(1);
    column_1->set_name("k1");
    column_1->set_type("INT");
    column_1->set_is_key(true);
    column_1->set_length(4);
    column_1->set_index_length(4);
    column_1->set_is_nullable(true);
    column_1->set_is_bf_column(false);

    ColumnPB* column_2 = tablet_schema_pb.add_column();
    column_2->set_unique_id(2);
    column_2->set_name("k2");
    column_2->set_type("INT"); // TODO change to varchar(20) when dict encoding for string is supported
    column_2->set_length(4);
    column_2->set_index_length(4);
    column_2->set_is_nullable(true);
    column_2->set_is_key(true);
    column_2->set_is_nullable(true);
    column_2->set_is_bf_column(false);

    ColumnPB* column_3 = tablet_schema_pb.add_column();
    column_3->set_unique_id(3);
    column_3->set_name("v1");
    column_3->set_type("INT");
    column_3->set_length(4);
    column_3->set_is_key(false);
    column_3->set_is_nullable(false);
    column_3->set_is_bf_column(false);
    column_3->set_aggregation("SUM");

    return std::make_shared<TabletSchema>(tablet_schema_pb);
}

} // namespace starrocks
