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

#pragma once

#include <gtest/gtest.h>

#include <filesystem>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/global_types.h"
#include "common/logging.h"
#include "exec/hdfs_scanner.h"
#include "exprs/binary_predicate.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/page_reader.h"
#include "fs/fs.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"

namespace starrocks::parquet {

class Utils {
public:
    struct SlotDesc {
        std::string name;
        TypeDescriptor type;
        SlotId id = -1;
    };

    static TupleDescriptor* create_tuple_descriptor(RuntimeState* state, ObjectPool* pool, const SlotDesc* slot_descs) {
        TDescriptorTableBuilder table_desc_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (int i = 0;; i++) {
            if (slot_descs[i].name == "") {
                break;
            }
            TSlotDescriptorBuilder b2;
            b2.column_name(slot_descs[i].name).type(slot_descs[i].type).id(slot_descs[i].id).nullable(true);
            tuple_desc_builder.add_slot(b2.build());
        }
        tuple_desc_builder.build(&table_desc_builder);

        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        std::vector<bool> nullable_tuples = std::vector<bool>{true};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(state, pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size).ok());

        RowDescriptor* row_desc = pool->add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
        return row_desc->tuple_descriptors()[0];
    }

    static void make_column_info_vector(const TupleDescriptor* tuple_desc,
                                        std::vector<HdfsScannerContext::ColumnInfo>* columns) {
        columns->clear();
        for (int i = 0; i < tuple_desc->slots().size(); i++) {
            SlotDescriptor* slot = tuple_desc->slots()[i];
            HdfsScannerContext::ColumnInfo c;
            c.col_name = slot->col_name();
            c.col_idx = i;
            c.slot_id = slot->id();
            c.col_type = slot->type();
            c.slot_desc = slot;
            columns->emplace_back(c);
        }
    }

    static void assert_equal_chunk(const Chunk* expected, const Chunk* actual) {
        if (expected->debug_columns() != actual->debug_columns()) {
            std::cout << expected->debug_columns() << std::endl;
            std::cout << actual->debug_columns() << std::endl;
        }
        ASSERT_EQ(expected->debug_columns(), actual->debug_columns());
        for (size_t i = 0; i < expected->num_columns(); i++) {
            const auto& expected_col = expected->get_column_by_index(i);
            const auto& actual_col = actual->get_column_by_index(i);
            if (expected_col->debug_string() != actual_col->debug_string()) {
                std::cout << expected_col->debug_string() << std::endl;
                std::cout << actual_col->debug_string() << std::endl;
            }
            ASSERT_EQ(expected_col->debug_string(), actual_col->debug_string());
        }
    }
};

} // namespace starrocks::parquet
