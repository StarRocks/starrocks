// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/convert_scan_node.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/es_http_scan_node.h"
#include "exprs/slot_ref.h"
#include "runtime/date_value.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/timestamp_value.h"
#include "runtime/tuple_row.h"

namespace starrocks {

class ConvertScanNodeTest : public testing::Test {
public:
    ConvertScanNodeTest() {}
    virtual ~ConvertScanNodeTest() {}
    void SetUp() override {}

    std::shared_ptr<RuntimeState> create_runtime_state() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        return std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
    }
};

TDescriptorTable create_full_descriptor_table() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("c2").column_pos(1).nullable(false).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder().string_type(64).column_name("c3").column_pos(2).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder().decimal_v2_type(5, 2).column_name("c4").column_pos(3).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_DATE).column_name("c1").column_pos(0).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_DATETIME).column_name("c1").column_pos(0).build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

TEST_F(ConvertScanNodeTest, convert_row_batch_to_chunk) {
    auto tdesc_tbl = create_full_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);

    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);

    TPlanNode tnode;
    tnode.row_tuples.push_back(0);
    tnode.nullable_tuples.push_back(false);

    auto runtime_state = create_runtime_state();
    runtime_state->init_instance_mem_tracker();
    runtime_state->set_desc_tbl(desc_tbl);

    EsHttpScanNode scan(&obj_pool, tnode, *desc_tbl);
    scan.prepare(runtime_state.get());

    RowBatch row_batch(row_desc, 1024, runtime_state->instance_mem_tracker());
    // row1
    {
        auto id = row_batch.add_row();
        auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        row_batch.get_row(id)->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());
        *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 987654;
        *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 1234567899876;

        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(tuple_desc->slots()[2]->tuple_offset()));
        str_val->ptr = (char*)row_batch.tuple_data_pool()->allocate(10);
        str_val->len = 3;
        memcpy(str_val->ptr, "abc", str_val->len);

        DecimalV2Value* dec_val =
                reinterpret_cast<DecimalV2Value*>(tuple->get_slot(tuple_desc->slots()[3]->tuple_offset()));
        *dec_val = DecimalV2Value("123.123456789");

        DateTimeValue* date_val =
                reinterpret_cast<DateTimeValue*>(tuple->get_slot(tuple_desc->slots()[4]->tuple_offset()));
        date_val->from_date_str("2022-01-01", 10);

        DateTimeValue* datetime_val =
                reinterpret_cast<DateTimeValue*>(tuple->get_slot(tuple_desc->slots()[5]->tuple_offset()));
        datetime_val->from_date_str("2022-01-01 19:39:43", 19);

        row_batch.commit_last_row();
    }

    // row2
    {
        auto id = row_batch.add_row();
        auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        row_batch.get_row(id)->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());
        *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 12345678;
        *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 9876567899876;
        tuple->set_null(tuple_desc->slots()[2]->null_indicator_offset());

        DecimalV2Value* dec_val =
                reinterpret_cast<DecimalV2Value*>(tuple->get_slot(tuple_desc->slots()[3]->tuple_offset()));
        *dec_val = DecimalV2Value("12.3");

        DateTimeValue* date_val =
                reinterpret_cast<DateTimeValue*>(tuple->get_slot(tuple_desc->slots()[4]->tuple_offset()));
        date_val->from_date_str("1997-01-01", 10);

        DateTimeValue* datetime_val =
                reinterpret_cast<DateTimeValue*>(tuple->get_slot(tuple_desc->slots()[5]->tuple_offset()));
        datetime_val->from_date_str("1997-01-01 00:00:00", 19);

        row_batch.commit_last_row();
    }

    // row3
    {
        auto id = row_batch.add_row();
        auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        row_batch.get_row(id)->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());
        tuple->set_null(tuple_desc->slots()[0]->null_indicator_offset());
        *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 76543234567;

        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(tuple_desc->slots()[2]->tuple_offset()));
        str_val->ptr = (char*)row_batch.tuple_data_pool()->allocate(10);
        str_val->len = 0;
        memcpy(str_val->ptr, "", str_val->len);

        DecimalV2Value* dec_val =
                reinterpret_cast<DecimalV2Value*>(tuple->get_slot(tuple_desc->slots()[3]->tuple_offset()));
        *dec_val = DecimalV2Value("123456789123.1234");

        tuple->set_null(tuple_desc->slots()[4]->null_indicator_offset());
        tuple->set_null(tuple_desc->slots()[5]->null_indicator_offset());

        row_batch.commit_last_row();
    }
    ChunkPtr result_chunk = std::make_shared<vectorized::Chunk>();
    scan.convert_rowbatch_to_chunk(row_batch, result_chunk.get());
    ASSERT_EQ(result_chunk->num_rows(), 3);

    vectorized::ColumnPtr column1 = result_chunk->get_column_by_slot_id(0);
    ASSERT_TRUE(column1->is_nullable());
    ASSERT_EQ(column1->get(0).get_int32(), 987654);
    ASSERT_EQ(column1->get(1).get_int32(), 12345678);
    ASSERT_TRUE(column1->is_null(2));

    vectorized::ColumnPtr column2 = result_chunk->get_column_by_slot_id(1);
    ASSERT_FALSE(column2->is_nullable());
    ASSERT_EQ(column2->get(0).get_int64(), 1234567899876);
    ASSERT_EQ(column2->get(1).get_int64(), 9876567899876);
    ASSERT_EQ(column2->get(2).get_int64(), 76543234567);

    vectorized::ColumnPtr column3 = result_chunk->get_column_by_slot_id(2);
    ASSERT_TRUE(column3->is_nullable());
    ASSERT_EQ(column3->get(0).get_slice().to_string(), "abc");
    ASSERT_TRUE(column3->is_null(1));
    ASSERT_EQ(column3->get(2).get_slice().to_string(), "");

    vectorized::ColumnPtr column4 = result_chunk->get_column_by_slot_id(3);
    ASSERT_TRUE(column4->is_nullable());
    ASSERT_EQ(column4->get(0).get_decimal(), DecimalV2Value("123.123456789"));
    ASSERT_EQ(column4->get(1).get_decimal(), DecimalV2Value("12.3"));
    ASSERT_EQ(column4->get(2).get_decimal(), DecimalV2Value("123456789123.1234"));

    vectorized::ColumnPtr column5 = result_chunk->get_column_by_slot_id(4);
    ASSERT_TRUE(column5->is_nullable());
    ASSERT_EQ(column5->get(0).get_date(), vectorized::DateValue::create(2022, 01, 01));
    ASSERT_EQ(column5->get(1).get_date(), vectorized::DateValue::create(1997, 01, 01));
    ASSERT_TRUE(column5->is_null(2));

    vectorized::ColumnPtr column6 = result_chunk->get_column_by_slot_id(5);
    ASSERT_TRUE(column6->is_nullable());
    ASSERT_EQ(column6->get(0).get_timestamp(), vectorized::TimestampValue::create(2022, 01, 01, 19, 39, 43));
    ASSERT_EQ(column6->get(1).get_timestamp(), vectorized::TimestampValue::create(1997, 01, 01, 0, 0, 0));
    ASSERT_TRUE(column6->is_null(2));
}

} // namespace starrocks