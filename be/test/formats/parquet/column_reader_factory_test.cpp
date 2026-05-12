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

#include "formats/parquet/column_reader_factory.h"

#include <gtest/gtest.h>

#include <sstream>

#include "base/testutil/assert.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/complex_column_reader.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {

namespace {

tparquet::ColumnChunk make_column_chunk(int idx) {
    tparquet::ColumnChunk chunk;
    chunk.__set_file_path("col" + std::to_string(idx));
    chunk.file_offset = 0;
    chunk.meta_data.data_page_offset = 4;
    return chunk;
}

ParquetField make_scalar_field(const std::string& name, int idx, tparquet::Type::type physical_type) {
    ParquetField f;
    f.name = name;
    f.type = ColumnType::SCALAR;
    f.physical_type = physical_type;
    f.physical_column_index = idx;
    return f;
}

ParquetField make_shredded_scalar_node(const std::string& name, int value_idx, int typed_idx,
                                       tparquet::Type::type typed_physical) {
    ParquetField node;
    node.name = name;
    node.type = ColumnType::STRUCT;
    node.children.emplace_back(make_scalar_field("value", value_idx, tparquet::Type::BYTE_ARRAY));
    node.children.emplace_back(make_scalar_field("typed_value", typed_idx, typed_physical));
    return node;
}

ParquetField make_variant_field_with_typed_group(const std::vector<ParquetField>& typed_children) {
    ParquetField variant;
    variant.name = "col_variant";
    variant.type = ColumnType::STRUCT;
    variant.children.emplace_back(make_scalar_field("metadata", 0, tparquet::Type::BYTE_ARRAY));
    variant.children.emplace_back(make_scalar_field("value", 1, tparquet::Type::BYTE_ARRAY));
    ParquetField typed_group;
    typed_group.name = "typed_value";
    typed_group.type = ColumnType::STRUCT;
    typed_group.children = typed_children;
    variant.children.emplace_back(std::move(typed_group));
    return variant;
}

ParquetField make_array_field(const std::string& name, const ParquetField& element_field) {
    ParquetField typed_value;
    typed_value.name = "typed_value";
    typed_value.type = ColumnType::ARRAY;
    typed_value.children = {element_field};

    ParquetField node;
    node.name = name;
    node.type = ColumnType::STRUCT;
    node.children.emplace_back(make_scalar_field("value", -1, tparquet::Type::BYTE_ARRAY));
    node.children.emplace_back(std::move(typed_value));
    return node;
}

ParquetField make_shredded_object_node_with_nested_scalar(const std::string& name, int value_idx, int nested_value_idx,
                                                          int nested_typed_idx,
                                                          tparquet::Type::type nested_typed_physical) {
    ParquetField node;
    node.name = name;
    node.type = ColumnType::STRUCT;
    node.children.emplace_back(make_scalar_field("value", value_idx, tparquet::Type::BYTE_ARRAY));

    ParquetField typed_group;
    typed_group.name = "typed_value";
    typed_group.type = ColumnType::STRUCT;
    typed_group.children.emplace_back(
            make_shredded_scalar_node("k", nested_value_idx, nested_typed_idx, nested_typed_physical));
    node.children.emplace_back(std::move(typed_group));
    return node;
}

ColumnReaderOptions make_opts_with_num_cols(int num_cols) {
    static tparquet::RowGroup rg;
    rg.columns.clear();
    for (int i = 0; i < num_cols; ++i) {
        rg.columns.emplace_back(make_column_chunk(i));
    }
    rg.__set_num_rows(0);

    ColumnReaderOptions opts;
    opts.row_group_meta = &rg;
    return opts;
}

} // namespace

TEST(ColumnReaderFactoryTest, VariantFallbackFieldIndexOutOfRange) {
    auto bad_node = make_shredded_scalar_node("bad", 100, 3, tparquet::Type::INT32);
    ParquetField variant = make_variant_field_with_typed_group({bad_node});
    auto opts = make_opts_with_num_cols(4);

    auto st = ColumnReaderFactory::create_variant_column_reader(opts, &variant, {});
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.status().is_invalid_argument()) << st.status().to_string();
}

TEST(ColumnReaderFactoryTest, VariantObjectArrayRewritesToElementValueReader) {
    ParquetField event_type = make_shredded_scalar_node("type", 4, 5, tparquet::Type::BYTE_ARRAY);
    ParquetField event_count = make_shredded_scalar_node("count", 6, 7, tparquet::Type::INT32);

    ParquetField element_typed_value;
    element_typed_value.name = "typed_value";
    element_typed_value.type = ColumnType::STRUCT;
    element_typed_value.children = {event_type, event_count};

    ParquetField element_value = make_scalar_field("value", 3, tparquet::Type::BYTE_ARRAY);

    ParquetField element_struct;
    element_struct.name = "element";
    element_struct.type = ColumnType::STRUCT;
    element_struct.children = {element_value, element_typed_value};

    ParquetField events_node = make_array_field("events", element_struct);
    events_node.children[0].physical_column_index = 2;

    ParquetField variant = make_variant_field_with_typed_group({events_node});
    auto opts = make_opts_with_num_cols(8);

    auto st = ColumnReaderFactory::create_variant_column_reader(opts, &variant, {});
    ASSERT_TRUE(st.ok()) << st.status().to_string();

    auto* reader = dynamic_cast<VariantColumnReader*>(st.value().get());
    ASSERT_NE(reader, nullptr);
    ASSERT_EQ(1, reader->_shredded_fields.size());

    const ShreddedFieldNode& node = reader->_shredded_fields[0];
    ASSERT_EQ(ShreddedFieldNode::Kind::ARRAY, node.kind);
    ASSERT_FALSE(node.scalar_array_layout);
    ASSERT_FALSE(node.children.empty());
    ASSERT_NE(node.typed_value_reader, nullptr);
    ASSERT_EQ(nullptr, node.array_element_value_reader.get());
    ASSERT_NE(node.typed_value_read_type, nullptr);
    ASSERT_EQ(TYPE_ARRAY, node.typed_value_read_type->type);
    ASSERT_EQ(1, node.typed_value_read_type->children.size());
    ASSERT_EQ(TYPE_VARBINARY, node.typed_value_read_type->children[0].type);
}

TEST(ColumnReaderFactoryTest, VariantScalarTypeInferenceBranches) {
    int idx = 2;
    auto next_idx = [&]() { return idx++; };
    std::vector<ParquetField> children;

    // invalid decimal precision => infer TYPE_VARIANT
    auto dec_prec0 = make_shredded_scalar_node("dec_prec0", next_idx(), next_idx(), tparquet::Type::INT32);
    dec_prec0.children[1].schema_element.__set_converted_type(tparquet::ConvertedType::DECIMAL);
    dec_prec0.children[1].precision = 0;
    dec_prec0.children[1].scale = 0;
    children.emplace_back(std::move(dec_prec0));

    // invalid decimal scale > precision => infer TYPE_VARIANT
    auto dec_scale = make_shredded_scalar_node("dec_scale", next_idx(), next_idx(), tparquet::Type::INT32);
    dec_scale.children[1].schema_element.__set_converted_type(tparquet::ConvertedType::DECIMAL);
    dec_scale.children[1].precision = 5;
    dec_scale.children[1].scale = 6;
    children.emplace_back(std::move(dec_scale));

    auto logical_i32 = make_shredded_scalar_node("logical_i32", next_idx(), next_idx(), tparquet::Type::INT32);
    {
        tparquet::IntType t;
        t.bitWidth = 32;
        t.isSigned = true;
        tparquet::LogicalType l;
        l.__set_INTEGER(t);
        logical_i32.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_i32));

    auto logical_i64 = make_shredded_scalar_node("logical_i64", next_idx(), next_idx(), tparquet::Type::INT64);
    {
        tparquet::IntType t;
        t.bitWidth = 64;
        t.isSigned = true;
        tparquet::LogicalType l;
        l.__set_INTEGER(t);
        logical_i64.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_i64));

    auto logical_i_invalid =
            make_shredded_scalar_node("logical_i_invalid", next_idx(), next_idx(), tparquet::Type::INT32);
    {
        tparquet::IntType t;
        t.bitWidth = 24;
        t.isSigned = true;
        tparquet::LogicalType l;
        l.__set_INTEGER(t);
        logical_i_invalid.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_i_invalid));

    auto logical_u8 = make_shredded_scalar_node("logical_u8", next_idx(), next_idx(), tparquet::Type::INT32);
    {
        tparquet::IntType t;
        t.bitWidth = 8;
        t.isSigned = false;
        tparquet::LogicalType l;
        l.__set_INTEGER(t);
        logical_u8.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_u8));

    auto logical_u16 = make_shredded_scalar_node("logical_u16", next_idx(), next_idx(), tparquet::Type::INT32);
    {
        tparquet::IntType t;
        t.bitWidth = 16;
        t.isSigned = false;
        tparquet::LogicalType l;
        l.__set_INTEGER(t);
        logical_u16.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_u16));

    auto logical_u64 = make_shredded_scalar_node("logical_u64", next_idx(), next_idx(), tparquet::Type::INT64);
    {
        tparquet::IntType t;
        t.bitWidth = 64;
        t.isSigned = false;
        tparquet::LogicalType l;
        l.__set_INTEGER(t);
        logical_u64.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_u64));

    auto logical_date = make_shredded_scalar_node("logical_date", next_idx(), next_idx(), tparquet::Type::INT32);
    {
        tparquet::DateType t;
        tparquet::LogicalType l;
        l.__set_DATE(t);
        logical_date.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_date));

    auto logical_ts = make_shredded_scalar_node("logical_ts", next_idx(), next_idx(), tparquet::Type::INT64);
    {
        tparquet::TimestampType ts;
        ts.isAdjustedToUTC = true;
        ts.unit.__set_MICROS(tparquet::MicroSeconds());
        tparquet::LogicalType l;
        l.__set_TIMESTAMP(ts);
        logical_ts.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_ts));

    auto logical_bson = make_shredded_scalar_node("logical_bson", next_idx(), next_idx(), tparquet::Type::BYTE_ARRAY);
    {
        tparquet::BsonType t;
        tparquet::LogicalType l;
        l.__set_BSON(t);
        logical_bson.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_bson));

    auto logical_uuid = make_shredded_scalar_node("logical_uuid", next_idx(), next_idx(), tparquet::Type::BYTE_ARRAY);
    {
        tparquet::UUIDType t;
        tparquet::LogicalType l;
        l.__set_UUID(t);
        logical_uuid.children[1].schema_element.__set_logicalType(l);
    }
    children.emplace_back(std::move(logical_uuid));

    auto cvt_utf8 = make_shredded_scalar_node("cvt_utf8", next_idx(), next_idx(), tparquet::Type::BYTE_ARRAY);
    cvt_utf8.children[1].schema_element.__set_converted_type(tparquet::ConvertedType::UTF8);
    children.emplace_back(std::move(cvt_utf8));

    auto cvt_enum = make_shredded_scalar_node("cvt_enum", next_idx(), next_idx(), tparquet::Type::BYTE_ARRAY);
    cvt_enum.children[1].schema_element.__set_converted_type(tparquet::ConvertedType::ENUM);
    children.emplace_back(std::move(cvt_enum));

    auto cvt_json = make_shredded_scalar_node("cvt_json", next_idx(), next_idx(), tparquet::Type::BYTE_ARRAY);
    cvt_json.children[1].schema_element.__set_converted_type(tparquet::ConvertedType::JSON);
    children.emplace_back(std::move(cvt_json));

    auto cvt_bson = make_shredded_scalar_node("cvt_bson", next_idx(), next_idx(), tparquet::Type::BYTE_ARRAY);
    cvt_bson.children[1].schema_element.__set_converted_type(tparquet::ConvertedType::BSON);
    children.emplace_back(std::move(cvt_bson));

    auto cvt_interval =
            make_shredded_scalar_node("cvt_interval", next_idx(), next_idx(), tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    cvt_interval.children[1].schema_element.__set_converted_type(tparquet::ConvertedType::INTERVAL);
    children.emplace_back(std::move(cvt_interval));

    auto cvt_u64 = make_shredded_scalar_node("cvt_u64", next_idx(), next_idx(), tparquet::Type::INT64);
    cvt_u64.children[1].schema_element.__set_converted_type(tparquet::ConvertedType::UINT_64);
    children.emplace_back(std::move(cvt_u64));

    auto phy_fixed =
            make_shredded_scalar_node("phy_fixed", next_idx(), next_idx(), tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    children.emplace_back(std::move(phy_fixed));

    auto phy_default = make_shredded_scalar_node("phy_default", next_idx(), next_idx(), tparquet::Type::INT96);
    children.emplace_back(std::move(phy_default));

    ParquetField variant = make_variant_field_with_typed_group(children);
    auto opts = make_opts_with_num_cols(idx + 1);

    auto st = ColumnReaderFactory::create_variant_column_reader(opts, &variant, {});
    ASSERT_TRUE(st.ok()) << st.status().to_string();
    ASSERT_NE(st.value(), nullptr);
}

TEST(ColumnReaderFactoryTest, VariantShreddedCollectIoRangeAndSelectOffsetIndex) {
    auto obj = make_shredded_object_node_with_nested_scalar("obj", 2, 3, 4, tparquet::Type::INT32);
    ParquetField variant = make_variant_field_with_typed_group({obj});
    auto opts = make_opts_with_num_cols(5);

    auto st = ColumnReaderFactory::create_variant_column_reader(opts, &variant, {});
    ASSERT_TRUE(st.ok()) << st.status().to_string();
    ASSERT_NE(st.value(), nullptr);

    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    st.value()->collect_column_io_range(&ranges, &end_offset, ColumnIOType::PAGES, true);
    // metadata/value plus shredded typed/fallback readers should contribute extra ranges.
    ASSERT_GT(ranges.size(), 2);

    SparseRange<uint64_t> sparse_range;
    sparse_range.add(Range<uint64_t>(0, 8));
    st.value()->select_offset_index(sparse_range, 0);
}

TEST(ColumnReaderFactoryTest, VariantShreddedAllNullFallbackSkipsIoRange) {
    auto obj = make_shredded_object_node_with_nested_scalar("obj", 2, 3, 4, tparquet::Type::INT32);
    ParquetField variant = make_variant_field_with_typed_group({obj});

    auto collect_range_count = [&variant](ColumnReaderOptions opts) {
        auto st = ColumnReaderFactory::create_variant_column_reader(opts, &variant, {});
        EXPECT_TRUE(st.ok()) << st.status().to_string();
        if (!st.ok()) {
            return size_t{0};
        }

        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        st.value()->collect_column_io_range(&ranges, &end_offset, ColumnIOType::PAGES, true);
        return ranges.size();
    };

    auto baseline_opts = make_opts_with_num_cols(5);
    const size_t baseline_ranges = collect_range_count(baseline_opts);

    auto all_null_opts = make_opts_with_num_cols(5);
    auto mark_all_null = [&](int idx) {
        auto* row_group_meta = const_cast<tparquet::RowGroup*>(all_null_opts.row_group_meta);
        auto& meta = row_group_meta->columns[idx].meta_data;
        meta.__set_num_values(8);
        tparquet::Statistics statistics;
        statistics.__set_null_count(meta.num_values);
        meta.__set_statistics(statistics);
    };
    mark_all_null(2);
    mark_all_null(3);

    const size_t skipped_ranges = collect_range_count(all_null_opts);
    ASSERT_GT(baseline_ranges, skipped_ranges);
    ASSERT_EQ(baseline_ranges - 2, skipped_ranges);
}

TEST(ColumnReaderFactoryTest, VariantShreddedTypedLeafSkipsTopLevelBinaryIoRange) {
    auto obj = make_shredded_object_node_with_nested_scalar("obj", 2, 3, 4, tparquet::Type::INT32);
    ParquetField variant = make_variant_field_with_typed_group({obj});
    auto opts = make_opts_with_num_cols(5);
    auto* row_group_meta = const_cast<tparquet::RowGroup*>(opts.row_group_meta);
    row_group_meta->__set_num_rows(8);

    auto set_null_count = [&](int idx, int64_t null_count, int64_t num_values) {
        auto& meta = row_group_meta->columns[idx].meta_data;
        meta.__set_num_values(num_values);
        tparquet::Statistics statistics;
        statistics.__set_null_count(null_count);
        meta.__set_statistics(statistics);
    };
    // Top-level metadata has no nulls, so the fast path can synthesize the outer null mask.
    set_null_count(0, 0, 8);
    // Requested leaf fallback is all null, so the fast path will not fall back to top-level value.
    set_null_count(3, 8, 8);

    VariantShreddedReadHints hints;
    ASSERT_OK(hints.add_path("obj.k"));
    auto st = ColumnReaderFactory::create_variant_column_reader(opts, &variant, hints);
    ASSERT_TRUE(st.ok()) << st.status().to_string();

    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    st.value()->collect_column_io_range(&ranges, &end_offset, ColumnIOType::PAGES, true);

    // Only the requested INT32 typed_value leaf should need page IO. Top-level metadata/value
    // and the all-null leaf fallback value are all skipped.
    ASSERT_EQ(1, ranges.size());
}

TEST(ColumnReaderFactoryTest, StructWithoutFieldIdMatchesIcebergSubfieldsByName) {
    ParquetField field;
    field.name = "col";
    field.type = ColumnType::STRUCT;
    field.children.emplace_back(make_scalar_field("a", 0, tparquet::Type::INT32));
    field.children.emplace_back(make_scalar_field("b", 1, tparquet::Type::INT32));

    TypeDescriptor col_type = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    col_type.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
    col_type.field_names.emplace_back("a");
    col_type.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
    col_type.field_names.emplace_back("missing");

    TIcebergSchemaField field_a;
    field_a.__set_field_id(1);
    field_a.__set_name("a");
    TIcebergSchemaField field_missing;
    field_missing.__set_field_id(2);
    field_missing.__set_name("missing");
    TIcebergSchemaField root_field;
    root_field.__set_field_id(10);
    root_field.__set_name("col");
    root_field.__set_children(std::vector<TIcebergSchemaField>{field_a, field_missing});

    tparquet::FileMetaData file_meta;
    tparquet::SchemaElement root_schema;
    root_schema.__set_name("table");
    root_schema.__set_num_children(1);
    tparquet::SchemaElement col_schema;
    col_schema.__set_name("col");
    col_schema.__set_num_children(2);
    tparquet::SchemaElement a_schema;
    a_schema.__set_name("a");
    a_schema.__set_type(tparquet::Type::INT32);
    a_schema.__set_num_children(0);
    tparquet::SchemaElement b_schema;
    b_schema.__set_name("b");
    b_schema.__set_type(tparquet::Type::INT32);
    b_schema.__set_num_children(0);
    file_meta.__set_schema(std::vector<tparquet::SchemaElement>{root_schema, col_schema, a_schema, b_schema});

    auto opts = make_opts_with_num_cols(2);
    FileMetaData parsed_meta;
    ASSERT_OK(parsed_meta.init(file_meta, true));
    opts.file_meta_data = &parsed_meta;

    auto reader_or = ColumnReaderFactory::create(opts, &field, col_type, &root_field);
    ASSERT_TRUE(reader_or.ok()) << reader_or.status().to_string();
    auto* struct_reader = dynamic_cast<StructColumnReader*>(reader_or.value().get());
    ASSERT_NE(struct_reader, nullptr);
    ASSERT_NE(struct_reader->get_child_column_reader("a"), nullptr);
    ASSERT_EQ(struct_reader->get_child_column_reader("missing"), nullptr);
}

TEST(ColumnReaderFactoryTest, StructWithoutFieldIdMatchesIcebergSubfieldsByPhysicalName) {
    ParquetField field;
    field.name = "col";
    field.type = ColumnType::STRUCT;
    field.children.emplace_back(make_scalar_field("a_phys", 0, tparquet::Type::INT32));
    field.children.emplace_back(make_scalar_field("b_phys", 1, tparquet::Type::INT32));

    TypeDescriptor col_type = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    col_type.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
    col_type.field_names.emplace_back("a");
    col_type.field_physical_names.emplace_back("a_phys");
    col_type.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
    col_type.field_names.emplace_back("missing");
    col_type.field_physical_names.emplace_back("missing_phys");

    TIcebergSchemaField field_a;
    field_a.__set_field_id(1);
    field_a.__set_name("a");
    TIcebergSchemaField field_missing;
    field_missing.__set_field_id(2);
    field_missing.__set_name("missing");
    TIcebergSchemaField root_field;
    root_field.__set_field_id(10);
    root_field.__set_name("col");
    root_field.__set_children(std::vector<TIcebergSchemaField>{field_a, field_missing});

    tparquet::FileMetaData file_meta;
    tparquet::SchemaElement root_schema;
    root_schema.__set_name("table");
    root_schema.__set_num_children(1);
    tparquet::SchemaElement col_schema;
    col_schema.__set_name("col");
    col_schema.__set_num_children(2);
    tparquet::SchemaElement a_schema;
    a_schema.__set_name("a_phys");
    a_schema.__set_type(tparquet::Type::INT32);
    a_schema.__set_num_children(0);
    tparquet::SchemaElement b_schema;
    b_schema.__set_name("b_phys");
    b_schema.__set_type(tparquet::Type::INT32);
    b_schema.__set_num_children(0);
    file_meta.__set_schema(std::vector<tparquet::SchemaElement>{root_schema, col_schema, a_schema, b_schema});

    auto opts = make_opts_with_num_cols(2);
    FileMetaData parsed_meta;
    ASSERT_OK(parsed_meta.init(file_meta, true));
    opts.file_meta_data = &parsed_meta;

    auto reader_or = ColumnReaderFactory::create(opts, &field, col_type, &root_field);
    ASSERT_TRUE(reader_or.ok()) << reader_or.status().to_string();
    auto* struct_reader = dynamic_cast<StructColumnReader*>(reader_or.value().get());
    ASSERT_NE(struct_reader, nullptr);
    ASSERT_NE(struct_reader->get_child_column_reader("a"), nullptr);
    ASSERT_EQ(struct_reader->get_child_column_reader("missing"), nullptr);
}

// Minimal concrete subclass to allow instantiation of the abstract ColumnReader.
class TestColumnReader : public ColumnReader {
public:
    explicit TestColumnReader(const ParquetField* field) : ColumnReader(field) {}
    Status prepare() override { return Status::OK(); }
    Status read_range(const Range<uint64_t>&, const Filter*, ColumnPtr&) override { return Status::OK(); }
    void get_levels(level_t**, level_t**, size_t*) override {}
    void set_need_parse_levels(bool) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>*, int64_t*, ColumnIOTypeFlags,
                                 bool) override {}
    void select_offset_index(const SparseRange<uint64_t>&, const uint64_t) override {}
};

// Covers the FIXED_LEN_BYTE_ARRAY condition added to check_type_can_apply_bloom_filter.
TEST(ColumnReaderTest, CheckTypeCanApplyBloomFilterFixedLenByteArray) {
    ParquetField field;
    field.name = "col";
    field.type = ColumnType::SCALAR;
    field.physical_column_index = 0;

    auto check = [&](LogicalType logical, tparquet::Type::type physical) -> bool {
        field.physical_type = physical;
        TestColumnReader reader(&field);
        return reader.check_type_can_apply_bloom_filter(TypeDescriptor(logical), field);
    };

    // VARBINARY + FIXED_LEN_BYTE_ARRAY is applicable (the new condition)
    EXPECT_TRUE(check(TYPE_VARBINARY, tparquet::Type::FIXED_LEN_BYTE_ARRAY));

    // VARCHAR + FIXED_LEN_BYTE_ARRAY is not applicable
    // UUID is stored as FIXED_LEN_BYTE_ARRAY and typically read as VARCHAR
    EXPECT_FALSE(check(TYPE_VARCHAR, tparquet::Type::FIXED_LEN_BYTE_ARRAY));

    // BYTE_ARRAY baseline: both VARCHAR and VARBINARY remain applicable
    EXPECT_TRUE(check(TYPE_VARCHAR, tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(check(TYPE_VARBINARY, tparquet::Type::BYTE_ARRAY));
}

} // namespace starrocks::parquet
