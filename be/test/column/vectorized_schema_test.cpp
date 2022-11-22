// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/vectorized_schema.h"

#include <gtest/gtest.h>

namespace starrocks::vectorized {

class SchemaTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    VectorizedFields make_fields(size_t size) {
        VectorizedFields fields;
        for (size_t i = 0; i < size; i++) {
            fields.emplace_back(make_field(i));
        }
        return fields;
    }

    std::string make_string(size_t i) { return std::string("c").append(std::to_string(static_cast<int32_t>(i))); }

    VectorizedFieldPtr make_field(size_t i) {
        return std::make_shared<VectorizedField>(i, make_string(i), get_type_info(TYPE_INT), false);
    }

    VectorizedSchema* make_schema(size_t i) {
        VectorizedFields fields = make_fields(i);
        return new VectorizedSchema(fields);
    }

    void check_field(const VectorizedFieldPtr& field, size_t i) {
        ASSERT_EQ(make_string(i), field->name());
        ASSERT_EQ(i, field->id());
        ASSERT_FALSE(field->is_nullable());
    }

    void check_schema(VectorizedSchema* schema, size_t size) {
        ASSERT_EQ(schema->num_fields(), size);
        for (size_t i = 0; i < size; i++) {
            VectorizedFieldPtr field = schema->get_field_by_name(make_string(i));
            check_field(field, i);
        }
    }
};

TEST_F(SchemaTest, test_construct) {
    VectorizedFields fields1 = make_fields(2);
    auto* schema1 = new VectorizedSchema(fields1);
    check_schema(schema1, 2);
    delete schema1;

    VectorizedFields fields2 = make_fields(2);
    auto* schema2 = new VectorizedSchema(std::move(fields2));
    check_schema(schema2, 2);
    delete schema2;
}

TEST_F(SchemaTest, append) {
    VectorizedSchema* schema = make_schema(2);
    schema->append(make_field(2));

    check_schema(schema, 3);
    delete schema;
}

TEST_F(SchemaTest, insert) {
    VectorizedSchema* schema = make_schema(0);
    schema->append(make_field(0));
    schema->append(make_field(2));
    schema->insert(1, make_field(1));

    check_schema(schema, 3);
    delete schema;
}

TEST_F(SchemaTest, remove) {
    VectorizedSchema* schema = make_schema(2);
    schema->remove(1);

    check_schema(schema, 1);
    delete schema;
}

TEST_F(SchemaTest, field) {
    VectorizedSchema* schema = make_schema(2);
    const VectorizedFieldPtr& ptr = schema->field(0);

    check_field(ptr, 0);
    delete schema;
}

TEST_F(SchemaTest, field_names) {
    VectorizedSchema* schema = make_schema(2);
    std::vector<std::string> names = schema->field_names();

    std::vector<std::string> check_names;
    check_names.emplace_back("c0");
    check_names.emplace_back("c1");
    ASSERT_EQ(check_names, names);
    delete schema;
}

TEST_F(SchemaTest, get_field_by_name) {
    VectorizedSchema* schema = make_schema(2);

    VectorizedFieldPtr ptr1 = schema->get_field_by_name("c0");
    check_field(ptr1, 0);

    VectorizedFieldPtr ptr2 = schema->get_field_by_name("c2");
    ASSERT_EQ(ptr2, nullptr);
    delete schema;
}

TEST_F(SchemaTest, get_field_index) {
    VectorizedSchema* schema = make_schema(2);
    size_t idx1 = schema->get_field_index_by_name("c0");
    ASSERT_EQ(0, idx1);
    size_t idx2 = schema->get_field_index_by_name("c2");
    ASSERT_EQ(-1, idx2);
    delete schema;
}

} // namespace starrocks::vectorized
