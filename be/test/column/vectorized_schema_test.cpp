// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <gtest/gtest.h>

#include "column/schema.h"

namespace starrocks::vectorized {

class SchemaTest : public testing::Test {
public:
    virtual void SetUp() {}
    virtual void TearDown() {}

    Fields make_fields(size_t size) {
        Fields fields;
        for (size_t i = 0; i < size; i++) {
            fields.emplace_back(make_field(i));
        }
        return fields;
    }

    std::string make_string(size_t i) { return std::string("c").append(std::to_string(static_cast<int32_t>(i))); }

    FieldPtr make_field(size_t i) {
        return std::make_shared<Field>(i, make_string(i), get_type_info(OLAP_FIELD_TYPE_INT), false);
    }

    Schema* make_schema(size_t i) {
        Fields fields = make_fields(i);
        return new Schema(fields);
    }

    void check_field(const FieldPtr field, size_t i) {
        ASSERT_EQ(make_string(i), field->name());
        ASSERT_EQ(i, field->id());
        ASSERT_FALSE(field->is_nullable());
    }

    void check_schema(Schema* schema, size_t size) {
        ASSERT_EQ(schema->num_fields(), size);
        for (size_t i = 0; i < size; i++) {
            FieldPtr field = schema->get_field_by_name(make_string(i));
            check_field(field, i);
        }
    }
};

TEST_F(SchemaTest, test_construct) {
    Fields fields1 = make_fields(2);
    Schema* schema1 = new Schema(fields1);
    check_schema(schema1, 2);
    delete schema1;

    Fields fields2 = make_fields(2);
    Schema* schema2 = new Schema(std::move(fields2));
    check_schema(schema2, 2);
    delete schema2;
}

TEST_F(SchemaTest, append) {
    Schema* schema = make_schema(2);
    schema->append(make_field(2));

    check_schema(schema, 3);
    delete schema;
}

TEST_F(SchemaTest, insert) {
    Schema* schema = make_schema(0);
    schema->append(make_field(0));
    schema->append(make_field(2));
    schema->insert(1, make_field(1));

    check_schema(schema, 3);
    delete schema;
}

TEST_F(SchemaTest, remove) {
    Schema* schema = make_schema(2);
    schema->remove(1);

    check_schema(schema, 1);
    delete schema;
}

TEST_F(SchemaTest, field) {
    Schema* schema = make_schema(2);
    const FieldPtr& ptr = schema->field(0);

    check_field(ptr, 0);
    delete schema;
}

TEST_F(SchemaTest, field_names) {
    Schema* schema = make_schema(2);
    std::vector<std::string> names = schema->field_names();

    std::vector<std::string> check_names;
    check_names.emplace_back("c0");
    check_names.emplace_back("c1");
    ASSERT_EQ(check_names, names);
    delete schema;
}

TEST_F(SchemaTest, get_field_by_name) {
    Schema* schema = make_schema(2);

    FieldPtr ptr1 = schema->get_field_by_name("c0");
    check_field(ptr1, 0);

    FieldPtr ptr2 = schema->get_field_by_name("c2");
    ASSERT_EQ(ptr2, nullptr);
    delete schema;
}

TEST_F(SchemaTest, get_field_index) {
    Schema* schema = make_schema(2);
    size_t idx1 = schema->get_field_index_by_name("c0");
    ASSERT_EQ(0, idx1);
    size_t idx2 = schema->get_field_index_by_name("c2");
    ASSERT_EQ(-1, idx2);
    delete schema;
}

} // namespace starrocks::vectorized
