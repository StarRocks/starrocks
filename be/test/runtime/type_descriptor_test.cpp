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

#include <tuple>

#include "gtest/gtest.h"
#include "runtime/types.h"

namespace starrocks {

class TypeDescriptorTest : public ::testing::Test {
public:
};

// NOLINTNEXTLINE
TEST_F(TypeDescriptorTest, test_from_thrift) {
    // INT
    {
        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::INT);
        ttype_desc.types.back().scalar_type.__set_len(-1);

        auto t = TypeDescriptor::from_thrift(ttype_desc);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_INT, t.type);

        ASSERT_EQ(ttype_desc, t.to_thrift());
    }
    // float
    {
        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::FLOAT);
        ttype_desc.types.back().scalar_type.__set_len(-1);

        auto t = TypeDescriptor::from_thrift(ttype_desc);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_FLOAT, t.type);

        ASSERT_EQ(ttype_desc, t.to_thrift());
    }
    // double
    {
        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::DOUBLE);
        ttype_desc.types.back().scalar_type.__set_len(-1);

        auto t = TypeDescriptor::from_thrift(ttype_desc);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_DOUBLE, t.type);

        ASSERT_EQ(ttype_desc, t.to_thrift());
    }
    // decimal
    {
        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::DECIMALV2);
        ttype_desc.types.back().scalar_type.__set_precision(4);
        ttype_desc.types.back().scalar_type.__set_scale(6);
        ttype_desc.types.back().scalar_type.__set_len(-1);

        auto t = TypeDescriptor::from_thrift(ttype_desc);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_DECIMALV2, t.type);
        ASSERT_EQ(6, t.scale);
        ASSERT_EQ(4, t.precision);

        ASSERT_EQ(ttype_desc, t.to_thrift());
    }
    // CHAR(10)
    {
        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::CHAR);
        ttype_desc.types.back().scalar_type.__set_len(10);

        auto t = TypeDescriptor::from_thrift(ttype_desc);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_CHAR, t.type);
        ASSERT_EQ(10, t.len);

        ASSERT_EQ(ttype_desc, t.to_thrift());
    }
    // VARCHAR(10)
    {
        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::VARCHAR);
        ttype_desc.types.back().scalar_type.__set_len(10);

        auto t = TypeDescriptor::from_thrift(ttype_desc);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_VARCHAR, t.type);
        ASSERT_EQ(10, t.len);

        ASSERT_EQ(ttype_desc, t.to_thrift());
    }
    // ARRAY<VARCHAR(10)>
    {
        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::ARRAY);
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::VARCHAR);
        ttype_desc.types.back().scalar_type.__set_len(10);

        auto t = TypeDescriptor::from_thrift(ttype_desc);
        ASSERT_TRUE(t.is_complex_type());
        ASSERT_TRUE(t.is_collection_type());
        ASSERT_EQ(1, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_ARRAY, t.type);
        ASSERT_FALSE(t.children.at(0).is_complex_type());
        ASSERT_EQ(0, t.children.at(0).children.size());
        ASSERT_EQ(LogicalType::TYPE_VARCHAR, t.children.at(0).type);
        ASSERT_EQ(10, t.children.at(0).len);

        ASSERT_EQ(ttype_desc, t.to_thrift());
    }
    // MAP<VARCHAR(10), MAP<INT, DOUBLE>>
    {
        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.resize(5);
        ttype_desc.types[0].__set_type(TTypeNodeType::MAP);
        ttype_desc.types[1].__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types[1].__set_scalar_type(TScalarType());
        ttype_desc.types[1].scalar_type.__set_type(TPrimitiveType::VARCHAR);
        ttype_desc.types[1].scalar_type.__set_len(10);
        ttype_desc.types[2].__set_type(TTypeNodeType::MAP);
        ttype_desc.types[3].__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types[3].__set_scalar_type(TScalarType());
        ttype_desc.types[3].scalar_type.__set_type(TPrimitiveType::INT);
        ttype_desc.types[4].__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types[4].__set_scalar_type(TScalarType());
        ttype_desc.types[4].scalar_type.__set_type(TPrimitiveType::DOUBLE);

        auto t = TypeDescriptor::from_thrift(ttype_desc);
        ASSERT_TRUE(t.is_complex_type());
        ASSERT_TRUE(t.is_collection_type());
        ASSERT_EQ(LogicalType::TYPE_MAP, t.type);
        ASSERT_EQ(2, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_VARCHAR, t.children[0].type);
        ASSERT_EQ(10, t.children[0].len);
        ASSERT_EQ(LogicalType::TYPE_MAP, t.children[1].type);
        ASSERT_EQ(2, t.children[1].children.size());
        ASSERT_EQ(LogicalType::TYPE_INT, t.children[1].children[0].type);
        ASSERT_EQ(LogicalType::TYPE_DOUBLE, t.children[1].children[1].type);
    }
    // struct{a INT, b ARRAY<int>}
    {
        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.resize(4);
        ttype_desc.types[0].__set_type(TTypeNodeType::STRUCT);
        ttype_desc.types[0].__isset.struct_fields = true;
        ttype_desc.types[0].struct_fields.resize(2);
        ttype_desc.types[0].struct_fields[0].__set_name("a");
        ttype_desc.types[0].struct_fields[1].__set_name("b");
        ttype_desc.types[1].__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types[1].__set_scalar_type(TScalarType());
        ttype_desc.types[1].scalar_type.__set_type(TPrimitiveType::INT);
        ttype_desc.types[2].__set_type(TTypeNodeType::ARRAY);
        ttype_desc.types[3].__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types[3].__set_scalar_type(TScalarType());
        ttype_desc.types[3].scalar_type.__set_type(TPrimitiveType::INT);

        auto t = TypeDescriptor::from_thrift(ttype_desc);
        ASSERT_TRUE(t.is_complex_type());
        ASSERT_FALSE(t.is_collection_type());
        ASSERT_EQ(LogicalType::TYPE_STRUCT, t.type);
        ASSERT_EQ(2, t.field_names.size());
        ASSERT_EQ("a", t.field_names[0]);
        ASSERT_EQ("b", t.field_names[1]);
        ASSERT_EQ(2, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_INT, t.children[0].type);
        ASSERT_EQ(LogicalType::TYPE_ARRAY, t.children[1].type);
        ASSERT_EQ(1, t.children[1].children.size());
        ASSERT_EQ(LogicalType::TYPE_INT, t.children[1].children[0].type);
    }
}

// NOLINTNEXTLINE
TEST_F(TypeDescriptorTest, test_to_thrift) {
    // TINYINT
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_TINYINT;
        TTypeDesc ttype_desc = t.to_thrift();
        ASSERT_EQ(1, ttype_desc.types.size());
        ASSERT_EQ(TTypeNodeType::SCALAR, ttype_desc.types[0].type);
        ASSERT_EQ(TPrimitiveType::TINYINT, ttype_desc.types[0].scalar_type.type);

        ASSERT_EQ(t, TypeDescriptor::from_thrift(ttype_desc));
    }
    // VARCHAR(8)
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_VARCHAR;
        t.len = 8;
        TTypeDesc ttype_desc = t.to_thrift();
        ASSERT_EQ(1, ttype_desc.types.size());
        ASSERT_EQ(TTypeNodeType::SCALAR, ttype_desc.types[0].type);
        ASSERT_EQ(TPrimitiveType::VARCHAR, ttype_desc.types[0].scalar_type.type);
        ASSERT_EQ(8, ttype_desc.types[0].scalar_type.len);

        ASSERT_EQ(t, TypeDescriptor::from_thrift(ttype_desc));
    }
    // DECIMAL(8, 4)
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_DECIMALV2;
        t.len = 16;
        t.scale = 8;
        t.precision = 4;
        TTypeDesc ttype_desc = t.to_thrift();
        ASSERT_EQ(1, ttype_desc.types.size());
        ASSERT_EQ(TTypeNodeType::SCALAR, ttype_desc.types[0].type);
        ASSERT_EQ(TPrimitiveType::DECIMALV2, ttype_desc.types[0].scalar_type.type);
        ASSERT_EQ(16, ttype_desc.types[0].scalar_type.len);
        ASSERT_EQ(8, ttype_desc.types[0].scalar_type.scale);
        ASSERT_EQ(4, ttype_desc.types[0].scalar_type.precision);

        ASSERT_EQ(t, TypeDescriptor::from_thrift(ttype_desc));
    }
    // ARRAY<INT>
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_ARRAY;
        t.children.emplace_back();
        t.children.back().type = LogicalType::TYPE_INT;
        TTypeDesc ttype_desc = t.to_thrift();
        ASSERT_EQ(2, ttype_desc.types.size());
        ASSERT_EQ(TTypeNodeType::ARRAY, ttype_desc.types[0].type);
        ASSERT_EQ(TPrimitiveType::INT, ttype_desc.types[1].scalar_type.type);

        ASSERT_EQ(t, TypeDescriptor::from_thrift(ttype_desc));
    }
    // ARRAY<ARRAY<INT>>
    {
        TypeDescriptor t;
        t.children.resize(1);
        t.type = LogicalType::TYPE_ARRAY;
        t.children[0].type = LogicalType::TYPE_ARRAY;
        t.children[0].children.resize(1);
        t.children[0].children[0].type = LogicalType::TYPE_INT;
        TTypeDesc ttype_desc = t.to_thrift();
        ASSERT_EQ(3, ttype_desc.types.size());
        ASSERT_EQ(TTypeNodeType::ARRAY, ttype_desc.types[0].type);
        ASSERT_EQ(TTypeNodeType::ARRAY, ttype_desc.types[1].type);
        ASSERT_EQ(TTypeNodeType::SCALAR, ttype_desc.types[2].type);
        ASSERT_EQ(TPrimitiveType::INT, ttype_desc.types[2].scalar_type.type);

        ASSERT_EQ(t, TypeDescriptor::from_thrift(ttype_desc));
    }
    // MAP<int, struct{a tinyint, b bigint}>
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_MAP;
        t.children.resize(2);
        t.children[0].type = LogicalType::TYPE_INT;
        t.children[1].type = LogicalType::TYPE_STRUCT;
        t.children[1].field_names = {"a", "b"};
        t.children[1].children.resize(2);
        t.children[1].children[0].type = LogicalType::TYPE_TINYINT;
        t.children[1].children[1].type = LogicalType::TYPE_BIGINT;

        TTypeDesc ttype_desc = t.to_thrift();
        ASSERT_TRUE(ttype_desc.__isset.types);
        ASSERT_EQ(5, ttype_desc.types.size());
        ASSERT_EQ(TTypeNodeType::MAP, ttype_desc.types[0].type);

        ASSERT_EQ(TTypeNodeType::SCALAR, ttype_desc.types[1].type);
        ASSERT_TRUE(ttype_desc.types[1].__isset.scalar_type);
        ASSERT_EQ(TPrimitiveType::INT, ttype_desc.types[1].scalar_type.type);

        ASSERT_EQ(TTypeNodeType::STRUCT, ttype_desc.types[2].type);
        ASSERT_TRUE(ttype_desc.types[2].__isset.struct_fields);
        ASSERT_EQ(2, ttype_desc.types[2].struct_fields.size());
        ASSERT_EQ("a", ttype_desc.types[2].struct_fields[0].name);
        ASSERT_EQ("b", ttype_desc.types[2].struct_fields[1].name);

        ASSERT_EQ(TTypeNodeType::SCALAR, ttype_desc.types[3].type);
        ASSERT_TRUE(ttype_desc.types[3].__isset.scalar_type);
        ASSERT_EQ(TPrimitiveType::TINYINT, ttype_desc.types[3].scalar_type.type);

        ASSERT_EQ(TTypeNodeType::SCALAR, ttype_desc.types[4].type);
        ASSERT_TRUE(ttype_desc.types[4].__isset.scalar_type);
        ASSERT_EQ(TPrimitiveType::BIGINT, ttype_desc.types[4].scalar_type.type);
    }
}

// NOLINTNEXTLINE
TEST_F(TypeDescriptorTest, test_from_protobuf) {
    // TINYINT
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.mutable_types(0)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::TINYINT);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_TINYINT, t.type);
    }
    // SMALLINT
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.mutable_types(0)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::SMALLINT);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_SMALLINT, t.type);
    }
    // INT
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.mutable_types(0)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::INT);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_INT, t.type);
    }
    // float
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.mutable_types(0)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::FLOAT);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_FLOAT, t.type);
    }
    // double
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.mutable_types(0)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::DOUBLE);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_DOUBLE, t.type);
    }
    // decimal
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.mutable_types(0)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::DECIMALV2);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_precision(4);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_scale(6);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_DECIMALV2, t.type);
        ASSERT_EQ(6, t.scale);
        ASSERT_EQ(4, t.precision);
    }
    // CHAR(10)
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.mutable_types(0)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::CHAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_len(10);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_CHAR, t.type);
        ASSERT_EQ(10, t.len);
    }
    // VARCHAR(10)
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.mutable_types(0)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_type(TPrimitiveType::VARCHAR);
        t_pb.mutable_types(0)->mutable_scalar_type()->set_len(10);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_FALSE(t.is_complex_type());
        ASSERT_EQ(0, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_VARCHAR, t.type);
        ASSERT_EQ(10, t.len);
    }
    // ARRAY<VARCHAR(10)>
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.mutable_types(0)->set_type(TTypeNodeType::ARRAY);

        t_pb.add_types();
        t_pb.mutable_types(1)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(1)->mutable_scalar_type()->set_type(TPrimitiveType::VARCHAR);
        t_pb.mutable_types(1)->mutable_scalar_type()->set_len(10);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_TRUE(t.is_complex_type());
        ASSERT_TRUE(t.is_collection_type());
        ASSERT_EQ(1, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_ARRAY, t.type);

        ASSERT_FALSE(t.children.at(0).is_complex_type());
        ASSERT_EQ(0, t.children.at(0).children.size());
        ASSERT_EQ(LogicalType::TYPE_VARCHAR, t.children.at(0).type);
        ASSERT_EQ(10, t.children.at(0).len);
    }
    // struct{a INT, b MAP<INT, DOUBLE>}
    {
        PTypeDesc t_pb;
        t_pb.add_types();
        t_pb.add_types();
        t_pb.add_types();
        t_pb.add_types();
        t_pb.add_types();

        t_pb.mutable_types(0)->set_type(TTypeNodeType::STRUCT);
        t_pb.mutable_types(0)->add_struct_fields()->set_name("a");
        t_pb.mutable_types(0)->add_struct_fields()->set_name("b");
        t_pb.mutable_types(1)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(1)->mutable_scalar_type()->set_type(TPrimitiveType::INT);
        t_pb.mutable_types(2)->set_type(TTypeNodeType::MAP);
        t_pb.mutable_types(3)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(3)->mutable_scalar_type()->set_type(TPrimitiveType::INT);
        t_pb.mutable_types(4)->set_type(TTypeNodeType::SCALAR);
        t_pb.mutable_types(4)->mutable_scalar_type()->set_type(TPrimitiveType::DOUBLE);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_TRUE(t.is_complex_type());
        ASSERT_FALSE(t.is_collection_type());
        ASSERT_EQ(LogicalType::TYPE_STRUCT, t.type);
        ASSERT_EQ(2, t.field_names.size());
        ASSERT_EQ("a", t.field_names[0]);
        ASSERT_EQ("b", t.field_names[1]);
        ASSERT_EQ(2, t.children.size());
        ASSERT_EQ(LogicalType::TYPE_INT, t.children[0].type);
        ASSERT_EQ(LogicalType::TYPE_MAP, t.children[1].type);
        ASSERT_EQ(2, t.children[1].children.size());
        ASSERT_EQ(LogicalType::TYPE_INT, t.children[1].children[0].type);
        ASSERT_EQ(LogicalType::TYPE_DOUBLE, t.children[1].children[1].type);
    }
    // struct{}
    {
        PTypeDesc t_pb;
        t_pb.add_types();

        t_pb.mutable_types(0)->set_type(TTypeNodeType::STRUCT);

        auto t = TypeDescriptor::from_protobuf(t_pb);
        ASSERT_TRUE(t.is_complex_type());
        ASSERT_FALSE(t.is_collection_type());
        ASSERT_EQ(LogicalType::TYPE_STRUCT, t.type);
        ASSERT_EQ(0, t.field_names.size());
    }
}

// NOLINTNEXTLINE
TEST_F(TypeDescriptorTest, test_to_protobuf) {
    // TINYINT
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_TINYINT;
        PTypeDesc t_pb = t.to_protobuf();
        ASSERT_EQ(1, t_pb.types().size());
        ASSERT_EQ(TTypeNodeType::SCALAR, t_pb.types(0).type());
        ASSERT_EQ(TPrimitiveType::TINYINT, t_pb.types(0).scalar_type().type());

        ASSERT_EQ(t, TypeDescriptor::from_protobuf(t_pb));
    }
    // VARCHAR(8)
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_VARCHAR;
        t.len = 8;
        PTypeDesc t_pb = t.to_protobuf();
        ASSERT_EQ(1, t_pb.types().size());
        ASSERT_EQ(TTypeNodeType::SCALAR, t_pb.types(0).type());
        ASSERT_EQ(TPrimitiveType::VARCHAR, t_pb.types(0).scalar_type().type());
        ASSERT_EQ(8, t_pb.types(0).scalar_type().len());

        ASSERT_EQ(t, TypeDescriptor::from_protobuf(t_pb));
    }
    // DECIMAL(8, 4)
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_DECIMALV2;
        t.len = 16;
        t.scale = 8;
        t.precision = 4;
        PTypeDesc t_pb = t.to_protobuf();
        ASSERT_EQ(1, t_pb.types().size());
        ASSERT_EQ(TTypeNodeType::SCALAR, t_pb.types(0).type());
        ASSERT_EQ(TPrimitiveType::DECIMALV2, t_pb.types(0).scalar_type().type());
        ASSERT_EQ(16, t_pb.types(0).scalar_type().len());
        ASSERT_EQ(8, t_pb.types(0).scalar_type().scale());
        ASSERT_EQ(4, t_pb.types(0).scalar_type().precision());

        ASSERT_EQ(t, TypeDescriptor::from_protobuf(t_pb));
    }
    // ARRAY<INT>
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_ARRAY;
        t.children.emplace_back();
        t.children.back().type = LogicalType::TYPE_INT;
        t.children.back().len = sizeof(int32_t);
        PTypeDesc t_pb = t.to_protobuf();
        ASSERT_EQ(2, t_pb.types().size());
        ASSERT_EQ(TTypeNodeType::ARRAY, t_pb.types(0).type());
        ASSERT_EQ(TPrimitiveType::INT, t_pb.types(1).scalar_type().type());
        ASSERT_EQ(sizeof(int32_t), t_pb.types(1).scalar_type().len());

        ASSERT_EQ(t, TypeDescriptor::from_protobuf(t_pb));
    }
    // ARRAY<ARRAY<INT>>
    {
        TypeDescriptor t;
        t.children.resize(1);
        t.type = LogicalType::TYPE_ARRAY;
        t.children[0].type = LogicalType::TYPE_ARRAY;
        t.children[0].children.resize(1);
        t.children[0].children[0].type = LogicalType::TYPE_INT;
        PTypeDesc t_pb = t.to_protobuf();
        ASSERT_EQ(3, t_pb.types().size());
        ASSERT_EQ(TTypeNodeType::ARRAY, t_pb.types(0).type());
        ASSERT_EQ(TTypeNodeType::ARRAY, t_pb.types(1).type());
        ASSERT_EQ(TTypeNodeType::SCALAR, t_pb.types(2).type());
        ASSERT_EQ(TPrimitiveType::INT, t_pb.types(2).scalar_type().type());

        ASSERT_EQ(t, TypeDescriptor::from_protobuf(t_pb));
    }
    // MAP<int, struct{a bool, b DECIMALV2(10, 2), c VARCHAR(10)}
    {
        TypeDescriptor t;
        t.children.resize(2);
        t.type = LogicalType::TYPE_MAP;
        t.children[0].type = LogicalType::TYPE_INT;
        t.children[1].type = LogicalType::TYPE_STRUCT;
        t.children[1].field_names = {"a", "b", "c"};
        t.children[1].children.resize(3);
        t.children[1].children[0].type = LogicalType::TYPE_BOOLEAN;
        t.children[1].children[1].type = LogicalType::TYPE_DECIMALV2;
        t.children[1].children[1].precision = 10;
        t.children[1].children[1].scale = 2;
        t.children[1].children[2].type = LogicalType::TYPE_VARCHAR;
        t.children[1].children[2].len = 10;

        PTypeDesc t_pb = t.to_protobuf();
        ASSERT_EQ(6, t_pb.types().size());
        ASSERT_EQ(TTypeNodeType::MAP, t_pb.types(0).type());

        ASSERT_EQ(TTypeNodeType::SCALAR, t_pb.types(1).type());
        ASSERT_TRUE(t_pb.types(1).has_scalar_type());
        ASSERT_EQ(TPrimitiveType::INT, t_pb.types(1).scalar_type().type());

        ASSERT_EQ(TTypeNodeType::STRUCT, t_pb.types(2).type());
        ASSERT_EQ(3, t_pb.types(2).struct_fields_size());
        ASSERT_EQ("a", t_pb.types(2).struct_fields(0).name());
        ASSERT_EQ("b", t_pb.types(2).struct_fields(1).name());
        ASSERT_EQ("c", t_pb.types(2).struct_fields(2).name());

        ASSERT_EQ(TTypeNodeType::SCALAR, t_pb.types(3).type());
        ASSERT_TRUE(t_pb.types(3).has_scalar_type());
        ASSERT_EQ(TPrimitiveType::BOOLEAN, t_pb.types(3).scalar_type().type());

        ASSERT_EQ(TTypeNodeType::SCALAR, t_pb.types(4).type());
        ASSERT_TRUE(t_pb.types(4).has_scalar_type());
        ASSERT_EQ(TPrimitiveType::DECIMALV2, t_pb.types(4).scalar_type().type());
        ASSERT_EQ(10, t_pb.types(4).scalar_type().precision());
        ASSERT_EQ(2, t_pb.types(4).scalar_type().scale());

        ASSERT_EQ(TTypeNodeType::SCALAR, t_pb.types(5).type());
        ASSERT_TRUE(t_pb.types(5).has_scalar_type());
        ASSERT_EQ(TPrimitiveType::VARCHAR, t_pb.types(5).scalar_type().type());
        ASSERT_EQ(10, t_pb.types(5).scalar_type().len());
    }
}

// NOLINTNEXTLINE
TEST_F(TypeDescriptorTest, test_debug_string) {
    // INT
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_INT;
        EXPECT_EQ("INT", t.debug_string());
    }
    // CHAR(10)
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_CHAR;
        t.len = 10;
        EXPECT_EQ("CHAR(10)", t.debug_string());
    }
    // VARCHAR(10)
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_VARCHAR;
        t.len = 10;
        EXPECT_EQ("VARCHAR(10)", t.debug_string());
    }
    // DECIMAL(15, 5)
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_DECIMAL;
        t.precision = 15;
        t.scale = 5;
        EXPECT_EQ("DECIMAL(15, 5)", t.debug_string());
    }
    // DECIMALV2(15, 5)
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_DECIMALV2;
        t.precision = 15;
        t.scale = 5;
        EXPECT_EQ("DECIMALV2(15, 5)", t.debug_string());
    }
    // ARRAY<VARCHAR(5)>
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_ARRAY;
        t.children.emplace_back();
        t.children.back().type = LogicalType::TYPE_VARCHAR;
        t.children.back().len = 5;
        EXPECT_EQ("ARRAY<VARCHAR(5)>", t.debug_string());
    }
    // MAP<INT, VARCHAR(10)>
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_MAP;
        t.children.resize(2);
        t.children[0].type = LogicalType::TYPE_INT;
        t.children[1].type = LogicalType::TYPE_VARCHAR;
        t.children[1].len = 10;
        EXPECT_EQ("MAP<INT, VARCHAR(10)>", t.debug_string());
    }
    // struct{name VARCHAR(20), age INT}
    {
        TypeDescriptor t;
        t.type = LogicalType::TYPE_STRUCT;
        t.field_names = {"name", "age"};
        t.children.resize(2);
        t.children[0].type = LogicalType::TYPE_VARCHAR;
        t.children[0].len = 20;
        t.children[1].type = LogicalType::TYPE_INT;
        EXPECT_EQ("STRUCT{name VARCHAR(20), age INT}", t.debug_string());
    }
}

TEST_F(TypeDescriptorTest, test_promote_types) {
    std::vector<std::tuple<TypeDescriptor, TypeDescriptor, TypeDescriptor>> cases = {
            // input1, input2, output
            {TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_BIGINT),
             TypeDescriptor::from_logical_type(TYPE_BIGINT)},

            {TypeDescriptor::from_logical_type(TYPE_FLOAT), TypeDescriptor::from_logical_type(TYPE_DOUBLE),
             TypeDescriptor::from_logical_type(TYPE_DOUBLE)},

            {TypeDescriptor::from_logical_type(TYPE_FLOAT), TypeDescriptor::from_logical_type(TYPE_BIGINT),
             TypeDescriptor::from_logical_type(TYPE_DOUBLE)},

            {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 2),
             TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 4, 3),
             TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 6, 3)},

            {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 2),
             TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 4, 1),
             TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 2)},

            {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 2),
             TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 17, 1),
             TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 2)},

            {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 2),
             TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 17, 0),
             TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 19, 2)},

            {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 38),
             TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 0),
             TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)},

            {TypeDescriptor::create_varchar_type(10), TypeDescriptor::create_varchar_type(20),
             TypeDescriptor::create_varchar_type(20)},

            {TypeDescriptor::create_char_type(10), TypeDescriptor::create_char_type(20),
             TypeDescriptor::create_char_type(20)},

            {TypeDescriptor::create_varbinary_type(10), TypeDescriptor::create_varbinary_type(20),
             TypeDescriptor::create_varbinary_type(20)},

            {TypeDescriptor::from_logical_type(TYPE_JSON), TypeDescriptor::from_logical_type(TYPE_BIGINT),
             TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)}};
    for (const auto& tuple : cases) {
        EXPECT_TRUE(TypeDescriptor::promote_types(std::get<0>(tuple), std::get<1>(tuple)) == std::get<2>(tuple));
    }
}

} // namespace starrocks
