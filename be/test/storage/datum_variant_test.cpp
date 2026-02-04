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

#include "storage/datum_variant.h"

#include <gtest/gtest.h>

#include <string>

#include "base/testutil/assert.h"
#include "runtime/types.h"

namespace starrocks {

TEST(DatumVariantTest, FromProtoNullValueAndTypeInfo) {
    VariantPB variant_pb;
    TypeDescriptor type_desc(TYPE_INT);
    variant_pb.mutable_type()->CopyFrom(type_desc.to_protobuf());
    variant_pb.set_variant_type(VariantTypePB::NULL_VALUE);

    Datum datum;
    TypeDescriptor out_type_desc;
    TypeInfoPtr out_type_info;
    ASSERT_OK(DatumVariant::from_proto(variant_pb, &datum, &out_type_desc, &out_type_info));
    ASSERT_TRUE(datum.is_null());
    ASSERT_EQ(TYPE_INT, out_type_desc.type);
    ASSERT_TRUE(out_type_info != nullptr);
}

TEST(DatumVariantTest, FromProtoMissingType) {
    VariantPB variant_pb;
    Datum datum;
    auto status = DatumVariant::from_proto(variant_pb, &datum);
    ASSERT_TRUE(status.is_invalid_argument());
    ASSERT_EQ("No type in variant", status.message());
}

TEST(DatumVariantTest, FromProtoUnsupportedType) {
    VariantPB variant_pb;
    PTypeDesc type_desc;
    auto* node = type_desc.add_types();
    node->set_type(TTypeNodeType::SCALAR);
    node->mutable_scalar_type()->set_type(TPrimitiveType::INVALID_TYPE);
    variant_pb.mutable_type()->CopyFrom(type_desc);
    variant_pb.set_variant_type(VariantTypePB::NORMAL_VALUE);
    variant_pb.set_value("1");

    Datum datum;
    auto status = DatumVariant::from_proto(variant_pb, &datum);
    ASSERT_TRUE(status.is_not_supported());
    ASSERT_NE(std::string::npos, status.message().find("not supported"));
}

TEST(DatumVariantTest, FromProtoInvalidVariantValue) {
    VariantPB variant_pb;
    TypeDescriptor type_desc(TYPE_INT);
    variant_pb.mutable_type()->CopyFrom(type_desc.to_protobuf());
    variant_pb.set_variant_type(VariantTypePB::NORMAL_VALUE);

    Datum datum;
    auto status = DatumVariant::from_proto(variant_pb, &datum);
    ASSERT_TRUE(status.is_invalid_argument());
    ASSERT_EQ("Invalid variant value", status.message());
}

} // namespace starrocks
