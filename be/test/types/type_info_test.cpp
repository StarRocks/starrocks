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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/storage_types_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <vector>

#include "base/string/slice.h"
#include "storage/types.h"
#include "types/type_traits.h"

namespace starrocks {

class TypeInfoTest : public testing::Test {
public:
    TypeInfoTest() = default;
    ~TypeInfoTest() override = default;
};

class LocalTypeInfoAllocator {
public:
    static uint8_t* allocate(void* ctx, size_t size) {
        auto* self = static_cast<LocalTypeInfoAllocator*>(ctx);
        self->_buffers.emplace_back(std::make_unique<uint8_t[]>(size));
        return self->_buffers.back().get();
    }

    TypeInfoAllocator make_allocator() { return TypeInfoAllocator{this, &LocalTypeInfoAllocator::allocate}; }

private:
    std::vector<std::unique_ptr<uint8_t[]>> _buffers;
};

template <LogicalType field_type>
void common_test(typename TypeTraits<field_type>::CppType src_val) {
    TypeInfoPtr type = get_type_info(field_type);

    ASSERT_EQ(field_type, type->type());
    ASSERT_EQ(sizeof(src_val), type->size());
    {
        typename TypeTraits<field_type>::CppType dst_val;
        LocalTypeInfoAllocator local_allocator;
        auto allocator = local_allocator.make_allocator();
        type->deep_copy((char*)&dst_val, (char*)&src_val, &allocator);
    }
    {
        typename TypeTraits<field_type>::CppType dst_val;
        type->direct_copy((char*)&dst_val, (char*)&src_val);
    }
    // test min
    {
        typename TypeTraits<field_type>::CppType dst_val;
        type->set_to_min((char*)&dst_val);
    }
    // test max
    {
        typename TypeTraits<field_type>::CppType dst_val;
        type->set_to_max((char*)&dst_val);
        // NOTE: bool input is true, this will return 0
    }
}

template <LogicalType fieldType>
void test_char(Slice src_val) {
    TypeInfoPtr type = get_type_info(TYPE_VARCHAR);

    ASSERT_EQ(type->type(), fieldType);
    ASSERT_EQ(sizeof(src_val), type->size());
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        LocalTypeInfoAllocator local_allocator;
        auto allocator = local_allocator.make_allocator();
        type->deep_copy((char*)&dst_val, (char*)&src_val, &allocator);
    }
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        type->direct_copy((char*)&dst_val, (char*)&src_val);
    }
    // test min
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        dst_val.size = 0;
    }
    // test max
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        memset(buf, 0xFF, 64);
    }
}

template <>
void common_test<TYPE_CHAR>(Slice src_val) {
    test_char<TYPE_VARCHAR>(src_val);
}

template <>
void common_test<TYPE_VARCHAR>(Slice src_val) {
    test_char<TYPE_VARCHAR>(src_val);
}

TEST(TypeInfoTest, copy_and_equal) {
    common_test<TYPE_BOOLEAN>(true);
    common_test<TYPE_TINYINT>(112);
    common_test<TYPE_SMALLINT>(54321);
    common_test<TYPE_INT>(-123454321);
    common_test<TYPE_UNSIGNED_INT>(1234543212L);
    common_test<TYPE_BIGINT>(123454321123456789L);
    __int128 int128_val = 1234567899L;
    common_test<TYPE_LARGEINT>(int128_val);
    common_test<TYPE_FLOAT>(1.11);
    common_test<TYPE_DOUBLE>(12221.11);
    decimal12_t decimal_val(123, 2345);
    common_test<TYPE_DECIMAL>(decimal_val);

    common_test<TYPE_DATE_V1>((1988 << 9) | (2 << 5) | 1);
    common_test<TYPE_DATETIME_V1>(19880201010203L);

    Slice slice("12345abcde");
    common_test<TYPE_CHAR>(slice);
    common_test<TYPE_VARCHAR>(slice);
}

} // namespace starrocks
