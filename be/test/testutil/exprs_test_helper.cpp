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

#include "testutil/exprs_test_helper.h"

namespace starrocks {
const TTypeDesc ExprsTestHelper::SmallIntTTypeDesc = ExprsTestHelper::create_scalar_type_desc(TPrimitiveType::SMALLINT);
const TTypeDesc ExprsTestHelper::IntTTypeDesc = ExprsTestHelper::create_scalar_type_desc(TPrimitiveType::INT);
const TTypeDesc ExprsTestHelper::BigIntTTypeDesc = ExprsTestHelper::create_scalar_type_desc(TPrimitiveType::BIGINT);
const TTypeDesc ExprsTestHelper::DateTTypeDesc = ExprsTestHelper::create_scalar_type_desc(TPrimitiveType::DATE);
const TTypeDesc ExprsTestHelper::DateTimeTTypeDesc = ExprsTestHelper::create_scalar_type_desc(TPrimitiveType::DATETIME);
const TTypeDesc ExprsTestHelper::Decimal128TTypeDesc =
        ExprsTestHelper::create_decimal_type_desc(TPrimitiveType::DECIMAL128, 27, 9);
const TTypeDesc ExprsTestHelper::VarcharTTypeDesc =
        ExprsTestHelper::create_varchar_type_desc(TypeDescriptor::MAX_VARCHAR_LENGTH);
} // namespace starrocks