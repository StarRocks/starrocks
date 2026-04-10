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

#include "formats/avro/cpp/utils.h"

#include <gtest/gtest.h>

#include <avrocpp/LogicalType.hh>

#include "base/testutil/assert.h"

namespace starrocks {

class UtilsTest : public ::testing::Test {};

TEST_F(UtilsTest, test_logical_type_to_string) {
    ASSERT_EQ("none", AvroUtils::logical_type_to_string(avro::LogicalType(avro::LogicalType::NONE)));
    avro::LogicalType decimal_type = avro::LogicalType(avro::LogicalType::DECIMAL);
    decimal_type.setPrecision(10);
    decimal_type.setScale(2);
    ASSERT_EQ("decimal(10,2)", AvroUtils::logical_type_to_string(decimal_type));
    ASSERT_EQ("date", AvroUtils::logical_type_to_string(avro::LogicalType(avro::LogicalType::DATE)));
    ASSERT_EQ("time-millis", AvroUtils::logical_type_to_string(avro::LogicalType(avro::LogicalType::TIME_MILLIS)));
    ASSERT_EQ("time-micros", AvroUtils::logical_type_to_string(avro::LogicalType(avro::LogicalType::TIME_MICROS)));
    ASSERT_EQ("timestamp-millis",
              AvroUtils::logical_type_to_string(avro::LogicalType(avro::LogicalType::TIMESTAMP_MILLIS)));
    ASSERT_EQ("timestamp-micros",
              AvroUtils::logical_type_to_string(avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS)));
    ASSERT_EQ("duration", AvroUtils::logical_type_to_string(avro::LogicalType(avro::LogicalType::DURATION)));
    ASSERT_EQ("uuid", AvroUtils::logical_type_to_string(avro::LogicalType(avro::LogicalType::UUID)));
}

} // namespace starrocks
