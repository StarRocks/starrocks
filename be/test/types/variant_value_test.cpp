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

#include "types/variant_value.h"

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

namespace starrocks {
namespace {

std::string make_primitive_value(VariantType type, std::string_view payload = {}) {
    std::string value;
    value.reserve(1 + payload.size());
    value.push_back(static_cast<char>(static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift));
    value.append(payload.data(), payload.size());
    return value;
}

std::string make_encoded_variant(std::string_view metadata, std::string_view value) {
    const uint32_t variant_size = static_cast<uint32_t>(metadata.size() + value.size());
    std::string encoded(sizeof(uint32_t) + variant_size, 0);
    std::memcpy(encoded.data(), &variant_size, sizeof(uint32_t));
    std::memcpy(encoded.data() + sizeof(uint32_t), metadata.data(), metadata.size());
    std::memcpy(encoded.data() + sizeof(uint32_t) + metadata.size(), value.data(), value.size());
    return encoded;
}

uint32_t decode_size_header(const uint8_t* bytes) {
    uint32_t size = 0;
    std::memcpy(&size, bytes, sizeof(uint32_t));
    return size;
}

} // namespace

TEST(VariantRowValueTypeCoreTest, CreateFromNullMetadataReturnsNullVariant) {
    auto row = VariantRowValue::create("", "");
    ASSERT_TRUE(row.ok());
    EXPECT_EQ(row->get_metadata().raw(), VariantMetadata::kEmptyMetadata);
    EXPECT_EQ(row->get_value().raw(), VariantValue::kEmptyValue);
}

TEST(VariantRowValueTypeCoreTest, CreateFromSliceParsesMetadataAndValue) {
    std::string value = make_primitive_value(VariantType::INT8, std::string(1, static_cast<char>(42)));
    std::string encoded = make_encoded_variant(VariantMetadata::kEmptyMetadata, value);

    auto row = VariantRowValue::create(Slice(encoded));
    ASSERT_TRUE(row.ok());
    EXPECT_EQ(row->get_metadata().raw(), VariantMetadata::kEmptyMetadata);
    EXPECT_EQ(row->get_value().raw(), value);
}

TEST(VariantRowValueTypeCoreTest, CreateFromSliceRejectsInvalidInput) {
    Slice null_data_slice(static_cast<const char*>(nullptr), sizeof(uint32_t));
    auto null_data = VariantRowValue::create(null_data_slice);
    ASSERT_FALSE(null_data.ok());
    EXPECT_TRUE(null_data.status().to_string().find("null data pointer") != std::string::npos);

    Slice short_slice("");
    auto short_data = VariantRowValue::create(short_slice);
    ASSERT_FALSE(short_data.ok());
    EXPECT_TRUE(short_data.status().to_string().find("too small") != std::string::npos);

    constexpr uint32_t oversized = VariantRowValue::kMaxVariantSize + 1;
    std::string oversized_header(sizeof(uint32_t), 0);
    std::memcpy(oversized_header.data(), &oversized, sizeof(uint32_t));
    auto too_large = VariantRowValue::create(Slice(oversized_header));
    ASSERT_FALSE(too_large.ok());
    EXPECT_TRUE(too_large.status().to_string().find("exceeds maximum limit") != std::string::npos);
}

TEST(VariantRowValueTypeCoreTest, ValidateMetadataRejectsInvalidHeader) {
    auto short_meta = VariantRowValue::validate_metadata(std::string_view("\x01\x00", 2));
    EXPECT_FALSE(short_meta.ok());

    const char v2_meta[] = {0x2, 0x0, 0x0};
    auto unsupported = VariantRowValue::validate_metadata(std::string_view(v2_meta, sizeof(v2_meta)));
    EXPECT_FALSE(unsupported.ok());
}

TEST(VariantRowValueTypeCoreTest, SerializeRoundTrip) {
    std::string value = make_primitive_value(VariantType::INT32, std::string_view("\x01\x00\x00\x00", 4));
    auto row = VariantRowValue::create(VariantMetadata::kEmptyMetadata, value);
    ASSERT_TRUE(row.ok());

    std::vector<uint8_t> buffer(row->serialize_size(), 0);
    size_t written = row->serialize(buffer.data());
    ASSERT_EQ(written, row->serialize_size());

    const uint32_t payload_size = decode_size_header(buffer.data());
    ASSERT_EQ(payload_size, VariantMetadata::kEmptyMetadata.size() + value.size());

    std::string encoded_expected = make_encoded_variant(VariantMetadata::kEmptyMetadata, value);
    std::string encoded_actual(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    EXPECT_EQ(encoded_actual, encoded_expected);
}

TEST(VariantRowValueTypeCoreTest, SerializeDefaultVariantUsesCanonicalEmptyPayload) {
    VariantRowValue row;
    std::vector<uint8_t> buffer(row.serialize_size(), 0);
    size_t written = row.serialize(buffer.data());
    ASSERT_EQ(written, row.serialize_size());

    const uint32_t payload_size = decode_size_header(buffer.data());
    ASSERT_EQ(payload_size, VariantMetadata::kEmptyMetadata.size() + VariantValue::kEmptyValue.size());

    std::string payload(reinterpret_cast<const char*>(buffer.data() + sizeof(uint32_t)), payload_size);
    EXPECT_EQ(payload, std::string(VariantMetadata::kEmptyMetadata) + std::string(VariantValue::kEmptyValue));
}

TEST(VariantRowValueTypeCoreTest, CopyAndMoveKeepViewsCorrectlyBound) {
    std::string value = make_primitive_value(VariantType::INT16, std::string_view("\x39\x30", 2));
    VariantRowValue original(VariantMetadata::kEmptyMetadata, value);

    VariantRowValue copied(original);
    EXPECT_EQ(copied.get_metadata().raw(), original.get_metadata().raw());
    EXPECT_EQ(copied.get_value().raw(), original.get_value().raw());
    EXPECT_NE(copied.get_metadata().raw().data(), original.get_metadata().raw().data());
    EXPECT_NE(copied.get_value().raw().data(), original.get_value().raw().data());

    VariantRowValue moved(std::move(copied));
    EXPECT_EQ(moved.get_metadata().raw(), original.get_metadata().raw());
    EXPECT_EQ(moved.get_value().raw(), original.get_value().raw());
    EXPECT_EQ(copied.get_metadata().raw(), VariantMetadata::kEmptyMetadata);
    EXPECT_EQ(copied.get_value().raw(), VariantValue::kEmptyValue);
}

TEST(VariantRowValueTypeCoreTest, AssignmentOperatorsPreserveValueAndMovedFromState) {
    std::string src_value = make_primitive_value(VariantType::INT8, std::string(1, static_cast<char>(7)));
    VariantRowValue source(VariantMetadata::kEmptyMetadata, src_value);

    std::string dst_value = make_primitive_value(VariantType::INT8, std::string(1, static_cast<char>(9)));
    VariantRowValue destination(VariantMetadata::kEmptyMetadata, dst_value);

    destination = source;
    EXPECT_EQ(destination.get_metadata().raw(), source.get_metadata().raw());
    EXPECT_EQ(destination.get_value().raw(), source.get_value().raw());
    EXPECT_NE(destination.get_metadata().raw().data(), source.get_metadata().raw().data());

    VariantRowValue move_target(VariantMetadata::kEmptyMetadata, dst_value);
    move_target = std::move(source);
    EXPECT_EQ(move_target.get_value().raw(), src_value);
    EXPECT_EQ(source.get_metadata().raw(), VariantMetadata::kEmptyMetadata);
    EXPECT_EQ(source.get_value().raw(), VariantValue::kEmptyValue);
}

TEST(VariantRowValueTypeCoreTest, CompareOperatorsFollowRawOrdering) {
    std::string low_value = make_primitive_value(VariantType::INT8, std::string(1, static_cast<char>(1)));
    std::string high_value = make_primitive_value(VariantType::INT8, std::string(1, static_cast<char>(2)));

    VariantRowValue low(VariantMetadata::kEmptyMetadata, low_value);
    VariantRowValue high(VariantMetadata::kEmptyMetadata, high_value);

    EXPECT_LT(compare(low, high), 0);
    EXPECT_LE(compare(low, high), 0);
    EXPECT_GT(compare(high, low), 0);
    EXPECT_GE(compare(high, low), 0);
    EXPECT_NE(compare(low, high), 0);

    VariantRowValue low_copy(VariantMetadata::kEmptyMetadata, low_value);
    EXPECT_EQ(compare(low, low_copy), 0);
}

} // namespace starrocks
