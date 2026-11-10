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

#include "connector/hive/paimon/paimon_blob_converter.h"

#include <arrow/builder.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "base/testutil/assert.h"
#include "column/file_column.h"
#include "column/nullable_column.h"

namespace starrocks {

namespace {

constexpr int8_t kVersion = 2;
constexpr int64_t kMagic = 0x424C4F4244455343LL; // "BLOBDESC"

template <typename T>
void append_le(std::string* out, T value) {
    char buf[sizeof(T)];
    std::memcpy(buf, &value, sizeof(T));
    out->append(buf, sizeof(T));
}

// Serializes a paimon BlobDescriptor:
//   version:int8 | magic:int64 | uri_len:int32 | uri | offset:int64 | length:int64
// `uri_len` defaults to the real uri size; tests pass a different value to build corrupt inputs.
std::string make_descriptor(std::string_view uri, int64_t offset, int64_t length, int8_t version = kVersion,
                            int64_t magic = kMagic, int32_t uri_len = -1) {
    std::string out;
    append_le<int8_t>(&out, version);
    append_le<int64_t>(&out, magic);
    append_le<int32_t>(&out, uri_len < 0 ? static_cast<int32_t>(uri.size()) : uri_len);
    out.append(uri.data(), uri.size());
    append_le<int64_t>(&out, offset);
    append_le<int64_t>(&out, length);
    return out;
}

template <typename Builder>
std::shared_ptr<arrow::Array> make_array(const std::vector<std::optional<std::string>>& values) {
    Builder builder;
    for (const auto& value : values) {
        if (value.has_value()) {
            EXPECT_TRUE(builder.Append(std::string_view(*value)).ok());
        } else {
            EXPECT_TRUE(builder.AppendNull().ok());
        }
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

NullableColumn::MutablePtr make_file_column() {
    return NullableColumn::create(FileColumn::create(), NullColumn::create());
}

std::string reference_row(const std::string& uri, int64_t offset, int64_t size) {
    return "{uri:'" + uri + "',offset:" + std::to_string(offset) + ",size:" + std::to_string(size) +
           ",content_type:NULL,checksum:NULL,inline:NULL}";
}

std::string inline_row(const std::string& bytes) {
    return "{uri:NULL,offset:NULL,size:NULL,content_type:NULL,checksum:NULL,inline:'" + bytes + "'}";
}

} // namespace

TEST(PaimonBlobConverterTest, descriptor_becomes_reference_row) {
    auto array = make_array<arrow::LargeBinaryBuilder>({
            make_descriptor("s3://warehouse/db/t/blob/data-0.blob", 4, 8),
            // length -1 means "to the end of the file" and is surfaced as stored.
            make_descriptor("hdfs://ns/warehouse/db/t/blob/data-1.blob", 0, -1),
    });

    auto dst = make_file_column();
    ASSERT_OK(append_paimon_blob_to_file_column(array.get(), 0, 2, dst.get()));
    ASSERT_EQ(2, dst->size());
    ASSERT_EQ(reference_row("s3://warehouse/db/t/blob/data-0.blob", 4, 8), dst->debug_item(0));
    ASSERT_EQ(reference_row("hdfs://ns/warehouse/db/t/blob/data-1.blob", 0, -1), dst->debug_item(1));
}

TEST(PaimonBlobConverterTest, payload_becomes_inline_row) {
    auto array = make_array<arrow::LargeBinaryBuilder>({std::string("png-bytes"), std::string("")});

    auto dst = make_file_column();
    ASSERT_OK(append_paimon_blob_to_file_column(array.get(), 0, 2, dst.get()));
    ASSERT_EQ(2, dst->size());
    ASSERT_EQ(inline_row("png-bytes"), dst->debug_item(0));
    ASSERT_EQ(inline_row(""), dst->debug_item(1));
}

TEST(PaimonBlobConverterTest, malformed_descriptor_falls_back_to_inline) {
    const std::string wrong_version = make_descriptor("uri", 1, 2, /*version=*/1);
    const std::string wrong_magic = make_descriptor("uri", 1, 2, kVersion, /*magic=*/0x1234);
    const std::string uri_len_too_large = make_descriptor("uri", 1, 2, kVersion, kMagic, /*uri_len=*/64);
    const std::string uri_len_too_small = make_descriptor("uri", 1, 2, kVersion, kMagic, /*uri_len=*/1);
    const std::string negative_offset = make_descriptor("uri", -1, 2);
    const std::string negative_length = make_descriptor("uri", 1, -2);
    // Shorter than the fixed header + trailer, so it can never be a descriptor.
    const std::string too_short(8, 'x');

    std::vector<std::optional<std::string>> values{wrong_version,     wrong_magic,     uri_len_too_large,
                                                   uri_len_too_small, negative_offset, negative_length,
                                                   too_short};
    auto array = make_array<arrow::LargeBinaryBuilder>(values);

    auto dst = make_file_column();
    ASSERT_OK(append_paimon_blob_to_file_column(array.get(), 0, values.size(), dst.get()));
    ASSERT_EQ(values.size(), dst->size());
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(inline_row(*values[i]), dst->debug_item(i)) << "row " << i;
    }
}

TEST(PaimonBlobConverterTest, null_values_and_row_range) {
    auto array = make_array<arrow::LargeBinaryBuilder>({
            std::string("skipped"),
            std::nullopt,
            make_descriptor("s3://w/db/t/blob/f.blob", 16, 32),
            std::string("payload"),
    });

    auto dst = make_file_column();
    // Only rows [1, 4) are appended; row 0 stays out.
    ASSERT_OK(append_paimon_blob_to_file_column(array.get(), 1, 3, dst.get()));
    ASSERT_EQ(3, dst->size());
    ASSERT_TRUE(dst->is_null(0));
    ASSERT_EQ("NULL", dst->debug_item(0));
    ASSERT_EQ(reference_row("s3://w/db/t/blob/f.blob", 16, 32), dst->debug_item(1));
    ASSERT_EQ(inline_row("payload"), dst->debug_item(2));

    // Appending again keeps the earlier rows in place.
    ASSERT_OK(append_paimon_blob_to_file_column(array.get(), 0, 1, dst.get()));
    ASSERT_EQ(4, dst->size());
    ASSERT_EQ(inline_row("skipped"), dst->debug_item(3));
}

TEST(PaimonBlobConverterTest, accepts_plain_binary_array) {
    auto array = make_array<arrow::BinaryBuilder>({make_descriptor("s3://w/db/t/blob/f.blob", 1, 2), std::nullopt});

    auto dst = make_file_column();
    ASSERT_OK(append_paimon_blob_to_file_column(array.get(), 0, 2, dst.get()));
    ASSERT_EQ(2, dst->size());
    ASSERT_EQ(reference_row("s3://w/db/t/blob/f.blob", 1, 2), dst->debug_item(0));
    ASSERT_TRUE(dst->is_null(1));
}

TEST(PaimonBlobConverterTest, rejects_bad_inputs) {
    auto dst = make_file_column();

    Status st = append_paimon_blob_to_file_column(nullptr, 0, 1, dst.get());
    ASSERT_TRUE(st.is_internal_error()) << st;
    ASSERT_NE(std::string::npos, st.message().find("missing from the arrow batch")) << st;

    auto array = make_array<arrow::LargeBinaryBuilder>({std::string("payload")});
    auto not_nullable = FileColumn::create();
    st = append_paimon_blob_to_file_column(array.get(), 0, 1, not_nullable.get());
    ASSERT_TRUE(st.is_internal_error()) << st;
    ASSERT_NE(std::string::npos, st.message().find("must be nullable")) << st;

    arrow::Int32Builder int_builder;
    ASSERT_TRUE(int_builder.Append(7).ok());
    std::shared_ptr<arrow::Array> int_array;
    ASSERT_TRUE(int_builder.Finish(&int_array).ok());
    st = append_paimon_blob_to_file_column(int_array.get(), 0, 1, dst.get());
    ASSERT_TRUE(st.is_internal_error()) << st;
    ASSERT_NE(std::string::npos, st.message().find("unexpected arrow type")) << st;

    ASSERT_EQ(0, dst->size());
}

} // namespace starrocks
