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

#include "storage/del_file_stream_converter.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/schema.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "serde/column_array_serde.h"
#include "storage/primary_key_encoder.h"
#include "types/datum.h"

namespace starrocks {

namespace {

// Build a primary-key schema with the given single PK column type. Mirrors the helper
// used by primary_key_encoder_test.cpp.
SchemaPtr create_single_pk_schema(LogicalType type) {
    Fields fields;
    auto* fd = new Field(0, "pk", type, false);
    fd->set_is_key(true);
    fd->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    fd->set_uid(0);
    fields.emplace_back(fd);
    return std::make_shared<Schema>(std::move(fields), PRIMARY_KEYS, std::vector<ColumnId>{0});
}

// Serialize a column with ColumnArraySerde into a string buffer.
std::string serialize_column(const Column& col) {
    int64_t max_size = serde::ColumnArraySerde::max_serialized_size(col);
    std::string buf;
    buf.resize(static_cast<size_t>(max_size));
    auto* begin = reinterpret_cast<uint8_t*>(buf.data());
    auto status_or_end = serde::ColumnArraySerde::serialize(col, begin);
    CHECK(status_or_end.ok()) << status_or_end.status();
    buf.resize(status_or_end.value() - begin);
    return buf;
}

// Encode the given source-typed PK column into V2-shaped BinaryColumn using PrimaryKeyEncoder.
// This is the reference encoder used in expectations.
MutableColumnPtr encode_to_v2(const Schema& schema, MutableColumnPtr src_col) {
    MutableColumnPtr dst;
    CHECK(PrimaryKeyEncoder::create_column(schema, &dst, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2).ok());
    Columns wrap;
    const size_t n = src_col->size();
    wrap.emplace_back(std::move(src_col));
    Chunk tmp_chunk(std::move(wrap), std::make_shared<Schema>(schema));
    PrimaryKeyEncoder::encode(schema, tmp_chunk, 0, n, dst.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    return dst;
}

// Drive the converter end-to-end with the given V1 input bytes and return the output bytes.
StatusOr<std::string> run_converter(const SchemaPtr& pkey_schema, const std::string& v1_bytes, uint64_t declared_size,
                                    PrimaryKeyEncodingType source_enc = PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                                    PrimaryKeyEncodingType target_enc = PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2,
                                    size_t chunk_size = 0) {
    MemoryFileSystem fs;
    const std::string path = "/del-test";
    ASSIGN_OR_RETURN(auto wfile, fs.new_writable_file(path));
    DelFileStreamConverter converter("test.del", declared_size, std::move(wfile), pkey_schema, source_enc, target_enc);
    if (chunk_size == 0 || chunk_size >= v1_bytes.size()) {
        RETURN_IF_ERROR(converter.append(v1_bytes.data(), v1_bytes.size()));
    } else {
        for (size_t off = 0; off < v1_bytes.size(); off += chunk_size) {
            size_t n = std::min(chunk_size, v1_bytes.size() - off);
            RETURN_IF_ERROR(converter.append(v1_bytes.data() + off, n));
        }
    }
    RETURN_IF_ERROR(converter.close());

    ASSIGN_OR_RETURN(auto rfile, fs.new_random_access_file(path));
    ASSIGN_OR_RETURN(std::string output, rfile->read_all());
    return output;
}

template <typename CppT>
void check_v1_to_v2(LogicalType pk_type, const std::vector<CppT>& values, size_t chunk_size = 0) {
    auto schema = create_single_pk_schema(pk_type);

    // Build V1 input: typed column, serialized via ColumnArraySerde.
    MutableColumnPtr v1_col;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*schema, &v1_col, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1).ok());
    for (const CppT& v : values) {
        Datum d;
        d.set<CppT>(v);
        v1_col->append_datum(d);
    }
    const std::string v1_bytes = serialize_column(*v1_col);

    // Expected V2 output bytes: encode a clone of the V1 column to V2 BinaryColumn, then serialize.
    auto expected_v2_col = encode_to_v2(*schema, v1_col->clone());
    const std::string expected_v2_bytes = serialize_column(*expected_v2_col);

    auto status_or_out = run_converter(schema, v1_bytes, v1_bytes.size(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                                       PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2, chunk_size);
    ASSERT_TRUE(status_or_out.ok()) << status_or_out.status();
    ASSERT_EQ(expected_v2_bytes, status_or_out.value());
}

} // namespace

class DelFileStreamConverterTest : public ::testing::Test {};

// Golden path: single INT PK, V1 source -> V2 target.
TEST_F(DelFileStreamConverterTest, V1ToV2_SingleInt) {
    check_v1_to_v2<int32_t>(TYPE_INT,
                            {42, 100, -5, 0, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()});
}

// Chunked append should produce the same output as a single-shot append.
TEST_F(DelFileStreamConverterTest, ChunkedAppend) {
    // Pick a chunk size smaller than the full payload to exercise the multi-append code path.
    check_v1_to_v2<int32_t>(TYPE_INT, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, /*chunk_size=*/4);
}

TEST_F(DelFileStreamConverterTest, V1ToV2_SingleBigInt) {
    check_v1_to_v2<int64_t>(TYPE_BIGINT, {123456789012LL, -42, 0, std::numeric_limits<int64_t>::max()});
}

// Declared input size shorter than what is appended -> Corruption on append (overflow guard).
TEST_F(DelFileStreamConverterTest, AppendOverflow) {
    auto schema = create_single_pk_schema(TYPE_INT);
    MemoryFileSystem fs;
    auto wfile_or = fs.new_writable_file("/del-overflow");
    ASSERT_TRUE(wfile_or.ok());

    const uint64_t declared = 8;
    DelFileStreamConverter converter("t.del", declared, std::move(wfile_or.value()), schema,
                                     PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                                     PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    std::string too_much(declared + 1, 0);
    auto st = converter.append(too_much.data(), too_much.size());
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption()) << st;
}

// Closing with fewer bytes than declared -> Corruption.
TEST_F(DelFileStreamConverterTest, ShortInput) {
    auto schema = create_single_pk_schema(TYPE_INT);
    MemoryFileSystem fs;
    auto wfile_or = fs.new_writable_file("/del-short");
    ASSERT_TRUE(wfile_or.ok());

    const uint64_t declared = 16;
    DelFileStreamConverter converter("t.del", declared, std::move(wfile_or.value()), schema,
                                     PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                                     PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    std::string partial(declared - 1, 0);
    ASSERT_TRUE(converter.append(partial.data(), partial.size()).ok());
    auto st = converter.close();
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption()) << st;
}

// Feeding garbage bytes for the correct declared size -> ColumnArraySerde::deserialize fails.
TEST_F(DelFileStreamConverterTest, CorruptInput) {
    auto schema = create_single_pk_schema(TYPE_INT);
    // 8 bytes of garbage, doesn't look like a valid serialized Int32Column.
    std::string garbage = "\xff\xff\xff\xff\xde\xad\xbe\xef";
    auto status_or_out = run_converter(schema, garbage, garbage.size());
    ASSERT_FALSE(status_or_out.ok());
}

} // namespace starrocks
