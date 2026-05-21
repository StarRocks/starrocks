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

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/schema.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "serde/column_array_serde.h"
#include "storage/primary_key_encoder.h"
#include "storage/tablet_schema.h"
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

// Maps LogicalType -> the protobuf-side type-name string used by TabletColumnPB.
const char* pk_type_name(LogicalType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return "BOOLEAN";
    case TYPE_TINYINT:
        return "TINYINT";
    case TYPE_SMALLINT:
        return "SMALLINT";
    case TYPE_INT:
        return "INT";
    case TYPE_BIGINT:
        return "BIGINT";
    case TYPE_LARGEINT:
        return "LARGEINT";
    case TYPE_DATE:
        return "DATE";
    case TYPE_DATETIME:
        return "DATETIME";
    case TYPE_VARCHAR:
        return "VARCHAR";
    default:
        return "UNKNOWN";
    }
}

// Build a TabletSchema with the given PK column types. Each column is keyed.
std::shared_ptr<TabletSchema> create_tablet_schema_with_pk_columns(const std::vector<LogicalType>& pk_types) {
    TabletSchemaPB pb;
    pb.set_keys_type(PRIMARY_KEYS);
    pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    for (size_t i = 0; i < pk_types.size(); ++i) {
        auto* col = pb.add_column();
        col->set_unique_id(static_cast<uint32_t>(i));
        col->set_name("c" + std::to_string(i));
        col->set_type(pk_type_name(pk_types[i]));
        if (pk_types[i] == TYPE_VARCHAR) {
            col->set_length(32);
        }
        col->set_is_key(true);
        col->set_is_nullable(false);
    }
    return std::make_shared<TabletSchema>(pb);
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

// Zero-byte append should be a fast no-op (covers the size==0 early return in append()).
TEST_F(DelFileStreamConverterTest, AppendZeroSize) {
    auto schema = create_single_pk_schema(TYPE_INT);
    MemoryFileSystem fs;
    auto wfile_or = fs.new_writable_file("/del-zero");
    ASSERT_TRUE(wfile_or.ok());

    // Declared input is a valid 1-row Int32Column payload.
    MutableColumnPtr v1_col;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*schema, &v1_col, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1).ok());
    Datum d;
    d.set<int32_t>(42);
    v1_col->append_datum(d);
    const std::string v1_bytes = serialize_column(*v1_col);

    DelFileStreamConverter converter("zero.del", v1_bytes.size(), std::move(wfile_or.value()), schema,
                                     PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                                     PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    // A zero-byte append must not advance the buffer.
    ASSERT_OK(converter.append(nullptr, 0));
    ASSERT_OK(converter.append(v1_bytes.data(), v1_bytes.size()));
    // Another zero-byte append after a full payload is also a no-op.
    ASSERT_OK(converter.append(nullptr, 0));
    ASSERT_OK(converter.close());
}

// Declared size larger than the actual serialized PK column leaves trailing bytes that
// ColumnArraySerde::deserialize will not consume, exercising the "did not consume full
// buffer" Corruption branch in close().
TEST_F(DelFileStreamConverterTest, DeserializeLeavesTrailingBytes) {
    auto schema = create_single_pk_schema(TYPE_INT);

    MutableColumnPtr v1_col;
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(*schema, &v1_col, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1).ok());
    Datum d;
    d.set<int32_t>(7);
    v1_col->append_datum(d);
    std::string v1_bytes = serialize_column(*v1_col);
    // Append a stray byte so deserialize stops short of the buffer end.
    v1_bytes.push_back('\0');

    MemoryFileSystem fs;
    auto wfile_or = fs.new_writable_file("/del-trailing");
    ASSERT_TRUE(wfile_or.ok());
    DelFileStreamConverter converter("trailing.del", v1_bytes.size(), std::move(wfile_or.value()), schema,
                                     PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1,
                                     PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    ASSERT_OK(converter.append(v1_bytes.data(), v1_bytes.size()));
    auto st = converter.close();
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_corruption()) << st;
}

// Round-trip golden tests for every supported single non-string fixed-length PK type.
// These also incidentally cover the macro expansion in is_fixed_length_non_string_pk_type.
TEST_F(DelFileStreamConverterTest, V1ToV2_SingleBoolean) {
    check_v1_to_v2<uint8_t>(TYPE_BOOLEAN, {0, 1});
}
TEST_F(DelFileStreamConverterTest, V1ToV2_SingleTinyInt) {
    check_v1_to_v2<int8_t>(TYPE_TINYINT, {-128, -1, 0, 1, 127});
}
TEST_F(DelFileStreamConverterTest, V1ToV2_SingleSmallInt) {
    check_v1_to_v2<int16_t>(TYPE_SMALLINT, {std::numeric_limits<int16_t>::min(), 0, std::numeric_limits<int16_t>::max()});
}
TEST_F(DelFileStreamConverterTest, V1ToV2_SingleDate) {
    // DATE storage type is int32 (Julian day count). Use representative values.
    check_v1_to_v2<int32_t>(TYPE_DATE, {0, 700000, 999999});
}
TEST_F(DelFileStreamConverterTest, V1ToV2_SingleDateTime) {
    check_v1_to_v2<int64_t>(TYPE_DATETIME, {0, 20260521000000LL, std::numeric_limits<int64_t>::max()});
}

// Predicate matrix: is_single_fixed_length_non_string_primary_key(const Schema&)
// and requires_v1_to_v2_del_transcode(...) for every combination that matters.
TEST_F(DelFileStreamConverterTest, Predicates_SchemaOverload) {
    // Single fixed-length non-string types -> true.
    for (LogicalType t : {TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_DATE,
                          TYPE_DATETIME}) {
        ASSERT_TRUE(is_single_fixed_length_non_string_primary_key(*create_single_pk_schema(t)))
                << "type=" << static_cast<int>(t);
    }
    // Single VARCHAR -> false (BinaryColumn on both V1/V2, byte-compatible).
    ASSERT_FALSE(is_single_fixed_length_non_string_primary_key(*create_single_pk_schema(TYPE_VARCHAR)));

    // Composite PK -> false. Build (INT, INT) schema directly.
    Fields composite;
    {
        auto* a = new Field(0, "a", TYPE_INT, false);
        a->set_is_key(true);
        a->set_aggregate_method(STORAGE_AGGREGATE_NONE);
        auto* b = new Field(1, "b", TYPE_INT, false);
        b->set_is_key(true);
        b->set_aggregate_method(STORAGE_AGGREGATE_NONE);
        composite.emplace_back(a);
        composite.emplace_back(b);
    }
    Schema composite_schema(std::move(composite), PRIMARY_KEYS, std::vector<ColumnId>{0, 1});
    ASSERT_FALSE(is_single_fixed_length_non_string_primary_key(composite_schema));
}

TEST_F(DelFileStreamConverterTest, Predicates_TabletSchemaOverload) {
    // Single fixed-length non-string -> true.
    ASSERT_TRUE(is_single_fixed_length_non_string_primary_key(*create_tablet_schema_with_pk_columns({TYPE_INT})));
    ASSERT_TRUE(is_single_fixed_length_non_string_primary_key(*create_tablet_schema_with_pk_columns({TYPE_DATETIME})));
    // Single VARCHAR -> false.
    ASSERT_FALSE(is_single_fixed_length_non_string_primary_key(*create_tablet_schema_with_pk_columns({TYPE_VARCHAR})));
    // Composite -> false.
    ASSERT_FALSE(
            is_single_fixed_length_non_string_primary_key(*create_tablet_schema_with_pk_columns({TYPE_INT, TYPE_INT})));
}

TEST_F(DelFileStreamConverterTest, Predicates_RequiresV1ToV2DelTranscodeMatrix) {
    auto int_schema = *create_single_pk_schema(TYPE_INT);
    auto varchar_schema = *create_single_pk_schema(TYPE_VARCHAR);

    using PKE = PrimaryKeyEncodingType;
    // Supported direction on a byte-incompatible shape.
    ASSERT_TRUE(requires_v1_to_v2_del_transcode(PKE::PK_ENCODING_TYPE_V1, PKE::PK_ENCODING_TYPE_V2, int_schema));
    // V2 -> V1 on the same shape: not supported (no decoder); predicate must say false.
    ASSERT_FALSE(requires_v1_to_v2_del_transcode(PKE::PK_ENCODING_TYPE_V2, PKE::PK_ENCODING_TYPE_V1, int_schema));
    // Same encoding on both sides: no transcoding needed.
    ASSERT_FALSE(requires_v1_to_v2_del_transcode(PKE::PK_ENCODING_TYPE_V1, PKE::PK_ENCODING_TYPE_V1, int_schema));
    ASSERT_FALSE(requires_v1_to_v2_del_transcode(PKE::PK_ENCODING_TYPE_V2, PKE::PK_ENCODING_TYPE_V2, int_schema));
    // VARCHAR PK: byte-compatible on V1/V2, no transcoding even with direction match.
    ASSERT_FALSE(requires_v1_to_v2_del_transcode(PKE::PK_ENCODING_TYPE_V1, PKE::PK_ENCODING_TYPE_V2, varchar_schema));
    // NONE encoding: never transcode.
    ASSERT_FALSE(requires_v1_to_v2_del_transcode(PKE::PK_ENCODING_TYPE_NONE, PKE::PK_ENCODING_TYPE_V2, int_schema));
}

} // namespace starrocks
