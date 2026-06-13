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

#include "exec/file_scanner/avro_datum_path.h"

#include <gtest/gtest.h>

#include <avrocpp/Compiler.hh>
#include <vector>

#include "exec/file_scanner/confluent_avro_decoder.h"
#include "exprs/json_functions.h"

namespace starrocks {

// Builds one decoded GenericDatum from a fixed schema and hand-encoded Avro binary, then exercises
// the jsonpath navigator against it (record descent, array index, nullable-union, missing field).
class AvroDatumPathTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const char* kJson = R"({
            "type": "record",
            "name": "r",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "nested", "type": {"type": "record", "name": "n",
                                            "fields": [{"name": "x", "type": "long"}]}},
                {"name": "arr", "type": {"type": "array", "items": "long"}},
                {"name": "opt", "type": ["null", "long"]}
            ]
        })";
        _decoder = std::make_unique<ConfluentAvroDecoder>(avro::compileJsonSchemaFromString(kJson));
        ASSERT_TRUE(_decoder->init().ok());

        // {id: 5, nested: {x: 7}, arr: [1, 2], opt: null}
        //   id        long 5  -> zigzag 10 -> 0x0A
        //   nested.x  long 7  -> zigzag 14 -> 0x0E
        //   arr       block count 2 (0x04), items 1 (0x02) 2 (0x04), end block 0 (0x00)
        //   opt       union branch 0 (null) -> 0x00
        const std::vector<uint8_t> bytes = {0x0A, 0x0E, 0x04, 0x02, 0x04, 0x00, 0x00};
        ASSERT_TRUE(_decoder->decode(bytes.data(), bytes.size(), &_datum).ok());
    }

    std::vector<SimpleJsonPath> path(const std::string& p) {
        std::vector<SimpleJsonPath> parsed;
        CHECK(JsonFunctions::parse_json_paths(p, &parsed).ok());
        return parsed;
    }

    std::unique_ptr<ConfluentAvroDecoder> _decoder;
    avro::GenericDatum _datum;
};

TEST_F(AvroDatumPathTest, top_level_field) {
    auto r = avro_extract_field(_datum, path("$.id"));
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(int64_t{5}, r.value()->value<int64_t>());
}

TEST_F(AvroDatumPathTest, nested_record) {
    auto r = avro_extract_field(_datum, path("$.nested.x"));
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(int64_t{7}, r.value()->value<int64_t>());
}

TEST_F(AvroDatumPathTest, array_index) {
    auto r = avro_extract_field(_datum, path("$.arr[1]"));
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(int64_t{2}, r.value()->value<int64_t>());
}

TEST_F(AvroDatumPathTest, array_index_out_of_range_errors) {
    auto r = avro_extract_field(_datum, path("$.arr[9]"));
    EXPECT_FALSE(r.ok());
    EXPECT_FALSE(r.status().is_not_found());
}

TEST_F(AvroDatumPathTest, nullable_union_null_branch) {
    auto r = avro_extract_field(_datum, path("$.opt"));
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(avro::AVRO_NULL, r.value()->type());
}

TEST_F(AvroDatumPathTest, path_through_scalar_leaf_errors) {
    // $.id.x on a long `id`: the legacy reader silently returns `id`; the native reader rejects it.
    auto r = avro_extract_field(_datum, path("$.id.x"));
    EXPECT_FALSE(r.ok());
    EXPECT_FALSE(r.status().is_not_found());

    auto deep = avro_extract_field(_datum, path("$.nested.x.y"));
    EXPECT_FALSE(deep.ok());
    EXPECT_FALSE(deep.status().is_not_found());
}

TEST_F(AvroDatumPathTest, missing_field_is_not_found) {
    auto r = avro_extract_field(_datum, path("$.missing"));
    EXPECT_TRUE(r.status().is_not_found());
}

TEST_F(AvroDatumPathTest, whole_datum) {
    auto r = avro_extract_field(_datum, path("$"));
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(avro::AVRO_RECORD, r.value()->type());
}

TEST_F(AvroDatumPathTest, root_index_on_non_array_errors) {
    // $[0] on a record root: the legacy reader silently ignores the index and returns the whole
    // record; the native reader rejects it.
    auto r = avro_extract_field(_datum, path("$[0]"));
    EXPECT_FALSE(r.ok());
    EXPECT_FALSE(r.status().is_not_found());

    auto deep = avro_extract_field(_datum, path("$[0].id"));
    EXPECT_FALSE(deep.ok());
    EXPECT_FALSE(deep.status().is_not_found());
}

TEST_F(AvroDatumPathTest, array_root) {
    ConfluentAvroDecoder decoder(avro::compileJsonSchemaFromString(R"({"type":"array","items":"long"})"));
    ASSERT_TRUE(decoder.init().ok());
    // [7, 9]: block count 2 (0x04), items 7 (0x0E) 9 (0x12), end block 0 (0x00)
    const std::vector<uint8_t> bytes = {0x04, 0x0E, 0x12, 0x00};
    avro::GenericDatum datum;
    ASSERT_TRUE(decoder.decode(bytes.data(), bytes.size(), &datum).ok());

    auto r = avro_extract_field(datum, path("$[1]"));
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(int64_t{9}, r.value()->value<int64_t>());

    // A field selector on an array root is invalid.
    auto bad = avro_extract_field(datum, path("$.x"));
    EXPECT_FALSE(bad.ok());
    EXPECT_FALSE(bad.status().is_not_found());
}

TEST_F(AvroDatumPathTest, nullable_array_root) {
    // ["null", array<long>] top-level union: the root union is peeled before the root-array branch,
    // so $[i] still indexes a nullable array, and a null root yields NULL for any path.
    ConfluentAvroDecoder decoder(avro::compileJsonSchemaFromString(R"(["null", {"type":"array","items":"long"}])"));
    ASSERT_TRUE(decoder.init().ok());

    // union branch 1 (0x02), array [5]: count 1 (0x02), item 5 (0x0A), end block 0 (0x00)
    const std::vector<uint8_t> bytes = {0x02, 0x02, 0x0A, 0x00};
    avro::GenericDatum datum;
    ASSERT_TRUE(decoder.decode(bytes.data(), bytes.size(), &datum).ok());
    auto r = avro_extract_field(datum, path("$[0]"));
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(int64_t{5}, r.value()->value<int64_t>());

    // union branch 0 (null)
    const std::vector<uint8_t> null_bytes = {0x00};
    avro::GenericDatum null_datum;
    ASSERT_TRUE(decoder.decode(null_bytes.data(), null_bytes.size(), &null_datum).ok());
    auto n = avro_extract_field(null_datum, path("$[0]"));
    ASSERT_TRUE(n.ok());
    EXPECT_EQ(avro::AVRO_NULL, n.value()->type());
}

TEST_F(AvroDatumPathTest, preprocess_jsonpaths_rewrites_union_star) {
    EXPECT_EQ("$.id", avro_preprocess_jsonpaths("$.*.id"));
    EXPECT_EQ("$.a.b", avro_preprocess_jsonpaths("$.a.*.b"));
    EXPECT_EQ("$.a", avro_preprocess_jsonpaths("$.a.*"));
}

} // namespace starrocks
