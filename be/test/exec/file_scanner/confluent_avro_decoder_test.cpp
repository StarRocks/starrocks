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

#include "exec/file_scanner/confluent_avro_decoder.h"

#include <gtest/gtest.h>

#include <avrocpp/Compiler.hh>
#include <vector>

namespace starrocks {

// Schema-injection mode treats input as a raw Avro datum without Confluent framing, so these tests
// run compile-schema -> GenericReader::read -> GenericDatum without a schema registry.
class ConfluentAvroDecoderTest : public ::testing::Test {
protected:
    static avro::ValidSchema record_schema() {
        // record { long id; string name }
        static const char* kJson = R"({
            "type": "record",
            "name": "r",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        })";
        return avro::compileJsonSchemaFromString(kJson);
    }
};

TEST_F(ConfluentAvroDecoderTest, decode_raw_datum) {
    ConfluentAvroDecoder decoder(record_schema());
    ASSERT_TRUE(decoder.init().ok());

    // Hand-encoded Avro binary for {id: 27, name: "hi"}:
    //   id   = long 27   -> zigzag(27)=54 -> varint 0x36
    //   name = string    -> length 2 -> zigzag(2)=4 -> 0x04, then UTF-8 'h'(0x68) 'i'(0x69)
    const std::vector<uint8_t> payload = {0x36, 0x04, 0x68, 0x69};

    avro::GenericDatum datum;
    ASSERT_TRUE(decoder.decode(payload.data(), payload.size(), &datum).ok());

    ASSERT_EQ(avro::AVRO_RECORD, datum.type());
    const auto& record = datum.value<avro::GenericRecord>();
    EXPECT_EQ(int64_t{27}, record.field("id").value<int64_t>());
    EXPECT_EQ("hi", record.field("name").value<std::string>());
}

TEST_F(ConfluentAvroDecoderTest, decode_reuses_datum_across_calls) {
    ConfluentAvroDecoder decoder(record_schema());
    ASSERT_TRUE(decoder.init().ok());

    avro::GenericDatum datum;
    const std::vector<uint8_t> first = {0x02, 0x02, 0x61};  // id=1, name="a"
    const std::vector<uint8_t> second = {0x04, 0x02, 0x62}; // id=2, name="b"

    ASSERT_TRUE(decoder.decode(first.data(), first.size(), &datum).ok());
    EXPECT_EQ(int64_t{1}, datum.value<avro::GenericRecord>().field("id").value<int64_t>());

    ASSERT_TRUE(decoder.decode(second.data(), second.size(), &datum).ok());
    EXPECT_EQ(int64_t{2}, datum.value<avro::GenericRecord>().field("id").value<int64_t>());
    EXPECT_EQ("b", datum.value<avro::GenericRecord>().field("name").value<std::string>());
}

TEST_F(ConfluentAvroDecoderTest, truncated_payload_returns_error) {
    ConfluentAvroDecoder decoder(record_schema());
    ASSERT_TRUE(decoder.init().ok());

    // Only the long field is present; reading the string runs past the buffer -> avro::Exception
    // -> InternalError (decode never lets the exception escape).
    const std::vector<uint8_t> truncated = {0x36};

    avro::GenericDatum datum;
    EXPECT_FALSE(decoder.decode(truncated.data(), truncated.size(), &datum).ok());
}

} // namespace starrocks
