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

#include "exec/pipeline/paimon_bucket_operator.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/util/thrift_util.h"
#include "exec/pipeline/exchange/shuffler.h"

namespace starrocks {
namespace {

TPaimonBucketRouting routing_descriptor() {
    TPaimonTableSink sink;
    sink.__set_writer_protocol_version(1);
    sink.__set_bucket_count(7);
    sink.__set_routing_bucket_slot_id(99);
    sink.__set_column_names({"p", "id"});
    sink.__set_partition_columns({"p"});
    sink.__set_schema_id(8);
    sink.__set_schema_json("{native-schema}");
    sink.__set_table_identity("routing-test");
    sink.__set_location("file:///unused");
    sink.__set_options({{"bucket", "7"}, {"bucket-key", "id"}});
    sink.__set_cloud_configuration(TCloudConfiguration{});
    ThriftSerializer serializer(false, 1024);
    std::string payload;
    EXPECT_OK(serializer.serialize(&sink, &payload));
    TPaimonBucketRouting routing;
    routing.__set_protocol_version(1);
    routing.__set_sink_payload(payload);
    routing.__set_bucket_slot_id(99);
    routing.__set_schema_exprs({TExpr{}, TExpr{}});
    return routing;
}

ChunkPtr chunk_with_rows() {
    auto ids = Int32Column::create();
    ids->append(2);
    ids->append(3);
    ids->append(5);
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(ids), 1);
    return chunk;
}

} // namespace

TEST(PaimonBucketRoutingTest, DecodesBoundedFrozenNativeDescriptor) {
    auto routing = routing_descriptor();
    auto sink = decode_paimon_bucket_routing(routing);
    ASSERT_TRUE(sink.ok()) << sink.status();
    EXPECT_EQ(99, sink->routing_bucket_slot_id);
    EXPECT_EQ(7, sink->bucket_count);
    EXPECT_EQ("{native-schema}", sink->schema_json);
    EXPECT_EQ((std::vector<std::string>{"p", "id"}), sink->column_names);
    EXPECT_EQ("id", sink->options.at("bucket-key"));
}

TEST(PaimonBucketRoutingTest, RejectsUnsupportedMalformedAndOversizedContracts) {
    auto routing = routing_descriptor();
    routing.__set_protocol_version(2);
    EXPECT_FALSE(decode_paimon_bucket_routing(routing).ok());
    routing = routing_descriptor();
    routing.__set_sink_payload(std::string(kMaxPaimonRoutingPayloadBytes + 1, 'x'));
    EXPECT_FALSE(decode_paimon_bucket_routing(routing).ok());
    routing = routing_descriptor();
    routing.__set_sink_payload("sensitive-malformed-input");
    auto malformed = decode_paimon_bucket_routing(routing);
    ASSERT_FALSE(malformed.ok());
    EXPECT_EQ(std::string::npos, malformed.status().to_string().find("sensitive-malformed-input"));
    routing = routing_descriptor();
    routing.sink_payload += "trailing";
    EXPECT_FALSE(decode_paimon_bucket_routing(routing).ok());
    routing = routing_descriptor();
    routing.__set_bucket_slot_id(100);
    EXPECT_FALSE(decode_paimon_bucket_routing(routing).ok());
    routing = routing_descriptor();
    routing.__set_schema_exprs({TExpr{}});
    EXPECT_FALSE(decode_paimon_bucket_routing(routing).ok());
    EXPECT_FALSE(decode_paimon_bucket_routing(TPaimonBucketRouting{}).ok());
}

TEST(PaimonBucketRoutingTest, AppendsOnlyActualNativeBucketIds) {
    auto chunk = chunk_with_rows();
    ASSERT_OK(append_paimon_bucket_column(chunk, 99, 7, {6, 0, 6}));
    EXPECT_EQ(2, chunk->num_columns());
    const auto& buckets = chunk->get_column_by_slot_id(99);
    EXPECT_FALSE(buckets->is_nullable());
    EXPECT_FALSE(buckets->is_constant());
    const auto& actual = down_cast<const Int32Column&>(*buckets).get_data();
    ASSERT_EQ(3, actual.size());
    EXPECT_EQ(6, actual[0]);
    EXPECT_EQ(0, actual[1]);
    EXPECT_EQ(6, actual[2]);
    // A placeholder or duplicate route slot cannot be silently overwritten.
    EXPECT_FALSE(append_paimon_bucket_column(chunk, 99, 7, {0, 0, 0}).ok());
}

TEST(PaimonBucketRoutingTest, RepeatedNativePartitionBucketHasOneGlobalAndLocalDriverOwner) {
    auto partitions = Int32Column::create();
    auto buckets = Int32Column::create();
    for (int32_t value : {7, 9, 7, 9, 7}) partitions->append(value);
    for (int32_t value : {3, 3, 3, 3, 3}) buckets->append(value);
    for (bool compatibility : {false, true}) {
        std::vector<uint32_t> hashes(5, HashUtil::FNV_SEED);
        partitions->fnv_hash(hashes.data(), 0, hashes.size());
        buckets->fnv_hash(hashes.data(), 0, hashes.size());
        std::vector<uint32_t> global(hashes.size());
        std::vector<uint32_t> local(hashes.size());
        pipeline::Shuffler exchange(compatibility, true, TPartitionType::HASH_PARTITIONED, 3, 8);
        pipeline::Shuffler writer(compatibility, true, TPartitionType::HASH_PARTITIONED, 8, 1);
        exchange.exchange_shuffle(global, hashes, {}, hashes.size());
        writer.local_exchange_shuffle(local, hashes, hashes.size());
        for (size_t row : {2, 4}) {
            EXPECT_EQ(global[0], global[row]);
            EXPECT_EQ(local[0], local[row]);
        }
        EXPECT_EQ(global[1], global[3]);
        EXPECT_EQ(local[1], local[3]);
        // The local writer's static driver mapping matches pipeline-level global exchange mapping.
        for (size_t row = 0; row < hashes.size(); ++row) EXPECT_EQ(global[row] % 8, local[row]);
    }
}

TEST(PaimonBucketRoutingTest, InvalidNativeResultsDoNotPublishARouteColumn) {
    for (const auto& buckets : std::vector<std::vector<int32_t>>{{0, 1}, {0, -1, 2}, {0, 7, 2}}) {
        auto chunk = chunk_with_rows();
        EXPECT_FALSE(append_paimon_bucket_column(chunk, 99, 7, buckets).ok());
        EXPECT_EQ(0, chunk->get_slot_id_to_index_map().count(99));
        EXPECT_EQ(1, chunk->num_columns());
    }
}

} // namespace starrocks
