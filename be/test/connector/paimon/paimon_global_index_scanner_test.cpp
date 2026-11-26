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

#include "connector/hive/paimon/paimon_global_index_scanner.h"

#include <paimon/global_index/bitmap_scored_global_index_result.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <limits>
#include <numeric>
#include <set>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "common/runtime_profile.h"
#include "connector/hive/hive_connector.h"
#include "connector/hive/paimon/paimon_global_index_ann.h"
#include "connector/paimon/paimon_global_index_test_util.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs_memory.h"
#include "runtime/runtime_state.h"

namespace starrocks {
using namespace paimon_global_index_test;

class PaimonGlobalIndexScannerTestAccessor {
public:
    static Status parse(const TPaimonGlobalIndexScanRange& range) {
        rapidjson::Document request;
        return PaimonGlobalIndexScanner::_parse_request(range, &request);
    }
    static void set_reader(PaimonGlobalIndexScanner* scanner, int32_t id,
                           std::shared_ptr<paimon::GlobalIndexReader> reader) {
        scanner->_readers[id] = std::move(reader);
    }
    static std::shared_ptr<paimon::GlobalIndexReader> reader(PaimonGlobalIndexScanner* scanner, int32_t id) {
        return scanner->_readers.at(id);
    }
    static std::shared_ptr<PaimonFileSystem> file_system(PaimonGlobalIndexScanner* scanner) {
        return scanner->_paimon_file_system;
    }
    static bool has_resources(const PaimonGlobalIndexScanner& scanner) {
        return scanner._operation || scanner._memory_pool || scanner._paimon_file_system || !scanner._readers.empty();
    }
};

namespace {
std::string empty_key_metadata() {
    // Two empty key bounds followed by the SDK's flags byte.
    return std::string(9, '\0');
}

TPaimonGlobalIndexScanRange valid_range() {
    TPaimonGlobalIndexFile file;
    file.__set_field_id(17);
    file.__set_field_type("int");
    file.__set_path("/index/segment.btree");
    file.__set_file_size(128);
    file.__set_row_range_from(100);
    file.__set_row_range_to(103);
    file.__set_metadata(empty_key_metadata());
    TPaimonGlobalIndexScanRange range;
    range.__set_protocol_version(5);
    range.__set_shard_id(3);
    range.__set_range_from(100);
    range.__set_range_to(103);
    range.__set_snapshot_id(7);
    range.__set_table_path("/table");
    range.__set_index_files({file});
    range.__set_query_json(R"({"version":5,"snapshotId":7,"schemaId":1,"schemaTimeMillis":1000,
        "predicate":{"o":"b","b":"EQ","c":[{"o":"cr","t":"int","n":"k"},{"o":"co","t":"int","v":10}]},
        "indexes":{"k":{"fieldId":17,"provider":"btree"}},"tablePath":"/table","branch":"main","resultBytesLimit":1048576})");
    return range;
}

void change_request(TPaimonGlobalIndexScanRange* range, const std::function<void(rapidjson::Document&)>& change) {
    auto request = parse_json(range->query_json.c_str());
    change(request);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    request.Accept(writer);
    range->__set_query_json(buffer.GetString());
}

TPaimonGlobalIndexScanRange valid_ann_range() {
    auto range = valid_range();
    range.__set_protocol_version(6);
    range.index_files[0].__set_field_type("array<float>");
    range.index_files[0].__set_metadata(
            R"({"index.dimension":"4","index.type":"diskann","distance.metric":"l2","encoding.type":"rawf32"})");
    range.__set_query_json(R"({"version":6,"snapshotId":7,"schemaId":1,"schemaTimeMillis":1000,
        "kind":"unfiltered_top_n","vector":{"fieldId":17,"fieldName":"vec","provider":"lumina",
        "metric":"l2","queryVector":[300,6,3,1],"k":5},
        "tablePath":"/table","branch":"main","resultBytesLimit":64})");
    return range;
}

class FakeScoredResult final : public paimon::BitmapScoredGlobalIndexResult {
public:
    explicit FakeScoredResult(std::vector<std::pair<int64_t, float>> hits)
            : paimon::BitmapScoredGlobalIndexResult(paimon::RoaringBitmap64(), {}), _hits(std::move(hits)) {}
    class Iterator final : public paimon::ScoredGlobalIndexResult::ScoredIterator {
    public:
        explicit Iterator(std::vector<std::pair<int64_t, float>> hits) : _hits(std::move(hits)) {}
        bool HasNext() const override { return _cursor < _hits.size(); }
        std::pair<int64_t, float> NextWithScore() override { return _hits[_cursor++]; }

    private:
        std::vector<std::pair<int64_t, float>> _hits;
        size_t _cursor = 0;
    };
    paimon::Result<std::unique_ptr<paimon::ScoredGlobalIndexResult::ScoredIterator>> CreateScoredIterator()
            const override {
        return std::unique_ptr<paimon::ScoredGlobalIndexResult::ScoredIterator>(new Iterator(_hits));
    }

private:
    std::vector<std::pair<int64_t, float>> _hits;
};

void expect_rows(const std::shared_ptr<paimon::GlobalIndexResult>& result, const std::vector<int64_t>& expected) {
    auto iterator = result->CreateIterator();
    ASSERT_TRUE(iterator.ok()) << iterator.status().ToString();
    std::vector<int64_t> actual;
    while (iterator.value()->HasNext()) actual.push_back(iterator.value()->Next());
    EXPECT_EQ(expected, actual);
}

class RecordingFileSystem : public MemoryFileSystem {
public:
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& options,
                                                                       const std::string& path) override {
        opened_paths.emplace_back(path);
        if (on_open) on_open();
        return FileSystem::Default()->new_random_access_file(options, path);
    }
    std::vector<std::string> opened_paths;
    std::function<void()> on_open;
};
} // namespace

TEST(PaimonGlobalIndexAnnRequestTest, AcceptsV6AndPreservesScalarV5) {
    EXPECT_OK(PaimonGlobalIndexScannerTestAccessor::parse(valid_range()));
    EXPECT_OK(PaimonGlobalIndexScannerTestAccessor::parse(valid_ann_range()));
    auto range = valid_ann_range();
    range.index_files[0].__set_metadata(
            R"({"index.dimension":"4","index.type":"bruteforce","distance.metric":"l2","encoding.type":"rawf32"})");
    EXPECT_OK(PaimonGlobalIndexScannerTestAccessor::parse(range));
    for (const char* metric : {"cosine", "inner_product"}) {
        range = valid_ann_range();
        change_request(&range, [&](auto& d) { d["vector"]["metric"].SetString(metric, d.GetAllocator()); });
        range.index_files[0].__set_metadata(
                std::string(
                        R"({"index.dimension":"4","index.type":"bruteforce","encoding.type":"rawf32","distance.metric":")") +
                metric + R"("})");
        EXPECT_OK(PaimonGlobalIndexScannerTestAccessor::parse(range));
    }
}

TEST(PaimonGlobalIndexAnnRequestTest, BoundsDiskAnnSearchWindowForSmallAndLargeTopK) {
    EXPECT_EQ(128, paimon_global_index::ann_diskann_search_list_size(5, 256));
    EXPECT_EQ(64, paimon_global_index::ann_diskann_search_list_size(5, 64));
    EXPECT_EQ(300, paimon_global_index::ann_diskann_search_list_size(200, 1024));
    EXPECT_EQ(200, paimon_global_index::ann_diskann_search_list_size(200, 200));
}

TEST(PaimonGlobalIndexAnnRequestTest, RejectsMalformedVectorAndMixedProtocols) {
    std::vector<std::function<void(rapidjson::Document&)>> changes = {
            [](auto& d) { d["kind"].SetString("filtered_top_n"); },
            [](auto& d) { d.AddMember("predicate", rapidjson::Value(rapidjson::kObjectType), d.GetAllocator()); },
            [](auto& d) { d.AddMember("indexes", rapidjson::Value(rapidjson::kObjectType), d.GetAllocator()); },
            [](auto& d) { d["vector"]["provider"].SetString("btree"); },
            [](auto& d) { d["vector"]["fieldId"].SetInt(-1); },
            [](auto& d) { d["vector"]["fieldName"].SetString(""); },
            [](auto& d) { d["vector"]["metric"].SetString("hamming"); },
            [](auto& d) { d["vector"]["k"].SetInt(0); },
            [](auto& d) { d["vector"]["k"].SetInt(std::numeric_limits<int32_t>::max()); },
            [](auto& d) { d["vector"]["queryVector"].SetArray(); },
            [](auto& d) { d["vector"]["queryVector"][0].SetString("300"); },
            [](auto& d) { d["vector"]["queryVector"][0].SetDouble(1e100); },
            [](auto& d) { d["resultBytesLimit"].SetInt64(63); }};
    for (size_t i = 0; i < changes.size(); ++i) {
        SCOPED_TRACE(i);
        auto range = valid_ann_range();
        change_request(&range, changes[i]);
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
    auto range = valid_ann_range();
    change_request(&range, [](auto& d) {
        for (int i = 4; i <= 4096; ++i) d["vector"]["queryVector"].PushBack(0, d.GetAllocator());
    });
    EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
}

TEST(PaimonGlobalIndexAnnRequestTest, RequiresOneExactFileAndMatchingOriginalMetadata) {
    for (int change = 0; change < 6; ++change) {
        auto range = valid_ann_range();
        if (change == 0) range.index_files.push_back(range.index_files[0]);
        if (change == 1) range.index_files[0].__set_field_id(18);
        if (change == 2) range.index_files[0].__set_field_type("int");
        if (change == 3) range.index_files[0].__set_row_range_from(99);
        if (change == 4) range.index_files[0].__set_row_range_to(104);
        if (change == 5) range.index_files[0].__isset.metadata = false;
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
    for (const char* metadata :
         {"null", "{}", "{", R"({"index.dimension":4})",
          R"({"index.dimension":"3","index.type":"diskann","distance.metric":"l2","encoding.type":"rawf32"})",
          R"({"index.dimension":"4","index.type":"ivf","distance.metric":"l2","encoding.type":"rawf32"})",
          R"({"index.dimension":"4","index.type":"diskann","distance.metric":"cosine","encoding.type":"rawf32"})",
          R"({"index.dimension":"4","index.type":"diskann","distance.metric":"l2","encoding.type":"sq8"})",
          R"({"index.dimension":"4","index.type":"diskann","distance.metric":"l2","encoding.type":"rawf32","extension.build.tag.tag_schema":"[]"})",
          R"({"index.dimension":"4","index.dimension":"4","index.type":"diskann","distance.metric":"l2","encoding.type":"rawf32"})"}) {
        auto range = valid_ann_range();
        range.index_files[0].__set_metadata(metadata);
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
}

TEST(PaimonGlobalIndexAnnResultTest, EncodesOffsetsScoresAndEmptyResultInLittleEndian) {
    auto check = [] { return Status::OK(); };
    auto result = std::make_shared<FakeScoredResult>(
            std::vector<std::pair<int64_t, float>>{{44, 0}, {43, 3}, {42, 12}, {41, 27}, {46, 33}});
    auto encoded = paimon_global_index::serialize_ann_result(result, 5, 256, 511, false, 64, check);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    ASSERT_EQ(64, encoded->size());
    auto read_le = [&](size_t offset, size_t length) {
        uint64_t value = 0;
        for (size_t i = 0; i < length; ++i) value |= uint64_t(uint8_t((*encoded)[offset + i])) << (8 * i);
        return value;
    };
    EXPECT_EQ(5, read_le(0, 4));
    const int64_t ids[] = {300, 299, 298, 297, 302};
    const float scores[] = {0, 3, 12, 27, 33};
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(ids[i], read_le(4 + 12 * i, 8));
        uint32_t bits = read_le(12 + 12 * i, 4);
        float score;
        std::memcpy(&score, &bits, sizeof(score));
        EXPECT_EQ(scores[i], score);
    }
    auto empty = std::make_shared<FakeScoredResult>(std::vector<std::pair<int64_t, float>>{});
    auto encoded_empty = paimon_global_index::serialize_ann_result(empty, 5, 256, 511, false, 64, check);
    ASSERT_TRUE(encoded_empty.ok());
    EXPECT_EQ(std::string(4, '\0'), *encoded_empty);
    auto ip = paimon_global_index::serialize_ann_result(result, 5, 256, 511, true, 64, check);
    ASSERT_TRUE(ip.ok());
    // Largest inner product comes first, and is represented as a negative score.
    EXPECT_EQ(char(46), (*ip)[4]);
    EXPECT_EQ(char(1), (*ip)[5]);
    uint32_t minus33;
    const float expected = -33;
    std::memcpy(&minus33, &expected, sizeof(expected));
    for (int i = 0; i < 4; ++i) EXPECT_EQ(uint8_t(minus33 >> (8 * i)), uint8_t((*ip)[12 + i]));
}

TEST(PaimonGlobalIndexAnnResultTest, RejectsBrokenProviderResultsAndPropagatesCancellation) {
    auto check = [] { return Status::OK(); };
    for (auto hits :
         std::vector<std::vector<std::pair<int64_t, float>>>{{{-1, 1}},
                                                             {{256, 1}},
                                                             {{1, std::numeric_limits<float>::infinity()}},
                                                             {{1, std::numeric_limits<float>::quiet_NaN()}},
                                                             {{1, 2}, {1, 3}},
                                                             {{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}}}) {
        auto result = std::make_shared<FakeScoredResult>(std::move(hits));
        EXPECT_FALSE(paimon_global_index::serialize_ann_result(result, 5, 256, 511, false, 64, check).ok());
    }
    auto result = std::make_shared<FakeScoredResult>(std::vector<std::pair<int64_t, float>>{{1, 2}});
    EXPECT_FALSE(paimon_global_index::serialize_ann_result(nullptr, 5, 256, 511, false, 64, check).ok());
    EXPECT_FALSE(paimon_global_index::serialize_ann_result(result, 5, 256, 511, false, 63, check).ok());
    auto cancelled = paimon_global_index::serialize_ann_result(result, 5, 256, 511, false, 64,
                                                               [] { return Status::Cancelled("ANN cancelled"); });
    EXPECT_TRUE(cancelled.status().is_cancelled());
}

TEST(PaimonGlobalIndexRequestTest, AcceptsSnapshotBoundFilesAndThriftRoundTrip) {
    auto range = valid_range();
    EXPECT_OK(PaimonGlobalIndexScannerTestAccessor::parse(range));
    auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    apache::thrift::protocol::TCompactProtocol protocol(transport);
    range.write(&protocol);
    TPaimonGlobalIndexScanRange decoded;
    decoded.read(&protocol);
    EXPECT_EQ(range, decoded);
    EXPECT_OK(PaimonGlobalIndexScannerTestAccessor::parse(decoded));
}

TEST(PaimonGlobalIndexRequestTest, EstimatesMemoryFromIndexFileBytesInsteadOfRowIdRange) {
    connector::HiveDataSourceProvider provider(0, THdfsScanNode());
    constexpr int64_t file_bytes = 32 * 1024 * 1024;
    auto request = valid_range();
    request.__set_range_to(1000000000000L);
    THdfsScanRange range;
    range.__set_file_length(file_bytes);
    range.__set_length(file_bytes);
    range.__set_paimon_global_index_scan_range(request);
    TScanRangeParams params;
    params.scan_range.__set_hdfs_scan_range(range);
    provider.prepare_scan_ranges({params});
    int64_t min_bytes = 0;
    int64_t max_bytes = 0;
    provider.default_data_source_mem_bytes(&min_bytes, &max_bytes);
    EXPECT_EQ(file_bytes * 3 / 2, min_bytes);
    EXPECT_EQ(min_bytes, max_bytes);
}

TEST(PaimonGlobalIndexRequestTest, RejectsMissingRequiredFieldsAndOldProtocols) {
    std::vector<std::function<void(TPaimonGlobalIndexScanRange&)>> changes = {
            [](auto& r) { r.__isset.protocol_version = false; }, [](auto& r) { r.__isset.shard_id = false; },
            [](auto& r) { r.__isset.range_from = false; },       [](auto& r) { r.__isset.range_to = false; },
            [](auto& r) { r.__isset.query_json = false; },       [](auto& r) { r.__isset.table_path = false; },
            [](auto& r) { r.__isset.snapshot_id = false; },      [](auto& r) { r.__isset.index_files = false; }};
    for (size_t i = 0; i < changes.size(); ++i) {
        SCOPED_TRACE(i);
        auto range = valid_range();
        changes[i](range);
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
    for (int version : {0, 4, 6}) {
        auto range = valid_range();
        range.__set_protocol_version(version);
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
}

TEST(PaimonGlobalIndexRequestTest, RejectsInvalidShardBoundsAndIdentity) {
    std::vector<std::function<void(TPaimonGlobalIndexScanRange&)>> changes = {[](auto& r) { r.__set_shard_id(-1); },
                                                                              [](auto& r) { r.__set_snapshot_id(-1); },
                                                                              [](auto& r) { r.__set_table_path(""); },
                                                                              [](auto& r) { r.__set_range_from(-1); },
                                                                              [](auto& r) { r.__set_range_to(99); },
                                                                              [](auto& r) { r.__set_range_to(std::numeric_limits<int64_t>::max()); }};
    for (size_t i = 0; i < changes.size(); ++i) {
        SCOPED_TRACE(i);
        auto range = valid_range();
        changes[i](range);
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
}

TEST(PaimonGlobalIndexRequestTest, AcceptsLargestRepresentableExclusiveUpperBound) {
    auto range = valid_range();
    range.__set_range_from(std::numeric_limits<int64_t>::max() - 2);
    range.__set_range_to(std::numeric_limits<int64_t>::max() - 1);
    range.index_files[0].__set_row_range_from(range.range_from);
    range.index_files[0].__set_row_range_to(range.range_to);
    EXPECT_OK(PaimonGlobalIndexScannerTestAccessor::parse(range));
}

TEST(PaimonGlobalIndexRequestTest, RejectsMalformedAndInconsistentEnvelopes) {
    for (const char* json : {"", "[1]", "null", "{"}) {
        auto range = valid_range();
        range.__set_query_json(json);
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
    const char* required[] = {"version", "snapshotId", "schemaId", "schemaTimeMillis", "predicate",
                              "indexes", "tablePath",  "branch",   "resultBytesLimit"};
    for (const char* field : required) {
        SCOPED_TRACE(field);
        auto range = valid_range();
        change_request(&range, [&](auto& d) { d.RemoveMember(field); });
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
        range = valid_range();
        change_request(&range, [&](auto& d) { d[field].SetNull(); });
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
    for (const char* field : {"version", "snapshotId", "schemaId", "schemaTimeMillis", "resultBytesLimit"}) {
        auto range = valid_range();
        change_request(&range, [&](auto& d) { d[field].SetInt(-1); });
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
    for (const auto& [field, value] : {std::pair{"version", 4}, std::pair{"snapshotId", 8}}) {
        SCOPED_TRACE(field);
        auto range = valid_range();
        change_request(&range, [&](auto& d) { d[field].SetInt(value); });
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
    for (const char* field : {"tablePath", "branch"}) {
        auto range = valid_range();
        change_request(&range, [&](auto& d) { d[field].SetString("another"); });
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
}

TEST(PaimonGlobalIndexRequestTest, RejectsInvalidBindingsAndResultBudgets) {
    std::vector<std::function<void(rapidjson::Document&)>> changes = {
            [](auto& d) { d["indexes"].RemoveAllMembers(); },
            [](auto& d) { d["indexes"]["k"].SetNull(); },
            [](auto& d) { d["indexes"]["k"].RemoveMember("fieldId"); },
            [](auto& d) { d["indexes"]["k"]["fieldId"].SetInt(-1); },
            [](auto& d) { d["indexes"]["k"]["fieldId"].SetInt(18); },
            [](auto& d) { d["indexes"]["k"]["provider"].SetString("unknown"); },
            [](auto& d) { d["resultBytesLimit"].SetInt(0); },
            [](auto& d) { d["resultBytesLimit"].SetInt(1024 * 1024 + 1); }};
    for (size_t i = 0; i < changes.size(); ++i) {
        SCOPED_TRACE(i);
        auto range = valid_range();
        change_request(&range, changes[i]);
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
}

TEST(PaimonGlobalIndexRequestTest, RejectsMissingAndInvalidFileDescriptors) {
    std::vector<std::function<void(TPaimonGlobalIndexFile&)>> missing = {
            [](auto& f) { f.__isset.field_id = false; },       [](auto& f) { f.__isset.field_type = false; },
            [](auto& f) { f.__isset.path = false; },           [](auto& f) { f.__isset.file_size = false; },
            [](auto& f) { f.__isset.row_range_from = false; }, [](auto& f) { f.__isset.row_range_to = false; },
            [](auto& f) { f.__isset.metadata = false; }};
    for (size_t i = 0; i < missing.size(); ++i) {
        SCOPED_TRACE(i);
        auto range = valid_range();
        missing[i](range.index_files[0]);
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
    std::vector<std::function<void(TPaimonGlobalIndexFile&)>> changes = {[](auto& f) { f.__set_field_id(-1); },
                                                                         [](auto& f) { f.__set_field_type("float"); },
                                                                         [](auto& f) { f.__set_path(""); },
                                                                         [](auto& f) { f.__set_file_size(0); },
                                                                         [](auto& f) { f.__set_row_range_from(-1); },
                                                                         [](auto& f) { f.__set_row_range_from(104); },
                                                                         [](auto& f) { f.__set_row_range_to(99); },
                                                                         [](auto& f) { f.__set_row_range_to(std::numeric_limits<int64_t>::max()); },
                                                                         [](auto& f) { f.__set_metadata(""); }};
    for (size_t i = 0; i < changes.size(); ++i) {
        SCOPED_TRACE(i);
        auto range = valid_range();
        changes[i](range.index_files[0]);
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    }
    auto range = valid_range();
    range.index_files.clear();
    EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
}

TEST(PaimonGlobalIndexRequestTest, RejectsOversizedRequestsAndAggregateFilePayloads) {
    auto range = valid_range();
    range.query_json.append(64 * 1024, ' ');
    EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    range = valid_range();
    range.index_files.resize(65537, range.index_files[0]);
    EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
    range = valid_range();
    range.index_files[0].path.assign(1024 * 1024, 'a');
    EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse(range).is_invalid_argument());
}

class PaimonGlobalIndexScannerTest : public ::testing::Test {
protected:
    void SetUp() override {
        ASSERT_NE(nullptr, dlopen("libpaimon_local_file_system.so", RTLD_NOW | RTLD_LOCAL));
        ASSERT_NE(nullptr, dlopen("libpaimon_global_index.so", RTLD_NOW | RTLD_LOCAL));
        _root = std::filesystem::temp_directory_path() / "sr-global-index-XXXXXX";
        ASSERT_NE(nullptr, mkdtemp(_root.data()));
        auto fs = paimon::FileSystemFactory::Get("local", _root, {});
        ASSERT_TRUE(fs.ok()) << fs.status().ToString();
        _files = std::make_shared<NativeIndexFiles>(std::move(fs).value(), _root);
        _state.init_mem_trackers(TUniqueId());
        _range.__set_paimon_global_index_scan_range(valid_range());
        request().index_files.clear();
        ASSERT_NO_FATAL_FAILURE(add_segment(17, "k", 100, {-5, 10, 10, 20}));
        ASSERT_FALSE(request().index_files.empty());
        _context.scan_range = &_range;
        _context.fs = &_source;
        parquet::Utils::SlotDesc slots[] = {{"shard_id", TYPE_BIGINT_DESC, 1},
                                            {"index_result", TYPE_VARBINARY_DESC, 2},
                                            {"unused", TYPE_INT_DESC, 3},
                                            {""}};
        auto* tuple = parquet::Utils::create_tuple_descriptor(&_state, &_pool, slots);
        _context.materialize_slots = tuple->slots();
        ASSERT_OK(_scanner.init(&_state, &_context));
    }
    void TearDown() override {
        _scanner.do_close(&_state);
        _files.reset();
        if (!_root.empty()) std::filesystem::remove_all(_root);
    }
    void add_segment(int32_t field_id, const std::string& name, int64_t from, const std::vector<int32_t>& values) {
        auto indexer = paimon::GlobalIndexerFactory::Get("btree", {});
        ASSERT_TRUE(indexer.ok()) << indexer.status().ToString();
        auto field = arrow::field(name, arrow::int32());
        auto schema = arrow::schema({field});
        ArrowSchema exported{};
        ASSERT_TRUE(arrow::ExportSchema(*schema, &exported).ok());
        DeferOp release([&] {
            if (exported.release) exported.release(&exported);
        });
        auto writer = indexer.value()->CreateWriter(name, &exported, _files, paimon::GetDefaultPool());
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        // The BTree writer requires sorted keys; keep each key's original local row ID.
        std::vector<int64_t> local_ids(values.size());
        std::iota(local_ids.begin(), local_ids.end(), 0);
        std::stable_sort(local_ids.begin(), local_ids.end(),
                         [&](int64_t a, int64_t b) { return values[a] < values[b]; });
        arrow::Int32Builder builder;
        for (int64_t id : local_ids) ASSERT_TRUE(builder.Append(values[id]).ok());
        auto array = builder.Finish();
        ASSERT_TRUE(array.ok());
        auto batch = arrow::StructArray::Make({array.ValueOrDie()}, {field});
        ASSERT_TRUE(batch.ok());
        ArrowArray exported_batch{};
        ASSERT_TRUE(arrow::ExportArray(*batch.ValueOrDie(), &exported_batch).ok());
        DeferOp release_batch([&] {
            if (exported_batch.release) exported_batch.release(&exported_batch);
        });
        auto status = writer.value()->AddBatch(&exported_batch, std::move(local_ids));
        ASSERT_TRUE(status.ok()) << status.ToString();
        auto metas = writer.value()->Finish();
        ASSERT_TRUE(metas.ok()) << metas.status().ToString();
        for (const auto& meta : metas.value()) {
            TPaimonGlobalIndexFile file;
            file.__set_field_id(field_id);
            file.__set_field_type("int");
            file.__set_path(meta.file_path);
            file.__set_file_size(meta.file_size);
            file.__set_row_range_from(from);
            file.__set_row_range_to(from + values.size() - 1);
            ASSERT_NE(nullptr, meta.metadata);
            file.__set_metadata(std::string(meta.metadata->data(), meta.metadata->size()));
            request().index_files.emplace_back(std::move(file));
        }
    }
    TPaimonGlobalIndexScanRange& request() { return _range.paimon_global_index_scan_range; }
    Status next(ChunkPtr* chunk) { return _scanner.do_get_next(&_state, chunk); }
    void expect_chunk_rows(const ChunkPtr& chunk, const std::vector<int64_t>& rows) {
        ASSERT_EQ(1, chunk->num_rows());
        EXPECT_EQ(3, chunk->get_column_by_slot_id(1)->get(0).get_int64());
        auto bytes = chunk->get_column_by_slot_id(2)->get(0).get_slice();
        auto result = paimon::GlobalIndexResult::Deserialize(bytes.data, bytes.size, paimon::GetDefaultPool());
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        expect_rows(result.value(), rows);
        EXPECT_TRUE(chunk->get_column_by_slot_id(3)->is_null(0));
    }
    void inject_result(std::shared_ptr<paimon::GlobalIndexResult> result) {
        PaimonGlobalIndexScannerTestAccessor::set_reader(&_scanner, 17,
                                                         std::make_shared<FakeGlobalIndexReader>(std::move(result)));
    }
    std::string _root;
    std::shared_ptr<NativeIndexFiles> _files;
    RuntimeState _state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    ObjectPool _pool;
    RecordingFileSystem _source;
    THdfsScanRange _range;
    HdfsScannerContext _context;
    PaimonGlobalIndexScanner _scanner;
};

TEST_F(PaimonGlobalIndexScannerTest, ReadsOnlySelectedFilesEmitsBinaryBitmapAndStopsAfterOneRow) {
    ASSERT_OK(_scanner.do_open(&_state));
    ChunkPtr chunk = std::make_shared<Chunk>();
    ASSERT_OK(next(&chunk));
    expect_chunk_rows(chunk, {101, 102});
    EXPECT_TRUE(next(&chunk).is_end_of_file());
    ASSERT_FALSE(_source.opened_paths.empty());
    std::set<std::string> selected;
    for (const auto& file : request().index_files) selected.insert(file.path);
    for (const auto& path : _source.opened_paths) EXPECT_TRUE(selected.contains(path)) << path;
    EXPECT_GT(_scanner.estimated_mem_usage(), 0);
    RuntimeProfile profile("index");
    HdfsScannerProfile counters;
    counters.runtime_profile = &profile;
    _scanner.do_update_counter(&counters);
    for (const char* name : {"OpenTime", "EvaluateTime", "SerializeTime", "AppIOCount", "AppIOBytes",
                             "FileSystemIOCount", "FileSystemIOBytes"}) {
        ASSERT_NE(nullptr, profile.get_counter(name)) << name;
        EXPECT_GT(profile.get_counter(name)->value(), 0) << name;
    }
    EXPECT_EQ(chunk->get_column_by_slot_id(2)->get(0).get_slice().size,
              profile.get_counter("SerializedBytes")->value());
    _scanner.do_close(&_state);
    RuntimeProfile closed_profile("closed");
    counters.runtime_profile = &closed_profile;
    _scanner.do_update_counter(&counters);
    EXPECT_EQ(profile.get_counter("AppIOBytes")->value(), closed_profile.get_counter("AppIOBytes")->value());
    EXPECT_EQ(profile.get_counter("FileSystemIOBytes")->value(),
              closed_profile.get_counter("FileSystemIOBytes")->value());
    EXPECT_FALSE(PaimonGlobalIndexScannerTestAccessor::has_resources(_scanner));
    EXPECT_EQ(0, _scanner.estimated_mem_usage());
    _scanner.do_close(&_state);
}

TEST_F(PaimonGlobalIndexScannerTest, OffsetsSegmentsBeforeMergingAndClipsToShard) {
    ASSERT_NO_FATAL_FAILURE(add_segment(17, "k", 104, {10, 20, 10, 30}));
    request().__set_range_from(102);
    request().__set_range_to(105);
    ASSERT_OK(_scanner.do_open(&_state));
    ChunkPtr chunk = std::make_shared<Chunk>();
    ASSERT_OK(next(&chunk));
    expect_chunk_rows(chunk, {102, 104});
}

TEST_F(PaimonGlobalIndexScannerTest, IntersectsColumnsWithDifferentSegmentBoundaries) {
    ASSERT_NO_FATAL_FAILURE(add_segment(18, "v", 101, {10, 30, 10}));
    change_request(&request(), [&](auto& d) {
        auto binding = parse_json(R"({"fieldId":18,"provider":"btree"})");
        d["indexes"].AddMember("v", rapidjson::Value(binding, d.GetAllocator()), d.GetAllocator());
        auto predicate = parse_json(R"({"o":"cp","ct":"AND","c":[
            {"o":"b","b":"EQ","c":[{"o":"cr","t":"int","n":"k"},{"o":"co","t":"int","v":10}]},
            {"o":"b","b":"EQ","c":[{"o":"cr","t":"int","n":"v"},{"o":"co","t":"int","v":10}]}]})");
        d["predicate"].CopyFrom(predicate, d.GetAllocator());
    });
    ASSERT_OK(_scanner.do_open(&_state));
    ChunkPtr chunk = std::make_shared<Chunk>();
    ASSERT_OK(next(&chunk));
    expect_chunk_rows(chunk, {101});
}

TEST_F(PaimonGlobalIndexScannerTest, EmitsAnEmptyBitmapForNoMatches) {
    change_request(&request(), [](auto& d) { d["predicate"]["c"][1]["v"].SetInt(99); });
    ASSERT_OK(_scanner.do_open(&_state));
    ChunkPtr chunk = std::make_shared<Chunk>();
    ASSERT_OK(next(&chunk));
    expect_chunk_rows(chunk, {});
}

TEST_F(PaimonGlobalIndexScannerTest, EnforcesResultBudgetWithoutEmittingPartialRows) {
    change_request(&request(), [](auto& d) { d["resultBytesLimit"].SetInt(1); });
    ASSERT_OK(_scanner.do_open(&_state));
    ChunkPtr chunk = std::make_shared<Chunk>();
    EXPECT_TRUE(next(&chunk).is_mem_limit_exceeded());
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(PaimonGlobalIndexScannerTest, RejectsInconsistentFieldTypesAndMissingFileSystem) {
    auto duplicate = request().index_files[0];
    duplicate.__set_field_type("bigint");
    request().index_files.push_back(duplicate);
    auto status = _scanner.do_open(&_state);
    EXPECT_TRUE(status.is_invalid_argument()) << status;
    _scanner.do_close(&_state);
    request().index_files.pop_back();
    _context.fs = nullptr;
    EXPECT_TRUE(_scanner.do_open(&_state).is_internal_error());
}

TEST_F(PaimonGlobalIndexScannerTest, ReportsMissingIndexFileAndRetainsIoErrorCause) {
    request().index_files[0].path += ".missing";
    ChunkPtr chunk = std::make_shared<Chunk>();
    auto status = _scanner.do_open(&_state);
    if (status.ok()) status = next(&chunk);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.message().find("missing")) << status;
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(PaimonGlobalIndexScannerTest, RejectsCancellationBeforeOpenAndBeforeEvaluation) {
    _state.set_is_cancelled(true);
    EXPECT_TRUE(_scanner.do_open(&_state).is_cancelled());
    EXPECT_FALSE(PaimonGlobalIndexScannerTestAccessor::has_resources(_scanner));
    _state.set_is_cancelled(false);
    ASSERT_OK(_scanner.do_open(&_state));
    _state.set_is_cancelled(true);
    ChunkPtr chunk = std::make_shared<Chunk>();
    EXPECT_TRUE(next(&chunk).is_cancelled());
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(PaimonGlobalIndexScannerTest, PreservesCancellationDuringLazyMaterialization) {
    ASSERT_OK(_scanner.do_open(&_state));
    for (bool fail : {false, true}) {
        _state.set_is_cancelled(false);
        bool materialized = false;
        inject_result(
                std::make_shared<paimon::BitmapGlobalIndexResult>([&]() -> paimon::Result<paimon::RoaringBitmap64> {
                    materialized = true;
                    _state.set_is_cancelled(true);
                    if (fail) return paimon::Status::IOError("read failed after cancellation");
                    return paimon::RoaringBitmap64();
                }));
        ChunkPtr chunk = std::make_shared<Chunk>();
        EXPECT_TRUE(next(&chunk).is_cancelled());
        EXPECT_TRUE(materialized);
        EXPECT_EQ(0, chunk->num_rows());
    }
}

TEST_F(PaimonGlobalIndexScannerTest, ReportsLazySerializationFailureAndReaderFailure) {
    ASSERT_OK(_scanner.do_open(&_state));
    inject_result(std::make_shared<paimon::BitmapGlobalIndexResult>([]() -> paimon::Result<paimon::RoaringBitmap64> {
        return paimon::Status::IOError("injected lazy read failure");
    }));
    ChunkPtr chunk = std::make_shared<Chunk>();
    auto status = next(&chunk);
    EXPECT_TRUE(status.is_internal_error()) << status;
    EXPECT_NE(std::string::npos, status.message().find("injected lazy read failure"));
    EXPECT_EQ(0, chunk->num_rows());
    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({{100, 101}}));
    reader->fail_with(paimon::Status::IOError("injected reader failure"));
    PaimonGlobalIndexScannerTestAccessor::set_reader(&_scanner, 17, reader);
    status = next(&chunk);
    EXPECT_TRUE(status.is_internal_error());
    EXPECT_NE(std::string::npos, status.message().find("injected reader failure"));
    reader->fail_with(paimon::Status::OK());
    reader->set_return_null(true);
    EXPECT_TRUE(next(&chunk).is_not_supported());
}

TEST_F(PaimonGlobalIndexScannerTest, RejectsUnboundPredicateColumns) {
    change_request(&request(), [](auto& d) { d["predicate"]["c"][0]["n"].SetString("missing"); });
    ASSERT_OK(_scanner.do_open(&_state));
    ChunkPtr chunk = std::make_shared<Chunk>();
    auto status = next(&chunk);
    EXPECT_TRUE(status.is_invalid_argument());
    EXPECT_NE(std::string::npos, status.message().find("binding"));
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(PaimonGlobalIndexScannerTest, PreservesQueryMemoryLimitFailureBeforeOpeningFiles) {
    RuntimeState limited{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    auto tracker = std::make_shared<MemTracker>(1, "index query");
    limited.init_mem_trackers(tracker);
    tracker->consume(2);
    DeferOp restore([&] { tracker->release(2); });
    EXPECT_TRUE(_scanner.do_open(&limited).is_mem_limit_exceeded());
    EXPECT_TRUE(_source.opened_paths.empty());
    EXPECT_FALSE(PaimonGlobalIndexScannerTestAccessor::has_resources(_scanner));
}

TEST_F(PaimonGlobalIndexScannerTest, ClosingOneRangeDoesNotInvalidateAnotherRange) {
    ASSERT_OK(_scanner.do_open(&_state));
    RecordingFileSystem other_source;
    auto other_range = _range;
    other_range.paimon_global_index_scan_range.__set_range_from(102);
    HdfsScannerContext other_context;
    other_context.fs = &other_source;
    other_context.scan_range = &other_range;
    other_context.materialize_slots = _context.materialize_slots;
    PaimonGlobalIndexScanner other;
    ASSERT_OK(other.init(&_state, &other_context));
    DeferOp close_other([&] { other.do_close(&_state); });
    ASSERT_OK(other.do_open(&_state));
    auto retained = PaimonGlobalIndexScannerTestAccessor::file_system(&_scanner);
    _scanner.do_close(&_state);
    const auto old_opens = _source.opened_paths.size();
    EXPECT_TRUE(retained->Open(request().index_files[0].path).status().IsCancelled());
    ChunkPtr chunk = std::make_shared<Chunk>();
    ASSERT_OK(other.do_get_next(&_state, &chunk));
    expect_chunk_rows(chunk, {102});
    EXPECT_EQ(old_opens, _source.opened_paths.size());
    EXPECT_FALSE(other_source.opened_paths.empty());
}

TEST_F(PaimonGlobalIndexScannerTest, RetainedCallbackDoesNotAccessDestroyedRangeContext) {
    auto source = std::make_unique<RecordingFileSystem>();
    auto state = std::make_unique<RuntimeState>(TUniqueId(), TQueryOptions(), TQueryGlobals(),
                                                static_cast<ExecEnv*>(nullptr));
    state->init_mem_trackers(TUniqueId());
    std::weak_ptr<MemTracker> owner = state->query_mem_tracker_ptr();
    _context.fs = source.get();
    ASSERT_OK(_scanner.do_open(state.get()));
    auto retained = PaimonGlobalIndexScannerTestAccessor::file_system(&_scanner);
    _scanner.do_close(state.get());
    state.reset();
    source.reset();
    EXPECT_FALSE(owner.expired());
    EXPECT_TRUE(retained->Open(request().index_files[0].path).status().IsCancelled());
    retained.reset();
    EXPECT_TRUE(owner.expired());
}

TEST_F(PaimonGlobalIndexScannerTest, WaitsForInflightOpenBeforeFinishingOperation) {
    ASSERT_OK(_scanner.do_open(&_state));
    auto adapter = PaimonGlobalIndexScannerTestAccessor::file_system(&_scanner);
    auto sdk_reader = PaimonGlobalIndexScannerTestAccessor::reader(&_scanner, 17);
    std::promise<void> started;
    auto opened = started.get_future().share();
    std::promise<void> release;
    auto released = release.get_future();
    std::atomic<bool> first_open{true};
    _source.on_open = [&] {
        if (first_open.exchange(false)) {
            started.set_value();
            released.wait();
        }
    };
    std::thread worker;
    inject_result(std::make_shared<paimon::BitmapGlobalIndexResult>([&]() -> paimon::Result<paimon::RoaringBitmap64> {
        worker = std::thread([&] {
            // Use the SDK file reader so Open holds the operation lease. After Open returns,
            // finishing evaluation may cancel any remaining reads on this worker.
            auto result = sdk_reader->VisitEqual(paimon::Literal(10));
            if (result.ok()) (void)result.value()->ToRanges();
        });
        if (opened.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            return paimon::Status::IOError("SDK worker did not enter Open");
        }
        return paimon::RoaringBitmap64();
    }));
    ChunkPtr chunk = std::make_shared<Chunk>();
    auto finished = std::async(std::launch::async, [&] { return next(&chunk); });
    const auto ready = opened.wait_for(std::chrono::seconds(10));
    if (ready == std::future_status::ready) {
        EXPECT_EQ(std::future_status::timeout, finished.wait_for(std::chrono::milliseconds(20)));
    }
    release.set_value();
    auto status = finished.get();
    if (worker.joinable()) worker.join();
    _source.on_open = {};
    ASSERT_EQ(std::future_status::ready, ready);
    ASSERT_OK(status);
    EXPECT_TRUE(adapter->Open(request().index_files[0].path).status().IsCancelled());
}

} // namespace starrocks
