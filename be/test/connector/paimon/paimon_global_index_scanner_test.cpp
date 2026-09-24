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

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <paimon/global_index/bitmap_global_index_result.h>
#include <paimon/global_index/bitmap_scored_global_index_result.h>
#include <paimon/global_index/global_index_reader.h>
#include <paimon/memory/memory_pool.h>

#include <cstdlib>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "common/runtime_profile.h"
#include "connector/hive/paimon/paimon_file_system.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs_memory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class PaimonGlobalIndexScannerTestAccessor {
public:
    static Status parse_request(const TPaimonGlobalIndexScanRange& scan_range, rapidjson::Document* request) {
        return PaimonGlobalIndexScanner::_parse_request(scan_range, request);
    }

    static Status parse_request(PaimonGlobalIndexScanner* scanner) { return scanner->_parse_request(); }

    static StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> evaluate(PaimonGlobalIndexScanner* scanner,
                                                                         RuntimeState* runtime_state) {
        return scanner->_evaluate(runtime_state);
    }

    static float normalize_score(const rapidjson::Value& score_expression, float score) {
        return PaimonGlobalIndexScanner::_normalize_score(score_expression, score);
    }

    static void set_scan(PaimonGlobalIndexScanner* scanner, std::unique_ptr<paimon::GlobalIndexScan> scan) {
        scanner->_global_index_scan = std::move(scan);
    }

    static void set_scan_factory(
            PaimonGlobalIndexScanner* scanner,
            std::function<paimon::Result<std::unique_ptr<paimon::GlobalIndexScan>>()> scan_factory) {
        scanner->_scan_factory_for_test = std::move(scan_factory);
    }

    static void set_memory_pool(PaimonGlobalIndexScanner* scanner, std::shared_ptr<paimon::MemoryPool> pool) {
        scanner->_memory_pool = std::move(pool);
    }

    static bool has_resources(const PaimonGlobalIndexScanner& scanner) {
        return scanner._global_index_scan != nullptr || scanner._paimon_file_system != nullptr ||
               scanner._memory_pool != nullptr;
    }

    static void seed_lifecycle_state(PaimonGlobalIndexScanner* scanner) {
        scanner->_emitted = true;
        scanner->_scored_rows = 17;
    }

    static bool emitted(const PaimonGlobalIndexScanner& scanner) { return scanner._emitted; }
    static int64_t scored_rows(const PaimonGlobalIndexScanner& scanner) { return scanner._scored_rows; }
};

namespace {

class FixedPeakMemoryPool final : public paimon::MemoryPool {
public:
    explicit FixedPeakMemoryPool(uint64_t peak) : _peak(peak) {}

    void* Malloc(uint64_t size, uint64_t) override { return std::malloc(size); }
    void* Realloc(void* p, size_t, size_t new_size, uint64_t) override { return std::realloc(p, new_size); }
    void Free(void* p, uint64_t) override { std::free(p); }
    uint64_t CurrentUsage() const override { return 0; }
    uint64_t MaxMemoryUsage() const override { return _peak; }

private:
    uint64_t _peak;
};

class FakeGlobalIndexReader final : public paimon::GlobalIndexReader {
public:
    explicit FakeGlobalIndexReader(std::shared_ptr<paimon::GlobalIndexResult> result) : _result(std::move(result)) {}

    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitIsNotNull() override { return read(); }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitIsNull() override { return read(); }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitEqual(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitNotEqual(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitLessThan(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitLessOrEqual(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitGreaterThan(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitGreaterOrEqual(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitIn(const std::vector<paimon::Literal>&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitNotIn(
            const std::vector<paimon::Literal>&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitStartsWith(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitEndsWith(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitContains(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitLike(const paimon::Literal&) override {
        return read();
    }
    paimon::Result<std::shared_ptr<paimon::ScoredGlobalIndexResult>> VisitVectorSearch(
            const std::shared_ptr<paimon::VectorSearch>& search) override {
        _last_vector_search = search;
        if (_vector_result == nullptr) {
            return paimon::Status::Invalid("unused vector search");
        }
        return _vector_result;
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitFullTextSearch(
            const std::shared_ptr<paimon::FullTextSearch>&) override {
        return paimon::Status::Invalid("unused full-text search");
    }
    bool IsThreadSafe() const override { return true; }
    std::string GetIndexType() const override { return "fake"; }

    const std::shared_ptr<paimon::VectorSearch>& last_vector_search() const { return _last_vector_search; }
    void set_vector_result(std::shared_ptr<paimon::ScoredGlobalIndexResult> result) {
        _vector_result = std::move(result);
    }

private:
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> read() { return _result; }

    std::shared_ptr<paimon::GlobalIndexResult> _result;
    std::shared_ptr<paimon::ScoredGlobalIndexResult> _vector_result;
    std::shared_ptr<paimon::VectorSearch> _last_vector_search;
};

class FakeGlobalIndexScan final : public paimon::GlobalIndexScan {
public:
    explicit FakeGlobalIndexScan(std::shared_ptr<paimon::GlobalIndexReader> reader) : _reader(std::move(reader)) {}

    paimon::Result<std::vector<std::shared_ptr<paimon::GlobalIndexReader>>> CreateReaders(
            const std::string&, const std::optional<paimon::RowRangeIndex>&) const override {
        if (!_status.ok()) {
            return _status;
        }
        return std::vector<std::shared_ptr<paimon::GlobalIndexReader>>{_reader};
    }

    paimon::Result<std::shared_ptr<paimon::GlobalIndexReader>> CreateReader(
            const std::string& field_name, const std::string& index_type,
            const std::optional<paimon::RowRangeIndex>& row_range_index) const override {
        _last_field_name = field_name;
        _last_index_type = index_type;
        _had_row_range = row_range_index.has_value();
        if (!_status.ok()) {
            return _status;
        }
        if (_return_null) {
            return std::shared_ptr<paimon::GlobalIndexReader>();
        }
        return _reader;
    }

    paimon::Result<std::vector<std::shared_ptr<paimon::GlobalIndexReader>>> CreateReaders(
            int32_t, const std::optional<paimon::RowRangeIndex>&) const override {
        if (!_status.ok()) {
            return _status;
        }
        return std::vector<std::shared_ptr<paimon::GlobalIndexReader>>{_reader};
    }

    void fail_with(paimon::Status status) { _status = std::move(status); }
    void set_return_null(bool return_null) { _return_null = return_null; }
    const std::string& last_field_name() const { return _last_field_name; }
    const std::string& last_index_type() const { return _last_index_type; }
    bool had_row_range() const { return _had_row_range; }

private:
    std::shared_ptr<paimon::GlobalIndexReader> _reader;
    paimon::Status _status;
    bool _return_null = false;
    mutable std::string _last_field_name;
    mutable std::string _last_index_type;
    mutable bool _had_row_range = false;
};

class UnsupportedGlobalIndexResult final : public paimon::GlobalIndexResult {
public:
    paimon::Result<bool> IsEmpty() const override { return false; }
    paimon::Result<std::unique_ptr<Iterator>> CreateIterator() const override {
        return paimon::Status::NotImplemented("unused iterator");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> AddOffset(int64_t) override {
        return shared_from_this();
    }
    std::string ToString() const override { return "unsupported"; }
};

class IteratorErrorScoredResult final : public paimon::BitmapScoredGlobalIndexResult {
public:
    IteratorErrorScoredResult() : paimon::BitmapScoredGlobalIndexResult(paimon::RoaringBitmap64::From({12}), {0.25f}) {}

    paimon::Result<std::unique_ptr<paimon::ScoredGlobalIndexResult::ScoredIterator>> CreateScoredIterator()
            const override {
        return paimon::Status::IOError("injected scored iterator failure");
    }
};

TPaimonGlobalIndexScanRange valid_scan_range() {
    TPaimonGlobalIndexScanRange scan_range;
    scan_range.__set_protocol_version(1);
    scan_range.__set_shard_id(0);
    scan_range.__set_range_from(0);
    scan_range.__set_range_to(9);
    scan_range.__set_query_json(
            R"({"version":1,"snapshotId":7,"predicate":{"o":"isn","ng":false,"c":[{"o":"cr","t":"int","n":"k"}]},"indexes":{"k":"range"}})");
    scan_range.__set_table_path("s3://warehouse/db/table");
    scan_range.__set_snapshot_id(7);
    return scan_range;
}

TPaimonGlobalIndexScanRange valid_top_n_scan_range(int32_t local_limit = 3,
                                                   std::string_view function = "approx_cosine_similarity",
                                                   bool ascending = false) {
    TPaimonGlobalIndexScanRange scan_range;
    scan_range.__set_protocol_version(2);
    scan_range.__set_shard_id(1);
    scan_range.__set_range_from(10);
    scan_range.__set_range_to(19);
    scan_range.__set_query_json(fmt::format(
            R"({{"version":2,"snapshotId":7,"kind":"top_n","scoreExpression":{{"o":"ca","f":"{}","a":[{{"o":"cr","t":"array<float>","n":"embedding"}},{{"o":"a","i":"float","c":[{{"o":"co","t":"float","v":1.25}},{{"o":"co","t":"float","v":2.5}}]}}]}},"localLimit":{},"ascending":{},"indexes":{{"embedding":"vector"}}}})",
            function, local_limit, ascending));
    scan_range.__set_table_path("s3://warehouse/db/table");
    scan_range.__set_snapshot_id(7);
    return scan_range;
}

Status parse_request(const TPaimonGlobalIndexScanRange& scan_range) {
    rapidjson::Document request;
    return PaimonGlobalIndexScannerTestAccessor::parse_request(scan_range, &request);
}

THdfsScanRange valid_hdfs_scan_range() {
    THdfsScanRange scan_range;
    scan_range.__set_paimon_global_index_scan_range(valid_scan_range());
    return scan_range;
}

THdfsScanRange valid_top_n_hdfs_scan_range(int32_t local_limit = 3) {
    THdfsScanRange scan_range;
    scan_range.__set_paimon_global_index_scan_range(valid_top_n_scan_range(local_limit));
    return scan_range;
}

std::shared_ptr<paimon::GlobalIndexResult> result_from_ranges(std::vector<paimon::Range> ranges) {
    return paimon::BitmapGlobalIndexResult::FromRanges(ranges);
}

std::shared_ptr<paimon::ScoredGlobalIndexResult> scored_result(std::vector<int64_t> row_ids,
                                                               std::vector<float> scores) {
    return std::make_shared<paimon::BitmapScoredGlobalIndexResult>(paimon::RoaringBitmap64::From(row_ids),
                                                                   std::move(scores));
}

Status init_scanner(PaimonGlobalIndexScanner* scanner, RuntimeState* runtime_state, HdfsScannerContext* scanner_ctx,
                    const THdfsScanRange* scan_range, FileSystem* file_system) {
    scanner_ctx->scan_range = scan_range;
    scanner_ctx->fs = file_system;
    return scanner->init(runtime_state, scanner_ctx);
}

TEST(PaimonGlobalIndexScannerTest, AcceptsConsistentVersionedRequest) {
    auto scan_range = valid_scan_range();
    rapidjson::Document request;

    Status status = PaimonGlobalIndexScannerTestAccessor::parse_request(scan_range, &request);

    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(request.IsObject());
    EXPECT_EQ(7, request["snapshotId"].GetInt64());
}

TEST(PaimonGlobalIndexScannerTest, AcceptsScalarV1AndVectorTopNV2Protocols) {
    EXPECT_TRUE(parse_request(valid_scan_range()).ok());

    struct TestCase {
        const char* function;
        bool ascending;
    };
    const std::vector<TestCase> test_cases = {
            {"approx_l2_distance", true}, {"approx_inner_product", false}, {"approx_cosine_similarity", false}};
    for (const TestCase& test_case : test_cases) {
        auto scan_range = valid_top_n_scan_range(3, test_case.function, test_case.ascending);
        rapidjson::Document request;
        Status status = PaimonGlobalIndexScannerTestAccessor::parse_request(scan_range, &request);
        ASSERT_TRUE(status.ok()) << test_case.function << ": " << status;
        EXPECT_EQ(2, request["version"].GetInt());
        EXPECT_STREQ("top_n", request["kind"].GetString());
    }
}

TEST(PaimonGlobalIndexScannerTest, NormalizesScoresToStarRocksSemantics) {
    auto normalize = [](std::string_view function, float score) {
        rapidjson::Document request;
        request.Parse(fmt::format(R"({{"f":"{}"}})", function).c_str());
        EXPECT_FALSE(request.HasParseError());
        return PaimonGlobalIndexScannerTestAccessor::normalize_score(request, score);
    };

    EXPECT_FLOAT_EQ(0.25f, normalize("approx_l2_distance", 0.25f));
    EXPECT_FLOAT_EQ(0.75f, normalize("approx_inner_product", 0.75f));
    EXPECT_FLOAT_EQ(0.75f, normalize("approx_cosine_similarity", 0.25f));
}

TEST(PaimonGlobalIndexScannerTest, ResetsPerRangeLifecycleStateOnInit) {
    RuntimeState runtime_state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    runtime_state.init_mem_trackers(TUniqueId());
    MemoryFileSystem file_system;
    HdfsScannerContext scanner_ctx;
    THdfsScanRange scan_range = valid_top_n_hdfs_scan_range();
    PaimonGlobalIndexScanner scanner;
    PaimonGlobalIndexScannerTestAccessor::seed_lifecycle_state(&scanner);

    ASSERT_TRUE(init_scanner(&scanner, &runtime_state, &scanner_ctx, &scan_range, &file_system).ok());
    EXPECT_FALSE(PaimonGlobalIndexScannerTestAccessor::emitted(scanner));
    EXPECT_EQ(0, PaimonGlobalIndexScannerTestAccessor::scored_rows(scanner));
}

TEST(PaimonGlobalIndexScannerTest, RejectsIncompleteAndUnsupportedProtocol) {
    TPaimonGlobalIndexScanRange incomplete;
    incomplete.__set_protocol_version(1);
    EXPECT_TRUE(parse_request(incomplete).is_invalid_argument());

    auto unsupported = valid_scan_range();
    unsupported.__set_protocol_version(3);
    Status status = parse_request(unsupported);
    EXPECT_TRUE(status.is_invalid_argument());
    EXPECT_NE(std::string::npos, status.message().find("protocol version 3"));
}

TEST(PaimonGlobalIndexScannerTest, RejectsInvalidShardAndJson) {
    auto invalid_range = valid_scan_range();
    invalid_range.__set_range_from(10);
    invalid_range.__set_range_to(9);
    EXPECT_TRUE(parse_request(invalid_range).is_invalid_argument());

    auto invalid_json = valid_scan_range();
    invalid_json.__set_query_json("not-json");
    EXPECT_TRUE(parse_request(invalid_json).is_invalid_argument());
}

TEST(PaimonGlobalIndexScannerTest, RejectsInconsistentEnvelope) {
    auto snapshot_mismatch = valid_scan_range();
    snapshot_mismatch.__set_snapshot_id(8);
    EXPECT_TRUE(parse_request(snapshot_mismatch).is_invalid_argument());

    auto missing_indexes = valid_scan_range();
    missing_indexes.__set_query_json(R"({"version":1,"snapshotId":7,"predicate":{"o":"isn","ng":false,"c":[]}})");
    EXPECT_TRUE(parse_request(missing_indexes).is_invalid_argument());

    auto missing_predicate = valid_scan_range();
    missing_predicate.__set_query_json(R"({"version":1,"snapshotId":7,"indexes":{"k":"range"}})");
    EXPECT_TRUE(parse_request(missing_predicate).is_invalid_argument());

    auto predicate_with_top_n_fields = valid_scan_range();
    predicate_with_top_n_fields.__set_query_json(
            R"({"version":1,"snapshotId":7,"predicate":{"o":"isn","ng":false,"c":[]},"kind":"top_n","indexes":{"k":"range"}})");
    EXPECT_TRUE(parse_request(predicate_with_top_n_fields).is_invalid_argument());

    auto top_n_with_predicate = valid_top_n_scan_range();
    top_n_with_predicate.__set_query_json(
            R"({"version":2,"snapshotId":7,"kind":"top_n","scoreExpression":{"o":"ca","f":"approx_cosine_similarity","a":[]},"localLimit":3,"ascending":false,"predicate":{},"indexes":{"embedding":"vector"}})");
    EXPECT_TRUE(parse_request(top_n_with_predicate).is_invalid_argument());

    auto top_n_with_scalar_index = valid_top_n_scan_range();
    top_n_with_scalar_index.__set_query_json(
            R"({"version":2,"snapshotId":7,"kind":"top_n","scoreExpression":{"o":"ca","f":"approx_cosine_similarity","a":[]},"localLimit":3,"ascending":false,"indexes":{"embedding":"range"}})");
    EXPECT_TRUE(parse_request(top_n_with_scalar_index).is_invalid_argument());

    EXPECT_TRUE(parse_request(valid_top_n_scan_range(3, "approx_l2_distance", false)).is_invalid_argument());
    EXPECT_TRUE(parse_request(valid_top_n_scan_range(3, "approx_inner_product", true)).is_invalid_argument());

    auto top_n_request_with_v1_range = valid_top_n_scan_range();
    top_n_request_with_v1_range.__set_protocol_version(1);
    EXPECT_TRUE(parse_request(top_n_request_with_v1_range).is_invalid_argument());

    const std::vector<std::string> malformed_envelopes = {
            R"({"version":"1","snapshotId":7,"predicate":{},"indexes":{}})",
            R"({"version":1,"snapshotId":"7","predicate":{},"indexes":{}})",
            R"({"version":1,"snapshotId":7,"predicate":[],"indexes":{}})",
            R"({"version":1,"snapshotId":7,"predicate":{},"indexes":[]})"};
    for (const std::string& envelope : malformed_envelopes) {
        auto malformed = valid_scan_range();
        malformed.__set_query_json(envelope);
        EXPECT_TRUE(parse_request(malformed).is_invalid_argument()) << envelope;
    }
}

TEST(PaimonGlobalIndexScannerTest, OpensWithInjectedFactoryAndReleasesResources) {
    RuntimeState runtime_state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    runtime_state.init_mem_trackers(TUniqueId());
    MemoryFileSystem file_system;
    HdfsScannerContext scanner_ctx;
    THdfsScanRange scan_range = valid_hdfs_scan_range();
    PaimonGlobalIndexScanner scanner;
    ASSERT_TRUE(init_scanner(&scanner, &runtime_state, &scanner_ctx, &scan_range, &file_system).ok());

    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    PaimonGlobalIndexScannerTestAccessor::set_scan_factory(
            &scanner, [reader]() -> paimon::Result<std::unique_ptr<paimon::GlobalIndexScan>> {
                return std::make_unique<FakeGlobalIndexScan>(reader);
            });

    ASSERT_TRUE(scanner.do_open(&runtime_state).ok());
    EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::has_resources(scanner));
    EXPECT_GE(scanner.estimated_mem_usage(), 0);

    HdfsScannerProfile empty_profile;
    scanner.do_update_counter(&empty_profile);
    RuntimeProfile runtime_profile("paimon-global-index-test");
    HdfsScannerProfile profile;
    profile.runtime_profile = &runtime_profile;
    scanner.do_update_counter(&profile);
    EXPECT_NE(nullptr, runtime_profile.get_counter("OpenTime"));
    EXPECT_NE(nullptr, runtime_profile.get_counter("AppIOCount"));

    scanner.do_close(&runtime_state);
    EXPECT_FALSE(PaimonGlobalIndexScannerTestAccessor::has_resources(scanner));
    EXPECT_EQ(0, scanner.estimated_mem_usage());
}

TEST(PaimonGlobalIndexScannerTest, RejectsOpenFailures) {
    RuntimeState runtime_state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    runtime_state.init_mem_trackers(TUniqueId());
    MemoryFileSystem file_system;
    THdfsScanRange scan_range = valid_hdfs_scan_range();

    HdfsScannerContext missing_fs_ctx;
    PaimonGlobalIndexScanner missing_fs_scanner;
    ASSERT_TRUE(init_scanner(&missing_fs_scanner, &runtime_state, &missing_fs_ctx, &scan_range, nullptr).ok());
    EXPECT_TRUE(missing_fs_scanner.do_open(&runtime_state).is_internal_error());

    HdfsScannerContext invalid_request_ctx;
    THdfsScanRange invalid_request_range;
    PaimonGlobalIndexScanner invalid_request_scanner;
    ASSERT_TRUE(init_scanner(&invalid_request_scanner, &runtime_state, &invalid_request_ctx, &invalid_request_range,
                             &file_system)
                        .ok());
    EXPECT_TRUE(invalid_request_scanner.do_open(&runtime_state).is_invalid_argument());

    HdfsScannerContext failed_factory_ctx;
    PaimonGlobalIndexScanner failed_factory_scanner;
    ASSERT_TRUE(
            init_scanner(&failed_factory_scanner, &runtime_state, &failed_factory_ctx, &scan_range, &file_system).ok());
    PaimonGlobalIndexScannerTestAccessor::set_scan_factory(
            &failed_factory_scanner, []() -> paimon::Result<std::unique_ptr<paimon::GlobalIndexScan>> {
                return paimon::Status::IOError("cannot create scan");
            });
    Status status = failed_factory_scanner.do_open(&runtime_state);
    EXPECT_TRUE(status.is_internal_error());
    EXPECT_NE(std::string::npos, status.message().find("cannot create scan"));

    runtime_state.set_is_cancelled(true);
    HdfsScannerContext cancelled_ctx;
    PaimonGlobalIndexScanner cancelled_scanner;
    ASSERT_TRUE(init_scanner(&cancelled_scanner, &runtime_state, &cancelled_ctx, &scan_range, &file_system).ok());
    EXPECT_TRUE(cancelled_scanner.do_open(&runtime_state).is_cancelled());
}

TEST(PaimonGlobalIndexScannerTest, EvaluatesValidatedRangeWithSelectedReader) {
    RuntimeState runtime_state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    runtime_state.init_mem_trackers(TUniqueId());
    MemoryFileSystem file_system;
    HdfsScannerContext scanner_ctx;
    THdfsScanRange scan_range = valid_hdfs_scan_range();
    PaimonGlobalIndexScanner scanner;
    ASSERT_TRUE(init_scanner(&scanner, &runtime_state, &scanner_ctx, &scan_range, &file_system).ok());
    ASSERT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse_request(&scanner).ok());

    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(2, 4)}));
    auto fake_scan = std::make_unique<FakeGlobalIndexScan>(reader);
    FakeGlobalIndexScan* fake_scan_ptr = fake_scan.get();
    PaimonGlobalIndexScannerTestAccessor::set_scan(&scanner, std::move(fake_scan));

    auto result = PaimonGlobalIndexScannerTestAccessor::evaluate(&scanner, &runtime_state);

    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ("k", fake_scan_ptr->last_field_name());
    EXPECT_EQ("btree", fake_scan_ptr->last_index_type());
    EXPECT_TRUE(fake_scan_ptr->had_row_range());
    auto ranges = result.value()->ToRanges();
    ASSERT_TRUE(ranges.ok()) << ranges.status().ToString();
    ASSERT_EQ(1, ranges.value().size());
    EXPECT_EQ(2, ranges.value()[0].from);
    EXPECT_EQ(4, ranges.value()[0].to);
}

TEST(PaimonGlobalIndexScannerTest, RejectsReaderSelectionFailures) {
    RuntimeState runtime_state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    runtime_state.init_mem_trackers(TUniqueId());
    MemoryFileSystem file_system;

    auto evaluate = [&](const std::string& query_json, std::unique_ptr<FakeGlobalIndexScan> fake_scan) {
        THdfsScanRange scan_range = valid_hdfs_scan_range();
        scan_range.paimon_global_index_scan_range.__set_query_json(query_json);
        HdfsScannerContext scanner_ctx;
        PaimonGlobalIndexScanner scanner;
        EXPECT_TRUE(init_scanner(&scanner, &runtime_state, &scanner_ctx, &scan_range, &file_system).ok());
        EXPECT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse_request(&scanner).ok());
        PaimonGlobalIndexScannerTestAccessor::set_scan(&scanner, std::move(fake_scan));
        return PaimonGlobalIndexScannerTestAccessor::evaluate(&scanner, &runtime_state);
    };

    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    auto missing = evaluate(
            R"({"version":1,"snapshotId":7,"predicate":{"o":"isn","ng":false,"c":[{"o":"cr","n":"k"}]},"indexes":{"other":"range"}})",
            std::make_unique<FakeGlobalIndexScan>(reader));
    ASSERT_FALSE(missing.ok());
    EXPECT_TRUE(missing.status().is_invalid_argument());

    auto unsupported = evaluate(
            R"({"version":1,"snapshotId":7,"predicate":{"o":"isn","ng":false,"c":[{"o":"cr","n":"k"}]},"indexes":{"k":"bitmap"}})",
            std::make_unique<FakeGlobalIndexScan>(reader));
    ASSERT_FALSE(unsupported.ok());
    EXPECT_TRUE(unsupported.status().is_invalid_argument());

    auto failing_scan = std::make_unique<FakeGlobalIndexScan>(reader);
    failing_scan->fail_with(paimon::Status::IOError("reader load failed"));
    auto failed = evaluate(valid_scan_range().query_json, std::move(failing_scan));
    ASSERT_FALSE(failed.ok());
    EXPECT_TRUE(failed.status().is_internal_error());

    auto null_scan = std::make_unique<FakeGlobalIndexScan>(reader);
    null_scan->set_return_null(true);
    auto disappeared = evaluate(valid_scan_range().query_json, std::move(null_scan));
    ASSERT_FALSE(disappeared.ok());
    EXPECT_TRUE(disappeared.status().is_internal_error());
    EXPECT_NE(std::string::npos, disappeared.status().message().find("disappeared"));

    runtime_state.set_is_cancelled(true);
    auto cancelled = evaluate(valid_scan_range().query_json, std::make_unique<FakeGlobalIndexScan>(reader));
    ASSERT_FALSE(cancelled.ok());
    EXPECT_TRUE(cancelled.status().is_cancelled());
}

TEST(PaimonGlobalIndexScannerTest, EmitsSerializedResultAndStopsAfterOneRow) {
    RuntimeState runtime_state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    runtime_state.init_mem_trackers(TUniqueId());
    ObjectPool pool;
    parquet::Utils::SlotDesc slots[] = {{"index_result", TYPE_VARBINARY_DESC, 1},
                                        {"args", TYPE_VARCHAR_DESC, 2},
                                        {"unused", TYPE_INT_DESC, 3},
                                        {""}};
    TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(&runtime_state, &pool, slots);

    MemoryFileSystem file_system;
    HdfsScannerContext scanner_ctx;
    scanner_ctx.materialize_slots = tuple_desc->slots();
    THdfsScanRange scan_range = valid_hdfs_scan_range();
    PaimonGlobalIndexScanner scanner;
    ASSERT_TRUE(init_scanner(&scanner, &runtime_state, &scanner_ctx, &scan_range, &file_system).ok());
    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(2, 4)}));
    PaimonGlobalIndexScannerTestAccessor::set_scan_factory(
            &scanner, [reader]() -> paimon::Result<std::unique_ptr<paimon::GlobalIndexScan>> {
                return std::make_unique<FakeGlobalIndexScan>(reader);
            });
    ASSERT_TRUE(scanner.do_open(&runtime_state).ok());

    ChunkPtr chunk = std::make_shared<Chunk>();
    ASSERT_TRUE(scanner.do_get_next(&runtime_state, &chunk).ok());
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_EQ(3, chunk->num_columns());
    EXPECT_FALSE(chunk->get_column_by_slot_id(1)->get(0).get_slice().empty());
    EXPECT_EQ(scan_range.paimon_global_index_scan_range.query_json,
              chunk->get_column_by_slot_id(2)->get(0).get_slice().to_string());
    EXPECT_TRUE(chunk->get_column_by_slot_id(3)->is_null(0));
    EXPECT_TRUE(scanner.do_get_next(&runtime_state, &chunk).is_end_of_file());
}

TEST(PaimonGlobalIndexScannerTest, EmitsScoredVectorCandidatesAndStopsAfterOneBatch) {
    RuntimeState runtime_state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    runtime_state.init_mem_trackers(TUniqueId());
    ObjectPool pool;
    parquet::Utils::SlotDesc slots[] = {{"row_id", TYPE_BIGINT_DESC, 1},
                                        {"score", TYPE_FLOAT_DESC, 2},
                                        {"args", TYPE_VARCHAR_DESC, 3},
                                        {"unused", TYPE_INT_DESC, 4},
                                        {""}};
    TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(&runtime_state, &pool, slots);

    MemoryFileSystem file_system;
    HdfsScannerContext scanner_ctx;
    scanner_ctx.materialize_slots = tuple_desc->slots();
    THdfsScanRange scan_range = valid_top_n_hdfs_scan_range();
    PaimonGlobalIndexScanner scanner;
    ASSERT_TRUE(init_scanner(&scanner, &runtime_state, &scanner_ctx, &scan_range, &file_system).ok());

    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(10, 19)}));
    reader->set_vector_result(scored_result({12, 17}, {0.25f, 0.75f}));
    FakeGlobalIndexScan* fake_scan = nullptr;
    PaimonGlobalIndexScannerTestAccessor::set_scan_factory(
            &scanner, [&]() -> paimon::Result<std::unique_ptr<paimon::GlobalIndexScan>> {
                auto scan = std::make_unique<FakeGlobalIndexScan>(reader);
                fake_scan = scan.get();
                return scan;
            });

    ASSERT_TRUE(scanner.do_open(&runtime_state).ok());
    ChunkPtr chunk = std::make_shared<Chunk>();
    ASSERT_TRUE(scanner.do_get_next(&runtime_state, &chunk).ok());

    ASSERT_NE(nullptr, fake_scan);
    EXPECT_EQ("embedding", fake_scan->last_field_name());
    EXPECT_EQ("lumina", fake_scan->last_index_type());
    EXPECT_TRUE(fake_scan->had_row_range());
    ASSERT_NE(nullptr, reader->last_vector_search());
    EXPECT_EQ("embedding", reader->last_vector_search()->field_name);
    EXPECT_EQ(3, reader->last_vector_search()->limit);
    EXPECT_EQ((std::vector<float>{1.25f, 2.5f}), reader->last_vector_search()->query);
    ASSERT_TRUE(reader->last_vector_search()->distance_type.has_value());
    EXPECT_EQ(paimon::VectorSearch::DistanceType::COSINE, reader->last_vector_search()->distance_type.value());

    ASSERT_EQ(2, chunk->num_rows());
    ASSERT_EQ(4, chunk->num_columns());
    EXPECT_EQ(12, chunk->get_column_by_slot_id(1)->get(0).get_int64());
    EXPECT_EQ(17, chunk->get_column_by_slot_id(1)->get(1).get_int64());
    EXPECT_FLOAT_EQ(0.75f, chunk->get_column_by_slot_id(2)->get(0).get_float());
    EXPECT_FLOAT_EQ(0.25f, chunk->get_column_by_slot_id(2)->get(1).get_float());
    EXPECT_EQ(scan_range.paimon_global_index_scan_range.query_json,
              chunk->get_column_by_slot_id(3)->get(0).get_slice().to_string());
    EXPECT_EQ(scan_range.paimon_global_index_scan_range.query_json,
              chunk->get_column_by_slot_id(3)->get(1).get_slice().to_string());
    EXPECT_TRUE(chunk->get_column_by_slot_id(4)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_slot_id(4)->is_null(1));
    EXPECT_TRUE(scanner.do_get_next(&runtime_state, &chunk).is_end_of_file());

    RuntimeProfile runtime_profile("paimon-vector-top-n-test");
    HdfsScannerProfile profile;
    profile.runtime_profile = &runtime_profile;
    scanner.do_update_counter(&profile);
    ASSERT_NE(nullptr, runtime_profile.get_counter("ScoredRows"));
    EXPECT_EQ(2, runtime_profile.get_counter("ScoredRows")->value());
}

TEST(PaimonGlobalIndexScannerTest, RejectsInvalidScoredVectorResults) {
    auto execute = [](std::shared_ptr<paimon::ScoredGlobalIndexResult> result, int32_t local_limit) {
        RuntimeState runtime_state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
        runtime_state.init_mem_trackers(TUniqueId());
        MemoryFileSystem file_system;
        HdfsScannerContext scanner_ctx;
        THdfsScanRange scan_range = valid_top_n_hdfs_scan_range(local_limit);
        PaimonGlobalIndexScanner scanner;
        Status status = init_scanner(&scanner, &runtime_state, &scanner_ctx, &scan_range, &file_system);
        if (!status.ok()) {
            return status;
        }
        auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(10, 19)}));
        reader->set_vector_result(std::move(result));
        PaimonGlobalIndexScannerTestAccessor::set_scan_factory(
                &scanner, [reader]() -> paimon::Result<std::unique_ptr<paimon::GlobalIndexScan>> {
                    return std::make_unique<FakeGlobalIndexScan>(reader);
                });
        status = scanner.do_open(&runtime_state);
        if (!status.ok()) {
            return status;
        }
        ChunkPtr chunk = std::make_shared<Chunk>();
        return scanner.do_get_next(&runtime_state, &chunk);
    };

    Status outside_shard = execute(scored_result({20}, {0.25f}), 3);
    EXPECT_TRUE(outside_shard.is_internal_error());
    EXPECT_NE(std::string::npos, outside_shard.message().find("outside shard [10, 19]"));

    Status iterator_error = execute(std::make_shared<IteratorErrorScoredResult>(), 3);
    EXPECT_TRUE(iterator_error.is_internal_error());
    EXPECT_NE(std::string::npos, iterator_error.message().find("injected scored iterator failure"));

    Status over_limit = execute(scored_result({10, 11, 12}, {0.1f, 0.2f, 0.3f}), 2);
    EXPECT_TRUE(over_limit.is_internal_error());
    EXPECT_NE(std::string::npos, over_limit.message().find("more than the requested 2 candidates"));

    Status non_finite = execute(scored_result({12}, {std::numeric_limits<float>::infinity()}), 3);
    EXPECT_TRUE(non_finite.is_internal_error());
    EXPECT_NE(std::string::npos, non_finite.message().find("non-finite score"));
}

TEST(PaimonGlobalIndexScannerTest, PropagatesSerializationFailureAndReportsInjectedPeakMemory) {
    RuntimeState runtime_state{TUniqueId(), TQueryOptions(), TQueryGlobals(), static_cast<ExecEnv*>(nullptr)};
    runtime_state.init_mem_trackers(TUniqueId());
    MemoryFileSystem file_system;
    HdfsScannerContext scanner_ctx;
    THdfsScanRange scan_range = valid_hdfs_scan_range();
    PaimonGlobalIndexScanner scanner;
    ASSERT_TRUE(init_scanner(&scanner, &runtime_state, &scanner_ctx, &scan_range, &file_system).ok());
    ASSERT_TRUE(PaimonGlobalIndexScannerTestAccessor::parse_request(&scanner).ok());
    auto unsupported_result = std::make_shared<UnsupportedGlobalIndexResult>();
    auto reader = std::make_shared<FakeGlobalIndexReader>(unsupported_result);
    PaimonGlobalIndexScannerTestAccessor::set_scan(&scanner, std::make_unique<FakeGlobalIndexScan>(reader));
    PaimonGlobalIndexScannerTestAccessor::set_memory_pool(&scanner, std::make_shared<FixedPeakMemoryPool>(4096));
    EXPECT_EQ(4096, scanner.estimated_mem_usage());

    ChunkPtr chunk = std::make_shared<Chunk>();
    Status status = scanner.do_get_next(&runtime_state, &chunk);
    EXPECT_TRUE(status.is_internal_error());
    EXPECT_NE(std::string::npos, status.message().find("serialization"));
}

} // namespace
} // namespace starrocks
