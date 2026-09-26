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

#include "connector/hive/paimon/paimon_global_index_evaluator.h"

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <paimon/global_index/bitmap_global_index_result.h>
#include <paimon/global_index/bitmap_scored_global_index_result.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace starrocks {
namespace {

class FakeGlobalIndexReader final : public paimon::GlobalIndexReader {
public:
    explicit FakeGlobalIndexReader(std::shared_ptr<paimon::GlobalIndexResult> result) : _result(std::move(result)) {}

    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitIsNotNull() override {
        return record("is_not_null");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitIsNull() override { return record("is_null"); }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitEqual(const paimon::Literal&) override {
        return record("equal");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitNotEqual(const paimon::Literal&) override {
        return record("not_equal");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitLessThan(const paimon::Literal&) override {
        return record("less_than");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitLessOrEqual(const paimon::Literal&) override {
        return record("less_or_equal");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitGreaterThan(const paimon::Literal&) override {
        return record("greater_than");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitGreaterOrEqual(const paimon::Literal&) override {
        return record("greater_or_equal");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitIn(const std::vector<paimon::Literal>&) override {
        return record("in");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitNotIn(
            const std::vector<paimon::Literal>&) override {
        return record("not_in");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitStartsWith(const paimon::Literal&) override {
        return record("starts_with");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitEndsWith(const paimon::Literal&) override {
        return record("ends_with");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitContains(const paimon::Literal&) override {
        return record("contains");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitLike(const paimon::Literal&) override {
        return record("like");
    }
    paimon::Result<std::shared_ptr<paimon::ScoredGlobalIndexResult>> VisitVectorSearch(
            const std::shared_ptr<paimon::VectorSearch>& search) override {
        _last_operation = "vector_search";
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

    const std::string& last_operation() const { return _last_operation; }
    const std::shared_ptr<paimon::VectorSearch>& last_vector_search() const { return _last_vector_search; }
    void set_vector_result(std::shared_ptr<paimon::ScoredGlobalIndexResult> result) {
        _vector_result = std::move(result);
    }
    void fail_with(paimon::Status status) { _status = std::move(status); }
    void set_return_null(bool return_null) { _return_null = return_null; }

private:
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> record(std::string operation) {
        _last_operation = std::move(operation);
        if (!_status.ok()) {
            return _status;
        }
        if (_return_null) {
            return std::shared_ptr<paimon::GlobalIndexResult>();
        }
        return _result;
    }

    std::shared_ptr<paimon::GlobalIndexResult> _result;
    std::shared_ptr<paimon::ScoredGlobalIndexResult> _vector_result;
    std::shared_ptr<paimon::VectorSearch> _last_vector_search;
    std::string _last_operation;
    paimon::Status _status;
    bool _return_null = false;
};

std::shared_ptr<paimon::GlobalIndexResult> result_from_ranges(std::vector<paimon::Range> ranges) {
    return paimon::BitmapGlobalIndexResult::FromRanges(ranges);
}

std::shared_ptr<paimon::ScoredGlobalIndexResult> scored_result(std::vector<int64_t> row_ids,
                                                               std::vector<float> scores) {
    return std::make_shared<paimon::BitmapScoredGlobalIndexResult>(paimon::RoaringBitmap64::From(row_ids),
                                                                   std::move(scores));
}

rapidjson::Document parse_json(const char* json) {
    rapidjson::Document document;
    document.Parse(json);
    EXPECT_FALSE(document.HasParseError());
    return document;
}

} // namespace

TEST(PaimonGlobalIndexEvaluatorTest, EvaluatesTypedBinaryPredicate) {
    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(7, 9)}));
    PaimonGlobalIndexEvaluator evaluator(
            [reader](std::string_view column) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
                EXPECT_EQ("k", column);
                return reader;
            });
    rapidjson::Document predicate =
            parse_json(R"({"o":"b","b":"GE","c":[{"o":"cr","t":"int","n":"k"},{"o":"co","t":"int","v":7}]})");

    auto result = evaluator.evaluate(predicate);

    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ("greater_or_equal", reader->last_operation());
    auto ranges = result.value()->ToRanges();
    ASSERT_TRUE(ranges.ok()) << ranges.status().ToString();
    ASSERT_EQ(1, ranges.value().size());
    EXPECT_EQ(7, ranges.value()[0].from);
    EXPECT_EQ(9, ranges.value()[0].to);
}

TEST(PaimonGlobalIndexEvaluatorTest, EvaluatesAllBinaryOperations) {
    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    PaimonGlobalIndexEvaluator evaluator(
            [reader](std::string_view) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> { return reader; });
    const std::vector<std::pair<std::string, std::string>> operations = {
            {"EQ", "equal"},         {"NE", "not_equal"},    {"LT", "less_than"},
            {"LE", "less_or_equal"}, {"GT", "greater_than"}, {"GE", "greater_or_equal"}};

    for (const auto& [binary_type, expected_operation] : operations) {
        rapidjson::Document predicate = parse_json(
                fmt::format(R"({{"o":"b","b":"{}","c":[{{"o":"cr","t":"int","n":"k"}},{{"o":"co","t":"int","v":7}}]}})",
                            binary_type)
                        .c_str());
        ASSERT_TRUE(evaluator.evaluate(predicate).ok()) << binary_type;
        EXPECT_EQ(expected_operation, reader->last_operation());
    }
}

TEST(PaimonGlobalIndexEvaluatorTest, ConvertsSupportedLiteralTypes) {
    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    PaimonGlobalIndexEvaluator evaluator(
            [reader](std::string_view) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> { return reader; });
    const std::vector<std::string> constants = {R"({"o":"co","t":"boolean","v":true})",
                                                R"({"o":"co","t":"tinyint","v":1})",
                                                R"({"o":"co","t":"smallint","v":2})",
                                                R"({"o":"co","t":"int","v":3})",
                                                R"({"o":"co","t":"bigint","v":4})",
                                                R"({"o":"co","t":"float","v":1.5})",
                                                R"({"o":"co","t":"double","v":2.5})",
                                                R"({"o":"co","t":"string","v":"s"})",
                                                R"json({"o":"co","t":"varchar(12)","v":"v"})json",
                                                R"json({"o":"co","t":"char(3)","v":"c"})json"};

    for (const std::string& constant : constants) {
        rapidjson::Document predicate = parse_json(
                fmt::format(R"({{"o":"b","b":"EQ","c":[{{"o":"cr","t":"int","n":"k"}},{}]}})", constant).c_str());
        ASSERT_TRUE(evaluator.evaluate(predicate).ok()) << constant;
        EXPECT_EQ("equal", reader->last_operation());
    }
}

TEST(PaimonGlobalIndexEvaluatorTest, IntersectsCompoundPredicateResults) {
    auto left = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 3)}));
    auto right = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(3, 5)}));
    PaimonGlobalIndexEvaluator evaluator(
            [left, right](std::string_view column) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
                return column == "left" ? left : right;
            });
    rapidjson::Document predicate = parse_json(
            R"({"o":"cp","ct":"AND","c":[{"o":"b","b":"EQ","c":[{"o":"cr","t":"int","n":"left"},{"o":"co","t":"int","v":1}]},{"o":"b","b":"EQ","c":[{"o":"cr","t":"int","n":"right"},{"o":"co","t":"int","v":1}]}]})");

    auto result = evaluator.evaluate(predicate);

    ASSERT_TRUE(result.ok()) << result.status();
    auto ranges = result.value()->ToRanges();
    ASSERT_TRUE(ranges.ok()) << ranges.status().ToString();
    ASSERT_EQ(1, ranges.value().size());
    EXPECT_EQ(3, ranges.value()[0].from);
    EXPECT_EQ(3, ranges.value()[0].to);
}

TEST(PaimonGlobalIndexEvaluatorTest, UnionsCompoundPredicateResults) {
    auto left = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    auto right = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(4, 5)}));
    PaimonGlobalIndexEvaluator evaluator(
            [left, right](std::string_view column) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
                return column == "left" ? left : right;
            });
    rapidjson::Document predicate = parse_json(
            R"({"o":"cp","ct":"OR","c":[{"o":"b","b":"EQ","c":[{"o":"cr","t":"int","n":"left"},{"o":"co","t":"int","v":1}]},{"o":"b","b":"EQ","c":[{"o":"cr","t":"int","n":"right"},{"o":"co","t":"int","v":1}]}]})");

    auto result = evaluator.evaluate(predicate);

    ASSERT_TRUE(result.ok()) << result.status();
    auto ranges = result.value()->ToRanges();
    ASSERT_TRUE(ranges.ok()) << ranges.status().ToString();
    ASSERT_EQ(2, ranges.value().size());
    EXPECT_EQ(1, ranges.value()[0].from);
    EXPECT_EQ(5, ranges.value()[1].to);
}

TEST(PaimonGlobalIndexEvaluatorTest, EvaluatesScalarPredicateFamilies) {
    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    PaimonGlobalIndexEvaluator evaluator(
            [reader](std::string_view column) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
                EXPECT_EQ("k", column);
                return reader;
            });

    rapidjson::Document in = parse_json(
            R"({"o":"ip","ng":true,"c":[{"o":"cr","t":"int","n":"k"},{"o":"co","t":"int","v":1},{"o":"co","t":"int","v":2}]})");
    ASSERT_TRUE(evaluator.evaluate(in).ok());
    EXPECT_EQ("not_in", reader->last_operation());

    rapidjson::Document positive_in =
            parse_json(R"({"o":"ip","ng":false,"c":[{"o":"cr","t":"int","n":"k"},{"o":"co","t":"int","v":1}]})");
    ASSERT_TRUE(evaluator.evaluate(positive_in).ok());
    EXPECT_EQ("in", reader->last_operation());

    rapidjson::Document is_null = parse_json(R"({"o":"isn","ng":true,"c":[{"o":"cr","t":"varchar","n":"k"}]})");
    ASSERT_TRUE(evaluator.evaluate(is_null).ok());
    EXPECT_EQ("is_not_null", reader->last_operation());

    rapidjson::Document is_null_positive =
            parse_json(R"({"o":"isn","ng":false,"c":[{"o":"cr","t":"varchar","n":"k"}]})");
    ASSERT_TRUE(evaluator.evaluate(is_null_positive).ok());
    EXPECT_EQ("is_null", reader->last_operation());

    rapidjson::Document starts_with = parse_json(
            R"({"o":"ca","f":"STARTS_WITH","a":[{"o":"cr","t":"varchar","n":"k"},{"o":"co","t":"varchar","v":"prefix"}]})");
    ASSERT_TRUE(evaluator.evaluate(starts_with).ok());
    EXPECT_EQ("starts_with", reader->last_operation());
}

TEST(PaimonGlobalIndexEvaluatorTest, EvaluatesVectorTopNScoreFunctions) {
    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    reader->set_vector_result(scored_result({3, 7}, {0.8f, 0.9f}));
    PaimonGlobalIndexEvaluator evaluator(
            [reader](std::string_view column) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
                EXPECT_EQ("embedding", column);
                return reader;
            });

    struct TestCase {
        const char* function;
        paimon::VectorSearch::DistanceType distance_type;
    };
    const std::vector<TestCase> test_cases = {
            {"approx_l2_distance", paimon::VectorSearch::DistanceType::EUCLIDEAN},
            {"approx_inner_product", paimon::VectorSearch::DistanceType::INNER_PRODUCT},
            {"approx_cosine_similarity", paimon::VectorSearch::DistanceType::COSINE},
    };
    for (const TestCase& test_case : test_cases) {
        std::string json =
                std::string(R"({"o":"ca","f":")") + test_case.function +
                R"(","a":[{"o":"cr","t":"array<float>","n":"embedding"},{"o":"a","i":"float","c":[{"o":"co","t":"float","v":1.25},{"o":"co","t":"float","v":2.5}]}]})";
        rapidjson::Document expression = parse_json(json.c_str());

        auto result = evaluator.evaluate_top_n(expression, 12);

        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("vector_search", reader->last_operation());
        ASSERT_NE(nullptr, reader->last_vector_search());
        EXPECT_EQ("embedding", reader->last_vector_search()->field_name);
        EXPECT_EQ(12, reader->last_vector_search()->limit);
        EXPECT_EQ((std::vector<float>{1.25f, 2.5f}), reader->last_vector_search()->query);
        EXPECT_FALSE(static_cast<bool>(reader->last_vector_search()->pre_filter));
        EXPECT_EQ(nullptr, reader->last_vector_search()->predicate);
        ASSERT_TRUE(reader->last_vector_search()->distance_type.has_value());
        EXPECT_EQ(test_case.distance_type, reader->last_vector_search()->distance_type.value());
        EXPECT_TRUE(reader->last_vector_search()->options.empty());
        EXPECT_NE(nullptr, std::dynamic_pointer_cast<paimon::ScoredGlobalIndexResult>(result.value()));
    }
}

TEST(PaimonGlobalIndexEvaluatorTest, EvaluatesVectorTopNWithLiteralFirst) {
    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    reader->set_vector_result(scored_result({3}, {0.8f}));
    PaimonGlobalIndexEvaluator evaluator(
            [reader](std::string_view column) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
                EXPECT_EQ("embedding", column);
                return reader;
            });
    rapidjson::Document expression = parse_json(
            R"({"o":"ca","f":"approx_inner_product","a":[{"o":"a","i":"float","c":[{"o":"co","t":"float","v":1.25}]},{"o":"cr","t":"array<float>","n":"embedding"}]})");

    auto result = evaluator.evaluate_top_n(expression, 3);

    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_NE(nullptr, reader->last_vector_search());
    EXPECT_EQ("embedding", reader->last_vector_search()->field_name);
    EXPECT_EQ((std::vector<float>{1.25f}), reader->last_vector_search()->query);
}

TEST(PaimonGlobalIndexEvaluatorTest, RejectsMalformedVectorTopN) {
    PaimonGlobalIndexEvaluator evaluator([](std::string_view) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
        return Status::InternalError("reader should not be requested");
    });
    rapidjson::Document empty_vector = parse_json(
            R"({"o":"ca","f":"approx_l2_distance","a":[{"o":"cr","t":"array<float>","n":"embedding"},{"o":"a","i":"float","c":[]}]})");
    rapidjson::Document unsupported = parse_json(
            R"({"o":"ca","f":"unsupported","a":[{"o":"cr","t":"array<float>","n":"embedding"},{"o":"a","i":"float","c":[{"o":"co","t":"float","v":1}]}]})");
    rapidjson::Document integral_vector = parse_json(
            R"({"o":"ca","f":"approx_l2_distance","a":[{"o":"cr","t":"array<float>","n":"embedding"},{"o":"a","i":"int","c":[{"o":"co","t":"int","v":1}]}]})");

    auto empty_result = evaluator.evaluate_top_n(empty_vector, 10);
    auto unsupported_result = evaluator.evaluate_top_n(unsupported, 10);
    auto integral_result = evaluator.evaluate_top_n(integral_vector, 10);
    auto invalid_limit_result = evaluator.evaluate_top_n(unsupported, 0);

    ASSERT_FALSE(empty_result.ok());
    EXPECT_TRUE(empty_result.status().is_invalid_argument());
    ASSERT_FALSE(unsupported_result.ok());
    EXPECT_TRUE(unsupported_result.status().is_invalid_argument());
    ASSERT_FALSE(integral_result.ok());
    EXPECT_TRUE(integral_result.status().is_invalid_argument());
    ASSERT_FALSE(invalid_limit_result.ok());
    EXPECT_TRUE(invalid_limit_result.status().is_invalid_argument());
}

TEST(PaimonGlobalIndexEvaluatorTest, RejectsMalformedPredicate) {
    PaimonGlobalIndexEvaluator evaluator([](std::string_view) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
        return Status::InternalError("reader should not be requested");
    });
    rapidjson::Document predicate = parse_json(R"({"o":"unknown"})");

    auto result = evaluator.evaluate(predicate);

    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
}

TEST(PaimonGlobalIndexEvaluatorTest, RejectsMalformedPredicateFamilies) {
    PaimonGlobalIndexEvaluator evaluator([](std::string_view) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
        return Status::InternalError("reader should not be requested");
    });
    const std::vector<std::string> malformed = {R"({})",
                                                R"({"o":1})",
                                                R"({"o":"b","b":"EQ","c":[]})",
                                                R"({"o":"cp","ct":"AND","c":[]})",
                                                R"({"o":"ip","ng":false,"c":[]})",
                                                R"({"o":"isn","ng":false,"c":[]})",
                                                R"({"o":"ca","f":"starts_with","a":[]})"};

    for (const std::string& json : malformed) {
        rapidjson::Document predicate = parse_json(json.c_str());
        auto result = evaluator.evaluate(predicate);
        ASSERT_FALSE(result.ok()) << json;
        EXPECT_TRUE(result.status().is_invalid_argument()) << result.status();
    }
}

TEST(PaimonGlobalIndexEvaluatorTest, RejectsInvalidColumnsLiteralsAndOperations) {
    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    PaimonGlobalIndexEvaluator evaluator(
            [reader](std::string_view) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> { return reader; });
    const std::vector<std::string> invalid = {
            R"({"o":"b","b":"EQ","c":[{"o":"co","t":"int","v":1},{"o":"co","t":"int","v":2}]})",
            R"({"o":"b","b":"EQ","c":[{"o":"cr","n":"k"},{"o":"co","t":"date","v":"2026-01-01"}]})",
            R"({"o":"b","b":"EQ","c":[{"o":"cr","n":"k"},{"o":"co","t":"boolean","v":1}]})",
            R"({"o":"b","b":"BETWEEN","c":[{"o":"cr","n":"k"},{"o":"co","t":"int","v":1}]})",
            R"({"o":"cp","ct":"XOR","c":[{"o":"isn","ng":false,"c":[{"o":"cr","n":"k"}]},{"o":"isn","ng":false,"c":[{"o":"cr","n":"k"}]}]})",
            R"({"o":"ca","f":"ends_with","a":[{"o":"cr","n":"k"},{"o":"co","t":"string","v":"x"}]})"};

    for (const std::string& json : invalid) {
        rapidjson::Document predicate = parse_json(json.c_str());
        auto result = evaluator.evaluate(predicate);
        ASSERT_FALSE(result.ok()) << json;
        EXPECT_TRUE(result.status().is_invalid_argument()) << result.status();
    }
}

TEST(PaimonGlobalIndexEvaluatorTest, PropagatesReaderLookupAndEvaluationFailures) {
    rapidjson::Document predicate =
            parse_json(R"({"o":"b","b":"EQ","c":[{"o":"cr","n":"k"},{"o":"co","t":"int","v":1}]})");
    PaimonGlobalIndexEvaluator missing_reader(
            [](std::string_view) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
                return Status::NotFound("missing reader");
            });
    auto missing = missing_reader.evaluate(predicate);
    ASSERT_FALSE(missing.ok());
    EXPECT_TRUE(missing.status().is_not_found());

    auto reader = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({paimon::Range(1, 2)}));
    PaimonGlobalIndexEvaluator evaluator(
            [reader](std::string_view) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> { return reader; });
    reader->fail_with(paimon::Status::IOError("broken index"));
    auto failed = evaluator.evaluate(predicate);
    ASSERT_FALSE(failed.ok());
    EXPECT_TRUE(failed.status().is_internal_error());
    EXPECT_NE(std::string::npos, failed.status().message().find("broken index"));

    reader->fail_with(paimon::Status::OK());
    reader->set_return_null(true);
    auto unsupported = evaluator.evaluate(predicate);
    ASSERT_FALSE(unsupported.ok());
    EXPECT_TRUE(unsupported.status().is_not_supported());
}

} // namespace starrocks
