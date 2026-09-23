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

#include <gtest/gtest.h>
#include <paimon/global_index/bitmap_global_index_result.h>

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
            const std::shared_ptr<paimon::VectorSearch>&) override {
        return paimon::Status::Invalid("unused vector search");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitFullTextSearch(
            const std::shared_ptr<paimon::FullTextSearch>&) override {
        return paimon::Status::Invalid("unused full-text search");
    }
    bool IsThreadSafe() const override { return true; }
    std::string GetIndexType() const override { return "fake"; }

    const std::string& last_operation() const { return _last_operation; }

private:
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> record(std::string operation) {
        _last_operation = std::move(operation);
        return _result;
    }

    std::shared_ptr<paimon::GlobalIndexResult> _result;
    std::string _last_operation;
};

std::shared_ptr<paimon::GlobalIndexResult> result_from_ranges(std::vector<paimon::Range> ranges) {
    return paimon::BitmapGlobalIndexResult::FromRanges(ranges);
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

    rapidjson::Document is_null = parse_json(R"({"o":"isn","ng":true,"c":[{"o":"cr","t":"varchar","n":"k"}]})");
    ASSERT_TRUE(evaluator.evaluate(is_null).ok());
    EXPECT_EQ("is_not_null", reader->last_operation());

    rapidjson::Document starts_with = parse_json(
            R"({"o":"ca","f":"starts_with","a":[{"o":"cr","t":"varchar","n":"k"},{"o":"co","t":"varchar","v":"prefix"}]})");
    ASSERT_TRUE(evaluator.evaluate(starts_with).ok());
    EXPECT_EQ("starts_with", reader->last_operation());
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

} // namespace starrocks
