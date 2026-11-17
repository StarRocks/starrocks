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

#include "platform/llm/ai_rerank.h"

#include <cmath>
#include <limits>
#include <string_view>
#include <unordered_set>

namespace starrocks {
namespace {

Status invalid_top_n() {
    return Status::InvalidArgument("AI rerank top_n must be a positive exact INT");
}

// AIProviderOptions::create has already validated a complete JSON NUMBER and owns immutable bytes.
// Parse the decimal spelling, not a rounded double: 5.0000000000000000001 is not an integer.
StatusOr<int32_t> parse_top_n(std::string_view number) {
    const auto start = number.find_first_not_of(" \t\r\n");
    if (start == std::string_view::npos) return invalid_top_n();
    number.remove_prefix(start);
    number = number.substr(0, number.find_last_not_of(" \t\r\n") + 1);
    if (number.empty() || number[0] == '-' || number.size() > std::numeric_limits<int32_t>::max()) {
        return invalid_top_n();
    }
    std::string digits;
    int64_t fractional_digits = 0;
    bool fraction = false;
    size_t position = 0;
    for (; position < number.size() && number[position] != 'e' && number[position] != 'E'; ++position) {
        if (number[position] == '.') {
            fraction = true;
        } else {
            digits.push_back(number[position]);
            fractional_digits += fraction;
        }
    }
    int64_t exponent = 0;
    if (position < number.size()) {
        ++position;
        bool negative = number[position] == '-';
        if (number[position] == '-' || number[position] == '+') ++position;
        for (; position < number.size(); ++position) {
            // No INT-sized coefficient can compensate for this exponent.
            if (exponent > std::numeric_limits<int32_t>::max()) return invalid_top_n();
            exponent = exponent * 10 + number[position] - '0';
        }
        if (negative) exponent = -exponent;
    }
    auto nonzero = digits.find_first_not_of('0');
    if (nonzero == std::string::npos) return invalid_top_n();
    digits.erase(0, nonzero);
    int64_t scale = exponent - fractional_digits;
    while (digits.back() == '0') {
        digits.pop_back();
        ++scale;
    }
    if (scale < 0 || scale > 10 || digits.size() + scale > 10) return invalid_top_n();
    int64_t result = 0;
    for (char digit : digits) result = result * 10 + digit - '0';
    while (scale-- > 0) result *= 10;
    if (result <= 0 || result > std::numeric_limits<int32_t>::max()) return invalid_top_n();
    return static_cast<int32_t>(result);
}

} // namespace

StatusOr<std::optional<int32_t>> parse_ai_rerank_top_n(const AIProviderOptions& options) {
    for (const auto& option : options.members()) {
        if (option.key != "top_n") continue;
        if (option.kind != AIProviderOptionKind::NUMBER) return invalid_top_n();
        auto parsed = parse_top_n(option.serialized_json);
        if (!parsed.ok()) return parsed.status();
        return std::optional<int32_t>(*parsed);
    }
    return std::optional<int32_t>{};
}

Status validate_ai_rerank_options(const AIProviderOptions& options) {
    for (const auto& option : options.members()) {
        if (option.key == "model" || option.key == "input" || option.key == "query" || option.key == "documents" ||
            option.key == "parameters") {
            return Status::InvalidArgument("AI rerank option overrides a request-owned field");
        }
    }
    return parse_ai_rerank_top_n(options).status();
}

Status validate_ai_rerank_hits(const AIRerankResult& hits, const AIRerankResponseShape& shape) {
    if (shape.candidate_count < 0 || shape.expected_hits < 0 || shape.expected_hits > shape.candidate_count ||
        hits.size() != static_cast<size_t>(shape.expected_hits)) {
        return Status::InvalidArgument("AI rerank response cardinality is invalid");
    }
    std::unordered_set<int32_t> seen;
    seen.reserve(hits.size());
    double previous = std::numeric_limits<double>::infinity();
    for (const auto& hit : hits) {
        if (hit.index < 0 || hit.index >= shape.candidate_count || !seen.insert(hit.index).second ||
            !std::isfinite(hit.relevance_score) || hit.relevance_score > previous) {
            return Status::InvalidArgument("AI rerank response ranking is invalid");
        }
        previous = hit.relevance_score;
    }
    return Status::OK();
}

} // namespace starrocks
