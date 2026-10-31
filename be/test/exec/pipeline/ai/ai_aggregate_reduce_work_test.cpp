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

#include "exec/pipeline/ai/ai_aggregate_reduce_work.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "column/binary_column.h"
#include "exprs/ai/ai_aggregate_state.h"

namespace starrocks::pipeline {

namespace {
ColumnPtr encoded_records(const std::vector<std::string>& records) {
    size_t bytes = 0;
    for (const auto& record : records) bytes += 8 + record.size();
    std::string encoded(kAiAggregateStateHeaderBytes + bytes, '\0');
    encode_ai_aggregate_state_header(reinterpret_cast<uint8_t*>(encoded.data()), records.size(), bytes);
    size_t offset = kAiAggregateStateHeaderBytes;
    for (const auto& record : records) {
        for (size_t i = 0; i < 8; ++i)
            encoded[offset++] = static_cast<char>((uint64_t(record.size()) >> (8 * i)) & 0xff);
        encoded.replace(offset, record.size(), record);
        offset += record.size();
    }
    auto column = BinaryColumn::create();
    column->append(Slice(encoded));
    return column;
}
AITaskSuccess success(std::string text) {
    return AITaskSuccess::create(std::move(text), {}).value();
}
bool has_format(const std::shared_ptr<const AIProviderOptions>& options) {
    if (!options) return false;
    for (const auto& option : options->members())
        if (option.key == "response_format") return true;
    return false;
}
std::shared_ptr<const AIProviderOptions> formatted_options() {
    return std::make_shared<const AIProviderOptions>(
            AIProviderOptions::create(
                    {{.key = "response_format",
                      .serialized_json = "{\"type\":\"json_object\"}",
                      .kind = AIProviderOptionKind::OBJECT},
                     {.key = "temperature", .serialized_json = "0", .kind = AIProviderOptionKind::NUMBER}})
                    .value());
}
} // namespace

TEST(AIAggregateReduceWorkTest, NullStateCompletesWithoutARequest) {
    AIAggregateReduceInputBatch batch;
    batch.instruction = "summarize";
    auto work = AIAggregateReduceWork::create(batch, 0);
    ASSERT_TRUE(work.ok()) << work.status();
    EXPECT_EQ(AIWorkProgress::COMPLETE, work.value()->progress());
    EXPECT_FALSE(work.value()->next_request(65536).value().has_value());
    EXPECT_FALSE(work.value()->take_result().has_value());
}

TEST(AIAggregateReduceWorkTest, EmptyEncodedStateAndSingleFinalPreserveOptionsAndDuplicates) {
    AIAggregateReduceInputBatch empty{.states = encoded_records({}), .instruction = "summarize"};
    auto empty_work = AIAggregateReduceWork::create(empty, 0);
    ASSERT_TRUE(empty_work.ok());
    EXPECT_EQ(AIWorkProgress::COMPLETE, empty_work.value()->progress());
    AIAggregateReduceInputBatch batch{.states = encoded_records({"a", "a", "", std::string("x\0y", 3)}),
                                      .instruction = "summarize",
                                      .model = "model",
                                      .options = formatted_options()};
    auto made = AIAggregateReduceWork::create(batch, 0);
    ASSERT_TRUE(made.ok()) << made.status();
    auto& work = *made.value();
    auto request = work.next_request(65536);
    ASSERT_TRUE(request.ok());
    ASSERT_TRUE(request->has_value());
    EXPECT_TRUE(has_format(request->value().options));
    EXPECT_NE(std::string::npos, request->value().prompt.find("[\"a\",\"a\",\"\",\"x\\u0000y\"]"));
    EXPECT_EQ(AIWorkProgress::WAITING, work.progress());
    ASSERT_TRUE(work.consume_success(request->value().ordinal, success("final")).ok());
    EXPECT_EQ(AIWorkProgress::COMPLETE, work.progress());
    EXPECT_EQ("final", work.take_result()->content());
}

TEST(AIAggregateReduceWorkTest, MultipleLevelsPreserveLogicalOrderAndOnlyFinalFormat) {
    std::vector<std::string> expected_inputs;
    for (size_t i = 0; i < 20; ++i) expected_inputs.push_back("record-" + std::to_string(i) + std::string(100, 'x'));
    expected_inputs.insert(expected_inputs.end(),
                           {expected_inputs.front(), "", std::string("x\0y", 3), "quoted\"\ntext"});
    AIAggregateReduceInputBatch batch{.states = encoded_records(expected_inputs),
                                      .instruction = "summarize",
                                      .model = "model",
                                      .options = formatted_options()};
    AIAggregateReducePolicy policy{.max_prompt_bytes = 320, .max_summary_bytes = 64};
    auto made = AIAggregateReduceWork::create(batch, 0, policy);
    ASSERT_TRUE(made.ok());
    auto& work = *made.value();
    size_t rounds = 0;
    size_t requests = 0;
    while (work.progress() != AIWorkProgress::COMPLETE && rounds < 8) {
        std::vector<AIWorkRequest> wave;
        while (work.progress() == AIWorkProgress::RUNNABLE) {
            auto next = work.next_request(65536);
            ASSERT_TRUE(next.ok()) << next.status();
            ASSERT_TRUE(next->has_value());
            wave.push_back(std::move(**next));
            EXPECT_LE(wave.back().prompt.size(), policy.max_prompt_bytes);
        }
        ASSERT_FALSE(wave.empty());
        // Compare complete decoded records in submission order. This catches
        // loss, duplication and reordering both for raw text and for summaries
        // whose callbacks deliberately complete in reverse ordinal order.
        std::vector<std::string> actual_inputs;
        for (const auto& request : wave) {
            const size_t begin = request.prompt.find("\n[");
            const size_t end = request.prompt.rfind("]\n");
            ASSERT_NE(std::string::npos, begin);
            ASSERT_NE(std::string::npos, end);
            rapidjson::Document records;
            records.Parse(request.prompt.data() + begin + 1, end - begin);
            ASSERT_FALSE(records.HasParseError());
            ASSERT_TRUE(records.IsArray());
            for (const auto& record : records.GetArray()) {
                ASSERT_TRUE(record.IsString());
                actual_inputs.emplace_back(record.GetString(), record.GetStringLength());
            }
        }
        EXPECT_EQ(expected_inputs, actual_inputs);
        const bool final = has_format(wave.front().options);
        if (final) {
            ASSERT_EQ(1, wave.size());
        }
        expected_inputs.clear();
        for (const auto& request : wave)
            expected_inputs.push_back("summary-" + std::to_string(request.ordinal) + std::string(20, 's'));
        for (auto it = wave.rbegin(); it != wave.rend(); ++it) {
            EXPECT_EQ(final, has_format(it->options));
            ASSERT_NE(nullptr, it->options);
            const auto& members = it->options->members();
            EXPECT_EQ(final ? 2 : 1, members.size());
            EXPECT_EQ("temperature", members.back().key);
            ASSERT_TRUE(work.consume_success(it->ordinal, success(final ? "final"
                                                                        : "summary-" + std::to_string(it->ordinal) +
                                                                                  std::string(20, 's')))
                                .ok());
            ++requests;
        }
        ++rounds;
    }
    EXPECT_GE(rounds, 3);
    EXPECT_EQ(AIWorkProgress::COMPLETE, work.progress());
    EXPECT_EQ(requests, work.requests());
    EXPECT_EQ("final", work.take_result()->content());
}

TEST(AIAggregateReduceWorkTest, RejectsMalformedStateOversizedRecordsAndSummaryWithoutPartialResult) {
    auto malformed = BinaryColumn::create();
    malformed->append("bad state");
    AIAggregateReduceInputBatch batch{.states = malformed, .instruction = "summarize"};
    EXPECT_FALSE(AIAggregateReduceWork::create(batch, 0).ok());
    batch.states = encoded_records({std::string(400, 'x')});
    auto made = AIAggregateReduceWork::create(batch, 0, {.max_prompt_bytes = 320});
    ASSERT_TRUE(made.ok());
    EXPECT_TRUE(made.value()->next_request(65536).status().is_invalid_argument());
    batch.states = encoded_records({std::string(110, 'x'), std::string(110, 'y'), std::string(110, 'z')});
    made = AIAggregateReduceWork::create(batch, 0, {.max_prompt_bytes = 320, .max_summary_bytes = 8});
    ASSERT_TRUE(made.ok());
    auto request = made.value()->next_request(65536);
    ASSERT_TRUE(request.ok());
    ASSERT_TRUE(request->has_value());
    EXPECT_TRUE(
            made.value()->consume_success(request->value().ordinal, success("over-eight-bytes")).is_invalid_argument());
    made.value()->finish_null();
    EXPECT_EQ(AIWorkProgress::COMPLETE, made.value()->progress());
    EXPECT_FALSE(made.value()->take_result().has_value());
}

TEST(AIAggregateReduceWorkTest, RequestAndRoundBudgetsAreHardLimits) {
    AIAggregateReduceInputBatch batch{.states = encoded_records(std::vector<std::string>(4, std::string(110, 'x'))),
                                      .instruction = "summarize"};
    auto made = AIAggregateReduceWork::create(batch, 0, {.max_prompt_bytes = 320, .max_requests = 1});
    ASSERT_TRUE(made.ok());
    ASSERT_TRUE(made.value()->next_request(65536).ok());
    EXPECT_TRUE(made.value()->next_request(65536).status().is_invalid_argument());
    made = AIAggregateReduceWork::create(batch, 0, {.max_prompt_bytes = 320, .max_rounds = 1});
    ASSERT_TRUE(made.ok());
    std::vector<uint64_t> ordinals;
    while (made.value()->progress() == AIWorkProgress::RUNNABLE) {
        auto request = made.value()->next_request(65536);
        ASSERT_TRUE(request.ok());
        ASSERT_TRUE(request->has_value());
        ordinals.push_back(request->value().ordinal);
    }
    for (auto ordinal : ordinals) ASSERT_TRUE(made.value()->consume_success(ordinal, success("s")).ok());
    EXPECT_TRUE(made.value()->next_request(65536).status().is_invalid_argument());
}

TEST(AIAggregateReduceWorkTest, OversizedInstructionAndIntermediateOptionsAreGroupBudgetFailures) {
    AIAggregateReduceInputBatch batch{.states = encoded_records({"value"}), .instruction = std::string(65537, 'i')};
    auto made = AIAggregateReduceWork::create(batch, 0);
    ASSERT_TRUE(made.ok()) << made.status();
    EXPECT_TRUE(made.value()->next_request(65536).status().is_invalid_argument());
    batch.instruction = "summarize";
    auto options = AIProviderOptions::create({{.key = "metadata",
                                               .serialized_json = "\"" + std::string(65536, 'v') + "\"",
                                               .kind = AIProviderOptionKind::STRING}});
    ASSERT_TRUE(options.ok());
    batch.options = std::make_shared<const AIProviderOptions>(std::move(options).value());
    made = AIAggregateReduceWork::create(batch, 0);
    ASSERT_TRUE(made.ok());
    auto direct_final = made.value()->next_request(65536);
    ASSERT_TRUE(direct_final.ok());
    ASSERT_TRUE(direct_final->has_value());
    EXPECT_EQ(batch.options, direct_final->value().options);
    batch.states = encoded_records({std::string(40000, 'x'), std::string(40000, 'y')});
    made = AIAggregateReduceWork::create(batch, 0);
    ASSERT_TRUE(made.ok());
    EXPECT_TRUE(made.value()->next_request(65536).status().is_invalid_argument());
    batch.states = encoded_records({});
    made = AIAggregateReduceWork::create(batch, 0);
    ASSERT_TRUE(made.ok());
    EXPECT_EQ(AIWorkProgress::COMPLETE, made.value()->progress());
    EXPECT_FALSE(made.value()->next_request(65536).value().has_value());
}

TEST(AIAggregateReduceWorkTest, NonStringChatSuccessIsAnInternalErrorNotAnIgnorableBudgetFailure) {
    AIAggregateReduceInputBatch batch{.states = encoded_records({"value"}), .instruction = "summarize"};
    auto made = AIAggregateReduceWork::create(batch, 0);
    ASSERT_TRUE(made.ok());
    auto request = made.value()->next_request(65536);
    ASSERT_TRUE(request.ok());
    ASSERT_TRUE(request->has_value());
    auto unexpected = AITaskSuccess::create(std::vector<float>{0.5f}, {});
    ASSERT_TRUE(unexpected.ok());
    const auto status = made.value()->consume_success(request->value().ordinal, std::move(unexpected).value());
    EXPECT_TRUE(status.is_internal_error()) << status;
    EXPECT_FALSE(status.is_invalid_argument());
    EXPECT_FALSE(made.value()->take_result().has_value());
}

} // namespace starrocks::pipeline
