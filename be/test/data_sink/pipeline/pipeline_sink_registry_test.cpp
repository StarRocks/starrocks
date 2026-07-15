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

#include "data_sink/pipeline/pipeline_sink_registry.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"

namespace starrocks::pipeline {
namespace {

PipelineSinkProvider make_provider(
        TDataSinkType::type type, SinkRole role = SinkRole::INTERMEDIATE,
        PipelineSinkBuildFn build = [](PipelineSinkBuildContext&&) { return Status::OK(); }) {
    auto it = _TDataSinkType_VALUES_TO_NAMES.find(type);
    return {
            .type = type,
            .name = it == _TDataSinkType_VALUES_TO_NAMES.end() ? "unknown" : it->second,
            .role = role,
            .build = std::move(build),
    };
}

Status add_all_providers(PipelineSinkRegistryBuilder* builder) {
    for (const auto& [value, name] : _TDataSinkType_VALUES_TO_NAMES) {
        RETURN_IF_ERROR(builder->add(make_provider(static_cast<TDataSinkType::type>(value))));
    }
    return Status::OK();
}

TEST(PipelineSinkRegistryTest, RejectsInvalidProviders) {
    PipelineSinkRegistryBuilder builder;

    auto empty_name = make_provider(TDataSinkType::RESULT_SINK);
    empty_name.name.clear();
    EXPECT_TRUE(builder.add(std::move(empty_name)).is_invalid_argument());

    auto missing_callback = make_provider(TDataSinkType::RESULT_SINK);
    missing_callback.build = {};
    EXPECT_TRUE(builder.add(std::move(missing_callback)).is_invalid_argument());

    auto unknown_type = make_provider(static_cast<TDataSinkType::type>(999));
    auto status = builder.add(std::move(unknown_type));
    EXPECT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_NE(status.message().find("999"), std::string_view::npos);
}

TEST(PipelineSinkRegistryTest, RejectsDuplicateType) {
    PipelineSinkRegistryBuilder builder;
    ASSERT_OK(builder.add(make_provider(TDataSinkType::RESULT_SINK)));

    auto duplicate = make_provider(TDataSinkType::RESULT_SINK);
    duplicate.name = "duplicate-result";
    auto status = builder.add(std::move(duplicate));
    EXPECT_TRUE(status.is_already_exist()) << status;
    EXPECT_NE(status.message().find("RESULT_SINK"), std::string_view::npos);
    EXPECT_NE(status.message().find("duplicate-result"), std::string_view::npos);
}

TEST(PipelineSinkRegistryTest, RejectsIncompleteRegistry) {
    PipelineSinkRegistryBuilder builder;
    ASSERT_OK(builder.add(make_provider(TDataSinkType::RESULT_SINK)));

    auto result = builder.freeze();
    EXPECT_TRUE(result.status().is_invalid_argument()) << result.status();
    EXPECT_NE(result.status().message().find("DATA_STREAM_SINK"), std::string_view::npos);
}

TEST(PipelineSinkRegistryTest, FreezesAndFindsProviders) {
    PipelineSinkRegistryBuilder builder;
    ASSERT_OK(add_all_providers(&builder));

    auto result = builder.freeze();
    ASSERT_OK(result.status());
    auto registry = result.value();
    EXPECT_EQ(_TDataSinkType_VALUES_TO_NAMES.size(), registry->size());

    auto provider = registry->find(TDataSinkType::RESULT_SINK);
    ASSERT_OK(provider.status());
    EXPECT_EQ(TDataSinkType::RESULT_SINK, provider.value()->type);
    EXPECT_EQ("RESULT_SINK", provider.value()->name);

    auto unknown = registry->find(static_cast<TDataSinkType::type>(999));
    EXPECT_TRUE(unknown.status().is_internal_error()) << unknown.status();
    EXPECT_NE(unknown.status().message().find("999"), std::string_view::npos);
}

TEST(PipelineSinkRegistryTest, FreezeIsIdempotentAndRejectsLateRegistration) {
    PipelineSinkRegistryBuilder builder;
    ASSERT_OK(add_all_providers(&builder));

    auto first = builder.freeze();
    auto second = builder.freeze();
    ASSERT_OK(first.status());
    ASSERT_OK(second.status());
    EXPECT_EQ(first.value().get(), second.value().get());

    auto status = builder.add(make_provider(TDataSinkType::RESULT_SINK));
    EXPECT_TRUE(status.is_internal_error()) << status;
}

TEST(PipelineSinkRegistryTest, SupportsConcurrentFrozenLookups) {
    PipelineSinkRegistryBuilder builder;
    ASSERT_OK(add_all_providers(&builder));
    auto result = builder.freeze();
    ASSERT_OK(result.status());
    auto registry = result.value();

    std::atomic<bool> failed{false};
    std::vector<std::thread> readers;
    for (int i = 0; i < 8; ++i) {
        readers.emplace_back([&] {
            for (int iteration = 0; iteration < 100; ++iteration) {
                for (const auto& [value, name] : _TDataSinkType_VALUES_TO_NAMES) {
                    auto provider = registry->find(static_cast<TDataSinkType::type>(value));
                    if (!provider.ok() || provider.value()->name != name) {
                        failed.store(true, std::memory_order_relaxed);
                    }
                }
            }
        });
    }
    for (auto& reader : readers) {
        reader.join();
    }
    EXPECT_FALSE(failed.load(std::memory_order_relaxed));
}

} // namespace
} // namespace starrocks::pipeline
