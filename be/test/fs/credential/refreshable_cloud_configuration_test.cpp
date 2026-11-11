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

#include "fs/credential/refreshable_cloud_configuration.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <limits>

namespace starrocks {

namespace {
TCloudConfiguration make_conf(const std::string& token, int64_t refresh_after_ms = -1) {
    TCloudConfiguration conf;
    conf.__set_cloud_type(TCloudType::AWS);
    conf.__set_cloud_properties({{"aws.s3.session_token", token}});
    if (refresh_after_ms >= 0) {
        conf.__set_refresh_after_ms(refresh_after_ms);
    }
    return conf;
}

std::string token_of(const std::shared_ptr<const TCloudConfiguration>& conf) {
    return conf->cloud_properties.at("aws.s3.session_token");
}

RefreshableCloudConfiguration::Clock at(int64_t ms) {
    return [ms] { return ms; };
}

RefreshableCloudConfiguration::Fetcher must_not_fetch() {
    return [](const TCloudConfiguration&) -> StatusOr<TCloudConfiguration> {
        ADD_FAILURE() << "fetched";
        return Status::InternalError("fetched");
    };
}
} // namespace

TEST(RefreshableCloudConfigurationTest, no_configuration) {
    RefreshableCloudConfiguration holder(nullptr);
    EXPECT_EQ(nullptr, holder.get(must_not_fetch(), at(1000)));
}

TEST(RefreshableCloudConfigurationTest, not_refreshable_never_fetches) {
    auto conf = make_conf("t0");
    RefreshableCloudConfiguration holder(&conf);
    EXPECT_EQ("t0", token_of(holder.get(must_not_fetch(), at(std::numeric_limits<int64_t>::max() - 1))));
}

TEST(RefreshableCloudConfigurationTest, refresh_turned_off_never_fetches) {
    auto conf = make_conf("t0", 1000);
    RefreshableCloudConfiguration holder(&conf, false);
    EXPECT_EQ("t0", token_of(holder.get(must_not_fetch(), at(std::numeric_limits<int64_t>::max() - 1))));
}

TEST(RefreshableCloudConfigurationTest, fetches_once_due_and_switches) {
    auto conf = make_conf("t0", 1000);
    conf.__set_refresh_token("token-0");
    RefreshableCloudConfiguration holder(&conf);
    EXPECT_EQ("t0", token_of(holder.get(must_not_fetch(), at(999))));

    int fetches = 0;
    auto fetch = [&](const TCloudConfiguration& current) -> StatusOr<TCloudConfiguration> {
        EXPECT_EQ("token-0", current.refresh_token);
        ++fetches;
        return make_conf("t1", 5000);
    };
    EXPECT_EQ("t1", token_of(holder.get(fetch, at(1000))));
    EXPECT_EQ("t1", token_of(holder.get(fetch, at(4999))));
    EXPECT_EQ(1, fetches);
}

TEST(RefreshableCloudConfigurationTest, failure_keeps_current_and_backs_off) {
    auto conf = make_conf("t0", 1000);
    RefreshableCloudConfiguration holder(&conf);
    int fetches = 0;
    auto failing = [&](const TCloudConfiguration&) -> StatusOr<TCloudConfiguration> {
        ++fetches;
        return Status::InternalError("coordinator gone");
    };
    EXPECT_EQ("t0", token_of(holder.get(failing, at(1000))));
    EXPECT_EQ("t0", token_of(holder.get(failing, at(1000 + RefreshableCloudConfiguration::kRetryIntervalMs - 1))));
    EXPECT_EQ(1, fetches);
    EXPECT_EQ("t0", token_of(holder.get(failing, at(1000 + RefreshableCloudConfiguration::kRetryIntervalMs))));
    EXPECT_EQ(2, fetches);
}

// A failure that took longer than the retry interval still backs off a full interval from when it ended.
TEST(RefreshableCloudConfigurationTest, slow_failure_backs_off_from_when_it_ended) {
    auto conf = make_conf("t0", 1000);
    RefreshableCloudConfiguration holder(&conf);
    int64_t now = 1000;
    auto clock = [&now] { return now; };
    int fetches = 0;
    auto slow_failing = [&](const TCloudConfiguration&) -> StatusOr<TCloudConfiguration> {
        ++fetches;
        now += 2 * RefreshableCloudConfiguration::kRetryIntervalMs;
        return Status::TimedOut("rpc timeout");
    };
    EXPECT_EQ("t0", token_of(holder.get(slow_failing, clock)));
    const int64_t ended = now;
    now = ended + RefreshableCloudConfiguration::kRetryIntervalMs - 1;
    EXPECT_EQ("t0", token_of(holder.get(slow_failing, clock)));
    EXPECT_EQ(1, fetches);
    now = ended + RefreshableCloudConfiguration::kRetryIntervalMs;
    EXPECT_EQ("t0", token_of(holder.get(slow_failing, clock)));
    EXPECT_EQ(2, fetches);
}

TEST(RefreshableCloudConfigurationTest, renewal_already_due_backs_off) {
    auto conf = make_conf("t0", 1000);
    RefreshableCloudConfiguration holder(&conf);
    int fetches = 0;
    auto stale = [&](const TCloudConfiguration&) -> StatusOr<TCloudConfiguration> {
        ++fetches;
        return make_conf("t1", 900);
    };
    EXPECT_EQ("t1", token_of(holder.get(stale, at(1000))));
    EXPECT_EQ("t1", token_of(holder.get(stale, at(1001))));
    EXPECT_EQ(1, fetches);
}

TEST(RefreshableCloudConfigurationTest, renewal_without_refresh_time_stops_refreshing) {
    auto conf = make_conf("t0", 1000);
    RefreshableCloudConfiguration holder(&conf);
    EXPECT_EQ("t1", token_of(holder.get(
                            [](const TCloudConfiguration&) { return StatusOr<TCloudConfiguration>(make_conf("t1")); },
                            at(1000))));
    EXPECT_EQ("t1", token_of(holder.get(must_not_fetch(), at(std::numeric_limits<int64_t>::max() - 1))));
}

// Once due, other file opens wait for the fetch in flight rather than open with credentials that may be expired,
// and use its result without fetching again.
TEST(RefreshableCloudConfigurationTest, due_callers_wait_for_the_fetch_in_flight) {
    auto conf = make_conf("t0", 1000);
    RefreshableCloudConfiguration holder(&conf);
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<int> fetches = 0;
    auto blocking = [&](const TCloudConfiguration&) -> StatusOr<TCloudConfiguration> {
        ++fetches;
        entered.set_value();
        released.wait();
        return make_conf("t1", 5000);
    };
    auto fetcher = std::async(std::launch::async, [&] { return holder.get(blocking, at(1000)); });
    entered.get_future().wait();

    auto waiter = std::async(std::launch::async, [&] { return holder.get(blocking, at(1000)); });
    EXPECT_EQ(std::future_status::timeout, waiter.wait_for(std::chrono::milliseconds(100)));
    // Not due yet: no waiting at all.
    EXPECT_EQ("t0", token_of(holder.get(must_not_fetch(), at(999))));

    release.set_value();
    EXPECT_EQ("t1", token_of(fetcher.get()));
    EXPECT_EQ("t1", token_of(waiter.get()));
    EXPECT_EQ(1, fetches.load());
}

} // namespace starrocks
