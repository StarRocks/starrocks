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

#include "platform/llm/ai_rate_limiter.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <new>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "base/testutil/sync_point.h"
#include "base/utility/scoped_cleanup.h"

namespace starrocks {

class AIRateLimitKeyTestPeer {
public:
    static const std::array<uint8_t, AIRateLimitKey::kCredentialDigestBytes>& credential_digest(
            const AIRateLimitKey& key) {
        return key._credential_digest;
    }
};

class AIRateLimiterTestPeer {
public:
    static bool contains(const AIRateLimiter& limiter, const AIRateLimitKey& key) {
        return limiter._contains_for_test(key);
    }
    static double tokens(const AIRateLimiter& limiter, const AIRateLimitKey& key) {
        return limiter._tokens_for_test(key);
    }
    static int64_t outstanding_reservations(const AIRateLimiter& limiter, const AIRateLimitKey& key) {
        return limiter._outstanding_reservations_for_test(key);
    }
    static int64_t pins(const AIRateLimiter& limiter, const AIRateLimitKey& key) { return limiter._pins_for_test(key); }
};

namespace {

constexpr int64_t kSecond = 1'000'000'000;
constexpr int64_t kMinute = 60 * kSecond;
constexpr int64_t kHour = 60 * kMinute;

class ManualAIClock final : public AIClock {
public:
    int64_t monotonic_now_ns() const noexcept override { return _monotonic_ns; }
    int64_t unix_now_seconds() const noexcept override { return _unix_seconds; }

    void advance_ns(int64_t delta_ns) {
        _monotonic_ns += delta_ns;
        _unix_seconds += delta_ns / kSecond;
    }

private:
    int64_t _monotonic_ns = kSecond;
    int64_t _unix_seconds = 1'700'000'000;
};

class MutableAIRateLimitSource final : public AIRateLimitSource {
public:
    int64_t qps(AICapability capability) const noexcept override {
        EXPECT_EQ(AICapability::CHAT, capability);
        return chat_qps;
    }

    int64_t chat_qps = 2;
};

class AIRateLimiterTest : public ::testing::Test {
protected:
    AIRateLimiterTest() : _limiter(&_clock, &_limits) {}

    AIRateLimitKey key(std::string endpoint = "https://model.invalid/v1/chat", std::string credential = "key-a") {
        return AIRateLimitKey::create(std::move(endpoint), credential, AICapability::CHAT);
    }

    bool commit_one(const AIRateLimitKey& bucket_key, int64_t* eligible_at_ns = nullptr) {
        std::optional<AITokenReservation> reservation = _limiter.try_reserve(bucket_key, eligible_at_ns);
        if (!reservation.has_value()) {
            return false;
        }
        reservation->commit();
        return true;
    }

    ManualAIClock _clock;
    MutableAIRateLimitSource _limits;
    AIRateLimiter _limiter;
};

TEST_F(AIRateLimiterTest, UsesFullOpaqueCredentialDigestAndSharesOnlyTheExactBucket) {
    static_assert(AIRateLimitKey::kCredentialDigestBytes == 32);
    static_assert(std::is_nothrow_move_constructible_v<AITokenReservation>);
    static_assert(std::is_nothrow_move_assignable_v<AITokenReservation>);
    static_assert(std::is_nothrow_destructible_v<AITokenReservation>);
    static_assert(noexcept(std::declval<AITokenReservation&>().commit()));

    const std::string raw_key = "raw-key-sentinel";
    const auto shared_a = key("https://model.invalid/v1/chat?tenant=secret", raw_key);
    const auto shared_b = key("https://model.invalid/v1/chat?tenant=secret", raw_key);
    const auto other_credential = key("https://model.invalid/v1/chat?tenant=secret", "other-key");
    const auto other_endpoint = key("https://other.invalid/v1/chat", raw_key);

    ASSERT_TRUE(commit_one(shared_a));
    ASSERT_TRUE(commit_one(shared_b));
    EXPECT_FALSE(commit_one(shared_a));
    EXPECT_TRUE(commit_one(other_credential));
    EXPECT_TRUE(commit_one(other_endpoint));
}

TEST_F(AIRateLimiterTest, StoresTheKnownSha256CredentialDigestAsOpaqueBinary) {
    const auto bucket_key = key("https://model.invalid/v1/chat", "abc");
    const std::array<uint8_t, AIRateLimitKey::kCredentialDigestBytes> expected = {
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
            0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
    };

    EXPECT_EQ(expected, AIRateLimitKeyTestPeer::credential_digest(bucket_key));
}

TEST_F(AIRateLimiterTest, StartsWithQpsTokensAndRefillsOnlyToTwoSecondCapacity) {
    const auto bucket_key = key();
    int64_t eligible_at_ns = 0;

    EXPECT_TRUE(commit_one(bucket_key));
    EXPECT_TRUE(commit_one(bucket_key));
    EXPECT_FALSE(commit_one(bucket_key, &eligible_at_ns));
    EXPECT_EQ(_clock.monotonic_now_ns() + kSecond / 2, eligible_at_ns);

    _clock.advance_ns(kSecond / 2);
    EXPECT_TRUE(commit_one(bucket_key));
    EXPECT_FALSE(commit_one(bucket_key));

    _clock.advance_ns(10 * kSecond);
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(commit_one(bucket_key)) << "burst token " << i;
    }
    EXPECT_FALSE(commit_one(bucket_key));
}

TEST_F(AIRateLimiterTest, RefundsAnUncommittedReservationButChargesEveryCommittedAttempt) {
    _limits.chat_qps = 1;
    const auto bucket_key = key();

    {
        auto unsent = _limiter.try_reserve(bucket_key);
        ASSERT_TRUE(unsent.has_value());
    }
    EXPECT_TRUE(commit_one(bucket_key)) << "a synchronous fire failure must refund its tentative token";
    EXPECT_FALSE(commit_one(bucket_key));

    _clock.advance_ns(kSecond);
    EXPECT_TRUE(commit_one(bucket_key)) << "the retry is a new charged network attempt";
    EXPECT_FALSE(commit_one(bucket_key));
}

TEST_F(AIRateLimiterTest, ReservationKeyAllocationFailureDoesNotConsumeTokenOrOutstandingCapacity) {
    _limits.chat_qps = 1;
    const auto bucket_key = key();

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    bool throw_once = true;
    sync_point->SetCallBack("AIRateLimiter::try_reserve:before_reservation_key_copy", [&throw_once](void*) {
        if (std::exchange(throw_once, false)) {
            throw std::bad_alloc();
        }
    });

    EXPECT_THROW(_limiter.try_reserve(bucket_key), std::bad_alloc);
    sync_point->ClearCallBack("AIRateLimiter::try_reserve:before_reservation_key_copy");

    EXPECT_DOUBLE_EQ(1.0, AIRateLimiterTestPeer::tokens(_limiter, bucket_key));
    EXPECT_EQ(0, AIRateLimiterTestPeer::outstanding_reservations(_limiter, bucket_key));
    std::optional<AITokenReservation> reservation = _limiter.try_reserve(bucket_key);
    ASSERT_TRUE(reservation.has_value())
            << "a failed reservation object allocation must leave the initial token and outstanding slot untouched";
    reservation.reset();
    EXPECT_TRUE(_limiter.try_reserve(bucket_key).has_value())
            << "rolling back the next reservation must restore the full pre-failure state";
}

TEST_F(AIRateLimiterTest, AppliesMutableQpsOnTheNextAcquisitionAndClampsStoredTokens) {
    _limits.chat_qps = 8;
    const auto bucket_key = key();
    ASSERT_TRUE(commit_one(bucket_key));

    _limits.chat_qps = 2;
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(commit_one(bucket_key)) << "clamped token " << i;
    }
    EXPECT_FALSE(commit_one(bucket_key));

    _limits.chat_qps = 8;
    EXPECT_FALSE(commit_one(bucket_key)) << "raising QPS must not mint tokens retroactively";
    _clock.advance_ns(kSecond / 8);
    EXPECT_TRUE(commit_one(bucket_key));
}

TEST_F(AIRateLimiterTest, LowerQpsWaitIncludesTheTimeForCommittedDebtToReleaseCapacity) {
    _limits.chat_qps = 4;
    const auto bucket_key = key();
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(commit_one(bucket_key));
    }

    _limits.chat_qps = 1;
    int64_t eligible_at_ns = 0;
    EXPECT_FALSE(commit_one(bucket_key, &eligible_at_ns));
    EXPECT_EQ(_clock.monotonic_now_ns() + 3 * kSecond, eligible_at_ns)
            << "the wakeup must satisfy both token refill and the lowered bucket capacity";
}

TEST_F(AIRateLimiterTest, PendingReservationsRequireAnExternalEventAcrossQpsReduction) {
    _limits.chat_qps = 4;
    const auto bucket_key = key();
    std::array<std::optional<AITokenReservation>, 4> pending;
    for (auto& reservation : pending) {
        reservation = _limiter.try_reserve(bucket_key);
        ASSERT_TRUE(reservation.has_value());
    }

    _limits.chat_qps = 1;
    int64_t eligible_at_ns = 0;
    EXPECT_FALSE(_limiter.try_reserve(bucket_key, &eligible_at_ns).has_value());
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), eligible_at_ns)
            << "pending reservations do not decay with time and require a commit or rollback event";
}

TEST_F(AIRateLimiterTest, SharesOneThrottleCooldownWithoutBlockingOtherBuckets) {
    _limits.chat_qps = 8;
    const auto throttled = key();
    const auto independent = key("https://other.invalid/v1/chat", "key-b");
    const auto invalid = key("https://invalid-cooldown.invalid/v1/chat", "invalid-key");
    const int64_t cooldown_until = _clock.monotonic_now_ns() + kSecond;
    _limiter.extend_cooldown(invalid, std::numeric_limits<int64_t>::max());
    EXPECT_TRUE(commit_one(invalid)) << "the rate limiter must reject the saturated rate-wait sentinel as a cooldown";
    _limiter.extend_cooldown(invalid, _clock.monotonic_now_ns());
    EXPECT_TRUE(commit_one(invalid)) << "an already-expired cooldown must not block a bucket";
    _limiter.extend_cooldown(throttled, cooldown_until);

    int64_t eligible_at_ns = 0;
    EXPECT_FALSE(commit_one(throttled, &eligible_at_ns));
    EXPECT_EQ(cooldown_until, eligible_at_ns);
    EXPECT_TRUE(commit_one(independent));

    _clock.advance_ns(kSecond);
    EXPECT_TRUE(commit_one(throttled)) << "the shared cooldown must not be applied a second time";
}

TEST_F(AIRateLimiterTest, SweepsAtMostEveryTenMinutesAndEvictsOnlyAfterSixIdleHours) {
    const auto old_key = key();
    ASSERT_TRUE(commit_one(old_key));
    EXPECT_TRUE(AIRateLimiterTestPeer::contains(_limiter, old_key));

    _clock.advance_ns(6 * kHour - kSecond);
    ASSERT_TRUE(commit_one(key("https://sweep-one.invalid/v1/chat", "sweep-one")));
    EXPECT_TRUE(AIRateLimiterTestPeer::contains(_limiter, old_key));

    _clock.advance_ns(kSecond + 1);
    ASSERT_TRUE(commit_one(key("https://too-soon.invalid/v1/chat", "too-soon")));
    EXPECT_TRUE(AIRateLimiterTestPeer::contains(_limiter, old_key))
            << "a sweep less than ten minutes later is suppressed";

    _clock.advance_ns(10 * kMinute);
    ASSERT_TRUE(commit_one(key("https://sweep-two.invalid/v1/chat", "sweep-two")));
    EXPECT_FALSE(AIRateLimiterTestPeer::contains(_limiter, old_key));
}

TEST_F(AIRateLimiterTest, OutstandingTentativeReservationPreventsIdleEviction) {
    _limits.chat_qps = 1;
    const auto active_key = key();
    std::optional<AITokenReservation> active = _limiter.try_reserve(active_key);
    ASSERT_TRUE(active.has_value());

    _clock.advance_ns(6 * kHour + kSecond);
    ASSERT_TRUE(commit_one(key("https://sweep-one.invalid/v1/chat", "sweep-one")));
    EXPECT_TRUE(AIRateLimiterTestPeer::contains(_limiter, active_key));

    active.reset();
    _clock.advance_ns(10 * kMinute);
    ASSERT_TRUE(commit_one(key("https://sweep-two.invalid/v1/chat", "sweep-two")));
    EXPECT_FALSE(AIRateLimiterTestPeer::contains(_limiter, active_key));
}

TEST_F(AIRateLimiterTest, CommittedReservationPinsBucketUntilItsOwnerReleasesIt) {
    _limits.chat_qps = 1;
    const auto active_key = key();
    std::optional<AITokenReservation> active = _limiter.try_reserve(active_key);
    ASSERT_TRUE(active.has_value());
    EXPECT_EQ(1, AIRateLimiterTestPeer::outstanding_reservations(_limiter, active_key));
    EXPECT_EQ(1, AIRateLimiterTestPeer::pins(_limiter, active_key));

    active->commit();
    EXPECT_EQ(0, AIRateLimiterTestPeer::outstanding_reservations(_limiter, active_key));
    EXPECT_EQ(1, AIRateLimiterTestPeer::pins(_limiter, active_key));

    _clock.advance_ns(6 * kHour + kSecond);
    ASSERT_TRUE(commit_one(key("https://sweep-one.invalid/v1/chat", "sweep-one")));
    EXPECT_TRUE(AIRateLimiterTestPeer::contains(_limiter, active_key));

    active.reset();
    EXPECT_EQ(0, AIRateLimiterTestPeer::pins(_limiter, active_key));
    _clock.advance_ns(10 * kMinute);
    ASSERT_TRUE(commit_one(key("https://sweep-two.invalid/v1/chat", "sweep-two")));
    EXPECT_FALSE(AIRateLimiterTestPeer::contains(_limiter, active_key));
}

TEST_F(AIRateLimiterTest, UnexpiredSharedCooldownPreventsIdleEviction) {
    const auto cooling_key = key();
    _limiter.extend_cooldown(cooling_key, _clock.monotonic_now_ns() + 7 * kHour);

    _clock.advance_ns(6 * kHour + 10 * kMinute);
    ASSERT_TRUE(commit_one(key("https://sweep-one.invalid/v1/chat", "sweep-one")));
    EXPECT_TRUE(AIRateLimiterTestPeer::contains(_limiter, cooling_key));

    _clock.advance_ns(50 * kMinute);
    ASSERT_TRUE(commit_one(key("https://sweep-two.invalid/v1/chat", "sweep-two")));
    EXPECT_FALSE(AIRateLimiterTestPeer::contains(_limiter, cooling_key));
}

TEST_F(AIRateLimiterTest, LatePendingRollbackCannotEraseCommittedDebtFromOtherAttempts) {
    _limits.chat_qps = 2;
    const auto bucket_key = key();

    std::optional<AITokenReservation> pending_a = _limiter.try_reserve(bucket_key);
    ASSERT_TRUE(pending_a.has_value());
    ASSERT_TRUE(commit_one(bucket_key));

    _clock.advance_ns(kSecond);
    ASSERT_TRUE(commit_one(bucket_key));
    pending_a.reset();

    _limits.chat_qps = 1;
    EXPECT_TRUE(commit_one(bucket_key));
    EXPECT_FALSE(commit_one(bucket_key))
            << "rolling back A must not erase C's committed debt and reopen a second token after the QPS clamp";
}

} // namespace
} // namespace starrocks
