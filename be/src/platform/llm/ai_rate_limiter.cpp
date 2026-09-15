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

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <mutex>
#include <unordered_map>

#include "base/crypto/sha.h"
#include "base/hash/hash_util.hpp"
#include "base/testutil/sync_point.h"
#include "common/logging.h"

namespace starrocks {
namespace {

constexpr int64_t kSecondNs = 1'000'000'000;
constexpr int64_t kSweepIntervalNs = 10 * 60 * kSecondNs;
constexpr int64_t kIdleExpirationNs = 6 * 60 * 60 * kSecondNs;

int64_t saturating_add(int64_t lhs, int64_t rhs) {
    if (rhs > 0 && lhs > std::numeric_limits<int64_t>::max() - rhs) {
        return std::numeric_limits<int64_t>::max();
    }
    if (rhs < 0 && lhs < std::numeric_limits<int64_t>::min() - rhs) {
        return std::numeric_limits<int64_t>::min();
    }
    return lhs + rhs;
}

int64_t wait_ns_for_earned_tokens(double required_tokens, int64_t qps) {
    DCHECK_GT(qps, 0);
    const long double wait_ns = std::ceil(static_cast<long double>(std::max(0.0, required_tokens)) * kSecondNs /
                                          static_cast<long double>(qps));
    if (wait_ns >= static_cast<long double>(std::numeric_limits<int64_t>::max())) {
        return std::numeric_limits<int64_t>::max();
    }
    return static_cast<int64_t>(wait_ns);
}

} // namespace

struct AIRateLimitBucket {
    double tokens = 0;
    double committed_debt = 0;
    int64_t qps = 0;
    int64_t last_refill_ns = 0;
    int64_t last_access_ns = 0;
    int64_t cooldown_until_ns = 0;
    int64_t outstanding_reservations = 0;
    int64_t pins = 0;
};

class AIRateLimiterCore {
public:
    AIRateLimiterCore(const AIClock* clock, const AIRateLimitSource* limits)
            : clock(clock), limits(limits), last_sweep_ns(clock->monotonic_now_ns()) {}

    const AIClock* clock;
    const AIRateLimitSource* limits;
    mutable std::mutex mutex;
    std::unordered_map<AIRateLimitKey, AIRateLimitBucket, AIRateLimitKeyHash> buckets;
    int64_t last_sweep_ns;

    static double capacity(const AIRateLimitBucket& bucket) noexcept {
        return std::max(0.0, 2.0 * static_cast<double>(bucket.qps) - bucket.committed_debt -
                                     static_cast<double>(bucket.outstanding_reservations));
    }

    void settle(AIRateLimitBucket* bucket, const AIRateLimitKey& key, int64_t now_ns) noexcept {
        const int64_t old_qps = bucket->qps;
        const int64_t elapsed_ns = std::max<int64_t>(0, now_ns - bucket->last_refill_ns);
        if (elapsed_ns > 0 && old_qps > 0) {
            const double earned = static_cast<double>(elapsed_ns) * static_cast<double>(old_qps) / kSecondNs;
            bucket->tokens += earned;
            bucket->committed_debt = std::max(0.0, bucket->committed_debt - earned);
        }
        bucket->qps = std::max<int64_t>(0, limits->qps(key.capability()));
        if (old_qps == 0 && bucket->qps > 0 && bucket->committed_debt == 0 && bucket->outstanding_reservations == 0) {
            bucket->tokens = std::max(bucket->tokens, static_cast<double>(bucket->qps));
        }
        bucket->tokens = std::min(bucket->tokens, capacity(*bucket));
        bucket->last_refill_ns = now_ns;
    }

    void rollback(const AIRateLimitKey& key, bool refill) noexcept {
        std::lock_guard lock(mutex);
        auto it = buckets.find(key);
        DCHECK(it != buckets.end());
        if (it == buckets.end()) {
            return;
        }
        auto& bucket = it->second;
        if (refill) {
            settle(&bucket, key, clock->monotonic_now_ns());
        }
        DCHECK_GT(bucket.outstanding_reservations, 0);
        DCHECK_GT(bucket.pins, 0);
        --bucket.outstanding_reservations;
        --bucket.pins;
        bucket.tokens = std::min(bucket.tokens + 1.0, capacity(bucket));
    }

    void commit(const AIRateLimitKey& key, bool refill) noexcept {
        std::lock_guard lock(mutex);
        auto it = buckets.find(key);
        DCHECK(it != buckets.end());
        if (it != buckets.end()) {
            auto& bucket = it->second;
            const int64_t now_ns = refill ? clock->monotonic_now_ns() : bucket.last_refill_ns;
            if (refill) {
                settle(&bucket, key, now_ns);
            }
            DCHECK_GT(bucket.outstanding_reservations, 0);
            DCHECK_GT(bucket.pins, 0);
            --bucket.outstanding_reservations;
            bucket.committed_debt += 1.0;
            bucket.tokens = std::min(bucket.tokens, capacity(bucket));
            if (refill) {
                bucket.last_access_ns = now_ns;
            }
        }
    }

    void extend_pinned_cooldown(const AIRateLimitKey& key, int64_t eligible_at_ns) noexcept {
        if (eligible_at_ns == std::numeric_limits<int64_t>::max()) {
            return;
        }
        std::lock_guard lock(mutex);
        if (eligible_at_ns <= clock->monotonic_now_ns()) {
            return;
        }
        auto it = buckets.find(key);
        DCHECK(it != buckets.end());
        if (it == buckets.end()) {
            return;
        }
        DCHECK_GT(it->second.pins, 0);
        it->second.cooldown_until_ns = std::max(it->second.cooldown_until_ns, eligible_at_ns);
    }

    void release_pin(const AIRateLimitKey& key) noexcept {
        std::lock_guard lock(mutex);
        auto it = buckets.find(key);
        DCHECK(it != buckets.end());
        if (it == buckets.end()) {
            return;
        }
        DCHECK_GT(it->second.pins, 0);
        --it->second.pins;
    }
};

AIRateLimitKey AIRateLimitKey::create(std::string endpoint, std::string_view credential, AICapability capability) {
    AIRateLimitKey key;
    key._endpoint = std::move(endpoint);
    key._capability = capability;
    SHA256Digest digest;
    digest.update(credential.data(), credential.size());
    digest.digest();
    std::memcpy(key._credential_digest.data(), digest.binary(), key._credential_digest.size());
    return key;
}

size_t AIRateLimitKeyHash::operator()(const AIRateLimitKey& key) const noexcept {
    size_t seed = HashUtil::hash(key._endpoint.data(), key._endpoint.size(), 0);
    seed = HashUtil::hash(key._credential_digest.data(), key._credential_digest.size(), seed);
    return HashUtil::hash(&key._capability, sizeof(key._capability), seed);
}

AITokenReservation::AITokenReservation(std::shared_ptr<AIRateLimiterCore> core, AIRateLimitKey key) noexcept
        : _core(std::move(core)), _key(std::move(key)) {}

AITokenReservation::~AITokenReservation() noexcept {
    _reset();
}

AITokenReservation::AITokenReservation(AITokenReservation&& other) noexcept
        : _core(std::move(other._core)), _key(std::move(other._key)), _committed(other._committed) {
    other._key.reset();
}

AITokenReservation& AITokenReservation::operator=(AITokenReservation&& other) noexcept {
    if (this != &other) {
        _reset();
        _core = std::move(other._core);
        _key = std::move(other._key);
        _committed = other._committed;
        other._key.reset();
    }
    return *this;
}

void AITokenReservation::commit() noexcept {
    if (_core != nullptr && _key.has_value() && !_committed) {
        _core->commit(*_key, true);
        _committed = true;
    }
}

void AITokenReservation::_commit_without_refill() noexcept {
    if (_core != nullptr && _key.has_value() && !_committed) {
        _core->commit(*_key, false);
        _committed = true;
    }
}

void AITokenReservation::_rollback_without_refill() noexcept {
    if (_core != nullptr && _key.has_value() && !_committed) {
        _core->rollback(*_key, false);
    }
    _key.reset();
    _core.reset();
}

void AITokenReservation::_extend_cooldown(int64_t eligible_at_ns) noexcept {
    if (_core != nullptr && _key.has_value() && _committed) {
        _core->extend_pinned_cooldown(*_key, eligible_at_ns);
    }
}

void AITokenReservation::_reset() noexcept {
    if (_core != nullptr && _key.has_value() && !_committed) {
        _core->rollback(*_key, true);
    } else if (_core != nullptr && _key.has_value()) {
        _core->release_pin(*_key);
    }
    _key.reset();
    _core.reset();
}

AIRateLimiter::AIRateLimiter(const AIClock* clock, const AIRateLimitSource* limits)
        : _core(std::make_shared<AIRateLimiterCore>(clock, limits)) {}

std::optional<AITokenReservation> AIRateLimiter::try_reserve(const AIRateLimitKey& key, int64_t* eligible_at_ns) {
    const int64_t now_ns = _core->clock->monotonic_now_ns();
    std::lock_guard lock(_core->mutex);

    if (now_ns - _core->last_sweep_ns >= kSweepIntervalNs) {
        for (auto it = _core->buckets.begin(); it != _core->buckets.end();) {
            const auto& bucket = it->second;
            if (now_ns - bucket.last_access_ns > kIdleExpirationNs && bucket.outstanding_reservations == 0 &&
                bucket.pins == 0 && bucket.cooldown_until_ns <= now_ns) {
                it = _core->buckets.erase(it);
            } else {
                ++it;
            }
        }
        _core->last_sweep_ns = now_ns;
    }

    auto [it, inserted] = _core->buckets.try_emplace(key);
    auto& bucket = it->second;
    if (inserted) {
        bucket.qps = std::max<int64_t>(0, _core->limits->qps(key.capability()));
        bucket.tokens = static_cast<double>(bucket.qps);
        bucket.last_refill_ns = now_ns;
        bucket.last_access_ns = now_ns;
    } else {
        _core->settle(&bucket, key, now_ns);
        bucket.last_access_ns = now_ns;
    }

    if (bucket.cooldown_until_ns > now_ns) {
        if (eligible_at_ns != nullptr) {
            *eligible_at_ns = bucket.cooldown_until_ns;
        }
        return std::nullopt;
    }
    if (bucket.qps <= 0 || bucket.tokens + 1e-12 < 1.0) {
        if (eligible_at_ns != nullptr) {
            if (bucket.qps <= 0) {
                *eligible_at_ns = std::numeric_limits<int64_t>::max();
            } else {
                const double qps = static_cast<double>(bucket.qps);
                const double pending = static_cast<double>(bucket.outstanding_reservations);
                if (2.0 * qps - pending + 1e-12 < 1.0) {
                    *eligible_at_ns = std::numeric_limits<int64_t>::max();
                } else {
                    const double token_refill = 1.0 - bucket.tokens;
                    const double capacity_refill = bucket.committed_debt + pending + 1.0 - 2.0 * qps;
                    const int64_t wait_ns =
                            wait_ns_for_earned_tokens(std::max(token_refill, capacity_refill), bucket.qps);
                    *eligible_at_ns = saturating_add(now_ns, wait_ns);
                }
            }
        }
        return std::nullopt;
    }

    TEST_SYNC_POINT("AIRateLimiter::try_reserve:before_reservation_key_copy");
    AIRateLimitKey reservation_key = key;
    bucket.tokens -= 1.0;
    ++bucket.outstanding_reservations;
    ++bucket.pins;
    return AITokenReservation(_core, std::move(reservation_key));
}

void AIRateLimiter::extend_cooldown(const AIRateLimitKey& key, int64_t eligible_at_ns) {
    if (eligible_at_ns == std::numeric_limits<int64_t>::max()) {
        return;
    }
    std::lock_guard lock(_core->mutex);
    const int64_t now_ns = _core->clock->monotonic_now_ns();
    if (eligible_at_ns <= now_ns) {
        return;
    }
    auto [it, inserted] = _core->buckets.try_emplace(key);
    auto& bucket = it->second;
    if (inserted) {
        bucket.qps = std::max<int64_t>(0, _core->limits->qps(key.capability()));
        bucket.tokens = static_cast<double>(bucket.qps);
        bucket.last_refill_ns = now_ns;
    }
    bucket.last_access_ns = now_ns;
    bucket.cooldown_until_ns = std::max(bucket.cooldown_until_ns, eligible_at_ns);
}

bool AIRateLimiter::_contains_for_test(const AIRateLimitKey& key) const {
    std::lock_guard lock(_core->mutex);
    return _core->buckets.contains(key);
}

double AIRateLimiter::_tokens_for_test(const AIRateLimitKey& key) const {
    std::lock_guard lock(_core->mutex);
    auto it = _core->buckets.find(key);
    return it == _core->buckets.end() ? 0 : it->second.tokens;
}

int64_t AIRateLimiter::_outstanding_reservations_for_test(const AIRateLimitKey& key) const {
    std::lock_guard lock(_core->mutex);
    auto it = _core->buckets.find(key);
    return it == _core->buckets.end() ? 0 : it->second.outstanding_reservations;
}

int64_t AIRateLimiter::_pins_for_test(const AIRateLimitKey& key) const {
    std::lock_guard lock(_core->mutex);
    auto it = _core->buckets.find(key);
    return it == _core->buckets.end() ? 0 : it->second.pins;
}

} // namespace starrocks
