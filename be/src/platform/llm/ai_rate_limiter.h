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

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace starrocks {

class AIRateLimitKeyTestPeer;
class AIRateLimiterTestPeer;
class AIAdmissionControllerCore;
struct AIRateLimitKeyHash;

enum class AICapability : uint8_t { CHAT = 0 };

class AIClock {
public:
    virtual ~AIClock() = default;

    virtual int64_t monotonic_now_ns() const noexcept = 0;
    virtual int64_t unix_now_seconds() const noexcept = 0;
};

class AIRateLimitSource {
public:
    virtual ~AIRateLimitSource() = default;

    virtual int64_t qps(AICapability capability) const noexcept = 0;
};

class AIRateLimitKey {
public:
    static constexpr size_t kCredentialDigestBytes = 32;

    AIRateLimitKey() = default;
    static AIRateLimitKey create(std::string endpoint, std::string_view credential, AICapability capability);

    AICapability capability() const { return _capability; }
    bool operator==(const AIRateLimitKey& rhs) const = default;

private:
    friend class AIRateLimitKeyTestPeer;
    friend struct AIRateLimitKeyHash;

    std::string _endpoint;
    std::array<uint8_t, kCredentialDigestBytes> _credential_digest{};
    AICapability _capability = AICapability::CHAT;
};

struct AIRateLimitKeyHash {
    size_t operator()(const AIRateLimitKey& key) const noexcept;
};

class AIRateLimiterCore;

class AITokenReservation {
public:
    AITokenReservation() = default;
    ~AITokenReservation() noexcept;

    AITokenReservation(const AITokenReservation&) = delete;
    AITokenReservation& operator=(const AITokenReservation&) = delete;
    AITokenReservation(AITokenReservation&& other) noexcept;
    AITokenReservation& operator=(AITokenReservation&& other) noexcept;

    void commit() noexcept;

private:
    friend class AIAdmissionControllerCore;
    friend class AIRateLimiter;

    AITokenReservation(std::shared_ptr<AIRateLimiterCore> core, AIRateLimitKey key) noexcept;
    void _commit_without_refill() noexcept;
    void _rollback_without_refill() noexcept;
    void _extend_cooldown(int64_t eligible_at_ns) noexcept;
    void _reset() noexcept;

    std::shared_ptr<AIRateLimiterCore> _core;
    std::optional<AIRateLimitKey> _key;
    bool _committed = false;
};

class AIRateLimiter {
public:
    AIRateLimiter(const AIClock* clock, const AIRateLimitSource* limits);

    std::optional<AITokenReservation> try_reserve(const AIRateLimitKey& key, int64_t* eligible_at_ns = nullptr);
    void extend_cooldown(const AIRateLimitKey& key, int64_t eligible_at_ns);

private:
    friend class AIAdmissionControllerCore;
    friend class AIRateLimiterTestPeer;

    bool _contains_for_test(const AIRateLimitKey& key) const;
    double _tokens_for_test(const AIRateLimitKey& key) const;
    int64_t _outstanding_reservations_for_test(const AIRateLimitKey& key) const;
    int64_t _pins_for_test(const AIRateLimitKey& key) const;

    std::shared_ptr<AIRateLimiterCore> _core;
};

} // namespace starrocks
