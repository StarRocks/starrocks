// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>
#include <memory>

#include "common/statusor.h"
#include "gutil/strings/substitute.h"

namespace starrocks::pipeline {

class DriverLimiter {
public:
    struct Token {
    public:
        Token(std::atomic<int>& num_total_drivers, int num_drivers)
                : num_total_drivers(num_total_drivers), num_drivers(num_drivers) {}
        ~Token() { num_total_drivers -= num_drivers; }

        // Disable copy/move ctor and assignment.
        Token(const Token&) = delete;
        Token& operator=(const Token&) = delete;
        Token(Token&&) = delete;
        Token& operator=(Token&&) = delete;

    private:
        std::atomic<int>& num_total_drivers;
        int num_drivers;
    };
    using TokenPtr = std::unique_ptr<Token>;

    explicit DriverLimiter(int max_num_drivers) : _max_num_drivers(max_num_drivers) {}
    ~DriverLimiter() = default;

    // Return non-ok status, if it cannot acquire `num_drivers` drivers from the limiter.
    // Otherwise, return the token, the destructor of which will release the acquired
    // `num_drivers` drivers back to the limiter.
    StatusOr<TokenPtr> try_acquire(int num_drivers);

private:
    const int _max_num_drivers;
    std::atomic<int> _num_total_drivers{0};
};

} // namespace starrocks::pipeline
