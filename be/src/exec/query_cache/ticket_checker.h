// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once

#include <glog/logging.h>

#include <memory>
#include <unordered_map>

#include "gutil/macros.h"
namespace starrocks::query_cache {

class TicketChecker;
using TicketCheckerRawPtr = TicketChecker*;
using TicketCheckerPtr = std::shared_ptr<TicketChecker>;
using TicketIdType = int64_t;
class TicketChecker {
    static constexpr int64_t ALL_READY_BIT = 1L << 63;
    static constexpr int64_t ENTER_COUNT_BITS = (1L << 30) - 1;
    static constexpr int LEAVE_COUNT_SHIFT = 30;
    static constexpr int64_t LEAVE_COUNT_BITS = (1L << 30) << LEAVE_COUNT_SHIFT;

    struct Ticket {
        Ticket(TicketIdType id, int64_t data) : id(id), data(data) {}
        TicketIdType id;
        int64_t data;
    };

public:
    TicketChecker() = default;
    ~TicketChecker() = default;
    void enter(TicketIdType id, bool is_last);
    bool leave(TicketIdType id);
    bool are_all_ready(TicketIdType id);

private:
    DISALLOW_COPY_AND_MOVE(TicketChecker);
    std::unordered_map<int64_t, Ticket> _tickets;
};
} // namespace starrocks::query_cache
