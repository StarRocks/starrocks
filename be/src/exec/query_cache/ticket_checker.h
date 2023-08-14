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

#include <glog/logging.h>

#include <memory>
#include <unordered_map>

#include "gutil/macros.h"
#include "util/spinlock.h"

namespace starrocks::query_cache {

// TicketChecker is used to count down EOS chunk of the SplitMorsels generated from the same
// original ScanMorsel.
// when we split a SplitMorsel from the ScanMorsel, we invoke TicketChecker::enter to increment
// enter_count by 1. if the SplitMorsel is the last one, we should turn on all_ready_bit of
// the TicketChecker. when ScanOperator receives an EOS chunk, we invoke TicketChecker::leave to
// increment leave_count by 1 then check if the all_ready_bit is on and enter_count is equivalent
// to the leave_count, if that is true, we know ScanOperator receives all of the EOS chunks of
// SplitMorsels, that means the original ScanMorsel has been processed completely and a EOS chunk
// can emit from ScanOperator to its successor MultilaneOperator.

class TicketChecker;
using TicketCheckerRawPtr = TicketChecker*;
using TicketCheckerPtr = std::shared_ptr<TicketChecker>;
using TicketIdType = int64_t;

class TicketChecker {
    // We use int64_t(Ticket::data) to bookkeeping the forth and back the SplitMorsels.
    // |--all_ready_bit(1bit)--|--not_used(1bit)--|--leave_count(30bit)--|--enter_count(30bit)--|
    static constexpr int64_t ALL_READY_BIT = 1L << 63;
    static constexpr int64_t ENTER_COUNT_BITS = (1L << 30) - 1;
    static constexpr int LEAVE_COUNT_SHIFT = 30;
    static constexpr int64_t LEAVE_COUNT_BITS = ((1L << 30) - 1) << LEAVE_COUNT_SHIFT;

    struct Ticket {
        Ticket(TicketIdType id, int64_t data) : id(id), data(data) {}
        TicketIdType id;
        int64_t data;
    };

public:
    TicketChecker() = default;
    ~TicketChecker() = default;
    // inc1 enter_count, turn all_ready_bit if the SplitMorsel is the last one, the last morsel
    // is always EOSMorsel.
    void enter(TicketIdType id, bool is_last);
    // inc1 leave_count, return true if enter_count == leave_count and all_read_bit is on.
    bool leave(TicketIdType id);
    // test if all_ready_bit is on, returning true means that morsel splitting is done.
    bool are_all_ready(TicketIdType id);

private:
    DISALLOW_COPY_AND_MOVE(TicketChecker);
    SpinLock _lock;
    std::unordered_map<int64_t, Ticket> _tickets;
};
} // namespace starrocks::query_cache
