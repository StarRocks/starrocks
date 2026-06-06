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

#include "base/concurrency/spinlock.h"
#include "gutil/macros.h"

namespace starrocks::pipeline {

// SplitMorselTicketChecker counts EOS chunks produced by split morsels from the same original ScanMorsel.
class SplitMorselTicketChecker {
    // We use int64_t(Ticket::data) to bookkeep the SplitMorsels.
    // |--all_ready_bit(1bit)--|--not_used(1bit)--|--leave_count(30bit)--|--enter_count(30bit)--|
    static constexpr int64_t ALL_READY_BIT = 1L << 63;
    static constexpr int64_t ENTER_COUNT_BITS = (1L << 30) - 1;
    static constexpr int LEAVE_COUNT_SHIFT = 30;
    static constexpr int64_t LEAVE_COUNT_BITS = ((1L << 30) - 1) << LEAVE_COUNT_SHIFT;

    struct Ticket {
        Ticket(int64_t id, int64_t data) : id(id), data(data) {}
        int64_t id;
        int64_t data;
    };

public:
    SplitMorselTicketChecker() = default;
    ~SplitMorselTicketChecker() = default;

    void enter(int64_t id, bool is_last);
    bool leave(int64_t id);
    bool are_all_ready(int64_t id);
    bool are_all_left(int64_t id);
    void more_tickets(int64_t id);

private:
    DISALLOW_COPY_AND_MOVE(SplitMorselTicketChecker);
    SpinLock _lock;
    std::unordered_map<int64_t, Ticket> _tickets;
};

using SplitMorselTicketCheckerRawPtr = SplitMorselTicketChecker*;
using SplitMorselTicketCheckerPtr = std::shared_ptr<SplitMorselTicketChecker>;
using SplitMorselTicketId = int64_t;

} // namespace starrocks::pipeline
