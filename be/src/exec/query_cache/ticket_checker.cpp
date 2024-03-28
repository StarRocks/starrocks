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

#include "exec/query_cache/ticket_checker.h"

#include <mutex>

namespace starrocks::query_cache {
void TicketChecker::enter(TicketIdType id, bool is_last) {
    std::lock_guard<SpinLock> require_lock(_lock);
    auto [it, _] = _tickets.try_emplace(id, id, 0);
    Ticket& ticket = it->second;
    ticket.data += 1L;
    ticket.data |= is_last ? ALL_READY_BIT : 0;
}

bool TicketChecker::leave(TicketIdType id) {
    std::lock_guard<SpinLock> require_lock(_lock);
    auto it = _tickets.find(id);
    DCHECK(it != _tickets.end());
    Ticket& ticket = it->second;
    ticket.data += (1L << LEAVE_COUNT_SHIFT);
    bool is_all_enter = (ticket.data & ALL_READY_BIT) == ALL_READY_BIT;
    int64_t enter_count = ticket.data & ENTER_COUNT_BITS;
    int64_t leave_count = (ticket.data & LEAVE_COUNT_BITS) >> LEAVE_COUNT_SHIFT;
    return is_all_enter && (enter_count == leave_count);
}

bool TicketChecker::are_all_ready(TicketIdType id) {
    std::lock_guard<SpinLock> require_lock(_lock);
    auto it = _tickets.find(id);
    DCHECK(it != _tickets.end());
    Ticket& ticket = it->second;
    return (ticket.data & ALL_READY_BIT) != 0L;
}

} // namespace starrocks::query_cache