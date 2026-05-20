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

#include "exec/query_cache/ticket_checker.h"

namespace starrocks::pipeline {

class TicketedMorselQueue {
public:
    virtual ~TicketedMorselQueue() = default;

    virtual void set_ticket_checker(const query_cache::TicketCheckerPtr& ticket_checker) = 0;
    virtual bool could_attch_ticket_checker() const = 0;
};

} // namespace starrocks::pipeline
