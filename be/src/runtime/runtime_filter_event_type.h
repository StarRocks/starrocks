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

#include <string>

namespace starrocks {

enum EventType {
    RECEIVE_TOTAL_RF = 0,
    CLOSE_QUERY = 1,
    OPEN_QUERY = 2,
    RECEIVE_PART_RF = 3,
    SEND_PART_RF = 4,
    SEND_BROADCAST_GRF = 5,
    SEND_SKEW_JOIN_BROADCAST_RF = 6,
    RECEIVE_SKEW_JOIN_BROADCAST_RF = 7,
    MAX_COUNT
};

inline std::string EventTypeToString(EventType type) {
    switch (type) {
    case RECEIVE_TOTAL_RF:
        return "RECEIVE_TOTAL_RF";
    case CLOSE_QUERY:
        return "CLOSE_QUERY";
    case OPEN_QUERY:
        return "OPEN_QUERY";
    case RECEIVE_PART_RF:
        return "RECEIVE_PART_RF";
    case SEND_SKEW_JOIN_BROADCAST_RF:
        return "SEND_SKEW_JOIN_BROADCAST_RF";
    case SEND_PART_RF:
        return "SEND_PART_RF";
    case SEND_BROADCAST_GRF:
        return "SEND_BROADCAST_GRF";
    case RECEIVE_SKEW_JOIN_BROADCAST_RF:
        return "RECEIVE_SKEW_JOIN_BROADCAST_RF";
    default:
        break;
    }
    __builtin_unreachable();
}

} // namespace starrocks
