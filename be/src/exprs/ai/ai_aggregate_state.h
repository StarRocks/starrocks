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

#include <cstddef>
#include <cstdint>

#include "base/statusor.h"
#include "base/string/slice.h"

namespace starrocks {

inline constexpr size_t kAiAggregateStateMaxBytes = 1024 * 1024;
inline constexpr size_t kAiAggregateStateHeaderBytes = 24;
inline constexpr size_t kAiAggregateRecordLengthBytes = sizeof(uint64_t);

// A cursor only exists for a completely validated state. The owner must retain the encoded
// bytes unchanged for the lifetime of the view and its cursors. Copies permit bounded lookahead.
class AiAggregateStateCursor {
public:
    AiAggregateStateCursor() = default;
    bool next(Slice* record);

private:
    friend class AiAggregateStateView;
    AiAggregateStateCursor(Slice records, uint64_t count) : _records(records), _remaining(count) {}
    Slice _records;
    uint64_t _remaining = 0;
};

class AiAggregateStateView {
public:
    uint64_t count() const { return _count; }
    Slice payload() const { return _payload; }
    AiAggregateStateCursor cursor() const { return AiAggregateStateCursor(_payload, _count); }

private:
    friend StatusOr<AiAggregateStateView> parse_ai_aggregate_state(Slice, size_t);
    AiAggregateStateView(uint64_t count, Slice payload) : _count(count), _payload(payload) {}
    uint64_t _count;
    Slice _payload;
};

// Validates the entire message, including exact consumption and the encoded size ceiling,
// before exposing any records. The optional ceiling permits testing the same limit at small sizes.
StatusOr<AiAggregateStateView> parse_ai_aggregate_state(Slice encoded,
                                                        size_t max_encoded_bytes = kAiAggregateStateMaxBytes);

// dst must have kAiAggregateStateHeaderBytes available. Only the native collector writes
// this envelope; it checks count and payload growth before mutating its owned record stream.
void encode_ai_aggregate_state_header(uint8_t* dst, uint64_t count, uint64_t payload_bytes);

} // namespace starrocks
