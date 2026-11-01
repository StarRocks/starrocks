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

#include "exprs/ai/ai_aggregate_state.h"

#include "base/coding.h"

namespace starrocks {
namespace {
constexpr uint32_t kMagic = 0x54434941; // "AICT"
constexpr uint16_t kVersion = 1;
constexpr uint16_t kFlags = 0;
} // namespace

void encode_ai_aggregate_state_header(uint8_t* dst, uint64_t count, uint64_t payload_bytes) {
    encode_fixed32_le(dst, kMagic);
    encode_fixed16_le(dst + 4, kVersion);
    encode_fixed16_le(dst + 6, kFlags);
    encode_fixed64_le(dst + 8, count);
    encode_fixed64_le(dst + 16, payload_bytes);
}

StatusOr<AiAggregateStateView> parse_ai_aggregate_state(Slice encoded, size_t max_encoded_bytes) {
    if (encoded.size > max_encoded_bytes) {
        return Status::InvalidArgument("AI aggregate state exceeds the encoded byte limit");
    }
    if (encoded.size < kAiAggregateStateHeaderBytes) {
        return Status::InvalidArgument("Truncated AI aggregate state header");
    }
    const auto* header = reinterpret_cast<const uint8_t*>(encoded.data);
    if (decode_fixed32_le(header) != kMagic) {
        return Status::InvalidArgument("Invalid AI aggregate state magic");
    }
    if (decode_fixed16_le(header + 4) != kVersion || decode_fixed16_le(header + 6) != kFlags) {
        return Status::InvalidArgument("Unsupported AI aggregate state version or flags");
    }
    const uint64_t count = decode_fixed64_le(header + 8);
    const uint64_t payload_bytes = decode_fixed64_le(header + 16);
    if (payload_bytes != encoded.size - kAiAggregateStateHeaderBytes) {
        return Status::InvalidArgument("Invalid AI aggregate state payload length");
    }
    if (count > payload_bytes / kAiAggregateRecordLengthBytes) {
        return Status::InvalidArgument("Invalid AI aggregate state record count");
    }
    Slice payload(encoded.data + kAiAggregateStateHeaderBytes, payload_bytes);
    Slice remaining = payload;
    for (uint64_t i = 0; i < count; ++i) {
        if (remaining.size < kAiAggregateRecordLengthBytes) {
            return Status::InvalidArgument("Truncated AI aggregate record length");
        }
        uint64_t length = decode_fixed64_le(reinterpret_cast<const uint8_t*>(remaining.data));
        remaining.remove_prefix(kAiAggregateRecordLengthBytes);
        if (length > remaining.size) {
            return Status::InvalidArgument("Truncated AI aggregate record");
        }
        remaining.remove_prefix(length);
    }
    if (!remaining.empty()) {
        return Status::InvalidArgument("Trailing bytes in AI aggregate state");
    }
    return AiAggregateStateView(count, payload);
}

bool AiAggregateStateCursor::next(Slice* record) {
    if (_remaining == 0) return false;
    uint64_t length = decode_fixed64_le(reinterpret_cast<const uint8_t*>(_records.data));
    _records.remove_prefix(kAiAggregateRecordLengthBytes);
    *record = Slice(_records.data, length);
    _records.remove_prefix(length);
    --_remaining;
    return true;
}

} // namespace starrocks
