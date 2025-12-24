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

#include "encoder.h"

namespace starrocks {

/**
 * VarInt (Variable-length integer) encoder
 * Uses 7 bits for data and 1 bit as continuation flag
 * Efficient for small integer values
 */
class VarIntEncoder final : public Encoder {
public:
    VarIntEncoder();
    ~VarIntEncoder() override;

    Status encode(uint32_t value, std::vector<uint8_t>* result) override;
    Status encode(const roaring::Roaring& roaring, std::vector<uint8_t>* result) override;
    Status decode(const std::vector<uint8_t>& data, roaring::Roaring* result) override;
    EncodingType getType() const override { return EncodingType::VARINT; }
    const char* getName() const override { return "VarInt"; }

    // Helper methods for encoding/decoding single values
    static void encodeValue(uint32_t value, std::vector<uint8_t>* output);
    static uint32_t decodeValue(const std::vector<uint8_t>& data, size_t& offset);
};

} // namespace starrocks
