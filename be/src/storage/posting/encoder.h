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

#include <memory>
#include <vector>

#include "common/statusor.h"
#include "roaring/roaring.hh"

namespace starrocks {

enum class EncodingType {
    UNKNOWN = 0,
    VARINT = 1, // Variable-length integer encoding
};

/**
 * Abstract base class for all integer encoders
 * Provides a common interface for encoding and decoding unsigned 32-bit integers
 */
class Encoder {
public:
    virtual ~Encoder() = default;

    /**
     * Encode a roaring bitmap whose range should be limited to signed integer
     *
     * @param roaring signed integer ranged roaring bitmap
     * @return encoded byte array
     */
    virtual StatusOr<std::vector<uint8_t>> encode(const roaring::Roaring& roaring) = 0;

    /**
     * Decode a byte array back to unsigned 32-bit integers
     * @param data The encoded byte array
     * @return Decoded values
     */
    virtual StatusOr<roaring::Roaring> decode(const std::vector<uint8_t>& data) = 0;

    /**
     * Get the encoding type of this encoder
     * @return The encoding type
     */
    virtual EncodingType getType() const = 0;

    /**
     * Get a human-readable name for this encoder
     * @return The encoder name
     */
    virtual const char* getName() const = 0;
};
/**
 * Factory class for creating encoder instances
 */
class EncoderFactory {
public:
    /**
     * Create an encoder instance based on encoding type
     * @param type The encoding type
     * @return Shared pointer to the encoder
     */
    static std::shared_ptr<Encoder> createEncoder(EncodingType type);
};

} // namespace starrocks