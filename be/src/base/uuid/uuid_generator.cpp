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

#include "base/uuid/uuid_generator.h"

namespace starrocks {

// Generate UUID v7 according to RFC 9562
// Format: 48-bit timestamp (ms) + 4-bit version + 12-bit random + 2-bit variant + 62-bit random
boost::uuids::uuid ThreadLocalUUIDGenerator::next_uuid_v7() {
    boost::uuids::uuid uuid;

    // Get current time in milliseconds since Unix epoch
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    // Generate random data efficiently using 64-bit values
    std::uniform_int_distribution<uint64_t> dist64;
    uint64_t rand1 = dist64(s_tls_rng);
    uint64_t rand2 = dist64(s_tls_rng);

    // Set timestamp (first 48 bits / 6 bytes)
    uuid.data[0] = static_cast<uint8_t>((ms >> 40) & 0xFF);
    uuid.data[1] = static_cast<uint8_t>((ms >> 32) & 0xFF);
    uuid.data[2] = static_cast<uint8_t>((ms >> 24) & 0xFF);
    uuid.data[3] = static_cast<uint8_t>((ms >> 16) & 0xFF);
    uuid.data[4] = static_cast<uint8_t>((ms >> 8) & 0xFF);
    uuid.data[5] = static_cast<uint8_t>(ms & 0xFF);

    // Fill random parts using the generated random values
    // Bytes 6-7: version + 12-bit random (will set version later)
    uuid.data[6] = static_cast<uint8_t>((rand1 >> 56) & 0xFF);
    uuid.data[7] = static_cast<uint8_t>((rand1 >> 48) & 0xFF);

    // Bytes 8-15: variant + 62-bit random (will set variant later)
    uuid.data[8] = static_cast<uint8_t>((rand1 >> 40) & 0xFF);
    uuid.data[9] = static_cast<uint8_t>((rand1 >> 32) & 0xFF);
    uuid.data[10] = static_cast<uint8_t>((rand1 >> 24) & 0xFF);
    uuid.data[11] = static_cast<uint8_t>((rand1 >> 16) & 0xFF);
    uuid.data[12] = static_cast<uint8_t>((rand1 >> 8) & 0xFF);
    uuid.data[13] = static_cast<uint8_t>(rand1 & 0xFF);
    uuid.data[14] = static_cast<uint8_t>((rand2 >> 8) & 0xFF);
    uuid.data[15] = static_cast<uint8_t>(rand2 & 0xFF);

    // Set version to 7 (4 bits: 0111) in byte 6, high nibble
    uuid.data[6] = (uuid.data[6] & 0x0F) | 0x70;

    // Set variant to RFC 4122 (2 bits: 10) in byte 8, high 2 bits
    uuid.data[8] = (uuid.data[8] & 0x3F) | 0x80;

    return uuid;
}

} // namespace starrocks
