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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/uuid_generator.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <boost/random/mersenne_twister.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <cstring>
#include <random>
#include <string>

namespace starrocks {

class ThreadLocalUUIDGenerator {
public:
    static boost::uuids::uuid next_uuid() { return s_tls_gen(); }

    static std::string next_uuid_string() { return boost::uuids::to_string(next_uuid()); }

    // Generate UUID v7 according to RFC 9562
    // Format: 48-bit timestamp (ms) + 4-bit version + 12-bit random + 2-bit variant + 62-bit random
    static boost::uuids::uuid next_uuid_v7() {
        boost::uuids::uuid uuid;

        // Get current time in milliseconds since Unix epoch
        auto now = std::chrono::system_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

        // Fill the UUID with random data first
        for (size_t i = 0; i < uuid.size(); ++i) {
            uuid.data[i] = static_cast<uint8_t>(s_tls_rng());
        }

        // Set timestamp (first 48 bits / 6 bytes)
        uuid.data[0] = static_cast<uint8_t>((ms >> 40) & 0xFF);
        uuid.data[1] = static_cast<uint8_t>((ms >> 32) & 0xFF);
        uuid.data[2] = static_cast<uint8_t>((ms >> 24) & 0xFF);
        uuid.data[3] = static_cast<uint8_t>((ms >> 16) & 0xFF);
        uuid.data[4] = static_cast<uint8_t>((ms >> 8) & 0xFF);
        uuid.data[5] = static_cast<uint8_t>(ms & 0xFF);

        // Set version to 7 (4 bits: 0111) in byte 6, high nibble
        uuid.data[6] = (uuid.data[6] & 0x0F) | 0x70;

        // Set variant to RFC 4122 (2 bits: 10) in byte 8, high 2 bits
        uuid.data[8] = (uuid.data[8] & 0x3F) | 0x80;

        return uuid;
    }

    static std::string next_uuid_v7_string() { return boost::uuids::to_string(next_uuid_v7()); }

private:
    static inline thread_local boost::uuids::basic_random_generator<boost::mt19937> s_tls_gen;
    static inline thread_local std::mt19937 s_tls_rng{std::random_device{}()};
};

} // namespace starrocks
