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
//   https://github.com/apache/incubator-doris/blob/master/be/src/base/uuid/uuid_generator.h

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
#include <thread>

namespace starrocks {

class ThreadLocalUUIDGenerator {
public:
    static boost::uuids::uuid next_uuid() { return s_tls_gen(); }

    static std::string next_uuid_string() { return boost::uuids::to_string(next_uuid()); }

    // Generate UUID v7 according to RFC 9562
    // Format: 48-bit timestamp (ms) + 4-bit version + 12-bit random + 2-bit variant + 62-bit random
    static boost::uuids::uuid next_uuid_v7();

    static std::string next_uuid_v7_string() { return boost::uuids::to_string(next_uuid_v7()); }

private:
    static inline thread_local boost::uuids::basic_random_generator<boost::mt19937> s_tls_gen;

    // Thread-local random number generator with efficient seeding
    // Combines thread ID and high-resolution clock for better seed distribution
    static inline thread_local std::mt19937 s_tls_rng{
            static_cast<unsigned int>(std::hash<std::thread::id>{}(std::this_thread::get_id()) ^
                                      std::chrono::high_resolution_clock::now().time_since_epoch().count())};
};

} // namespace starrocks
