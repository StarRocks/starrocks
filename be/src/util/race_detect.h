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
#include <glog/logging.h>

#include <atomic>

namespace starrocks {

class RaceDetector {
public:
    class Guard {
    public:
        Guard(std::atomic_uint64_t& ref_) : ref(ref_) {
            ref++;
            CHECK_EQ(ref, 1) << "not expected concurrency detected";
        }
        ~Guard() { --ref; }

    private:
        std::atomic_uint64_t& ref;
    };
    Guard guard() { return {_running}; }

private:
    std::atomic_uint64_t _running{};
};
} // namespace starrocks

#ifndef NDEBUG
#define ENABLE_RACE_DETECTOR
#endif

#ifdef ENABLE_RACE_DETECTOR
#define DECLARE_RACE_DETECTOR(name) RaceDetector name;
#define RACE_DETECT(name, var) auto var = name.guard()
#else
#define DECLARE_RACE_DETECTOR(name)
#define RACE_DETECT(name, var)
#endif