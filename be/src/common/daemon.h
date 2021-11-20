// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/common/daemon.h

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

#ifndef STARROCKS_BE_SRC_COMMON_COMMON_DAEMON_H
#define STARROCKS_BE_SRC_COMMON_COMMON_DAEMON_H

#include <atomic>
#include <thread>
#include <vector>

#include "storage/options.h"

namespace starrocks {

class Daemon {
public:
    Daemon() = default;
    ~Daemon() = default;

    void init(int argc, char** argv, const std::vector<StorePath>& paths);
    void stop();
    bool stopped();

private:
    std::atomic<bool> _stopped{false};

    std::vector<std::thread> _daemon_threads;
    DISALLOW_COPY_AND_ASSIGN(Daemon);
};

} // namespace starrocks

#endif
