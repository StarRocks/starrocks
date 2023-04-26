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

#include <atomic>

namespace starrocks {

class BackgroundTask {
public:
    BackgroundTask() : _stopped(false) {}

    virtual ~BackgroundTask() = default;

    virtual void run() = 0;

    void start() { run(); }

    // just set the _stopped flag to true
    // the task should check the flag to stop
    void stop() { _stopped = true; }

    virtual bool should_stop() const { return _stopped; }

protected:
    std::atomic_bool _stopped;
};

} // namespace starrocks
