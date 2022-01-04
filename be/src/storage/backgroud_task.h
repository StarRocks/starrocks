// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>

namespace starrocks {

class BackgroudTask {
public:
    BackgroudTask() : _stopped(false) {}

    virtual ~BackgroudTask() = default;

    virtual void run() = 0;

    void start() { run(); }

    // just set the _stopped flag to true
    // the task should check the flag to stop
    void stop() { _stopped = true; }

    virtual bool should_stop() { return _stopped; }

protected:
    std::atomic_bool _stopped;
};

} // namespace starrocks
