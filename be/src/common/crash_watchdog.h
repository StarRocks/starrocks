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

#include <cstdint>

namespace starrocks {

// Starts a detached, process-lifetime background thread that force-terminates the process when the
// fatal-signal (crash) handler has marked the process as crashing (is_process_crashing()) but the
// process is still alive `timeout_seconds` later. This bounds the zombie state where a stalled
// crash handler keeps the process alive while every FE heartbeat is answered with SHUTDOWN forever
// (https://github.com/StarRocks/starrocks/issues/76441).
//
// Config agnostic: the caller decides the timeout (in production, from
// config::process_force_exit_after_crash_handler_hang_second). When `timeout_seconds <= 0` the
// watchdog is disabled and no thread is launched, hence "may". The crash flag is never set here;
// set_process_is_crashing() stays in the handler where it is, so this only observes the flag and
// enforces a deadline. Call once during startup; later calls are no-ops. Never call this from the
// crash handler itself (pthread_create is not async-signal-safe).
void may_start_crash_watchdog(int64_t timeout_seconds);

// Deadline logic behind the watchdog thread, split out so it can be unit-tested without spawning a
// thread or terminating the test process. It latches the first time it observes the process
// crashing and, once the crash flag has stayed set for at least the timeout, invokes a terminate
// callback (injectable so tests can substitute a stub for the real force-exit).
class CrashWatchdog {
public:
    using TerminateFn = void (*)();

    // `terminate` is invoked when the deadline expires; nullptr (the default) means the real
    // force-exit path used by the watchdog thread.
    explicit CrashWatchdog(TerminateFn terminate = nullptr) : _terminate(terminate) {}

    // Runs one watchdog step. `now_mono_ns` is a CLOCK_MONOTONIC reading in nanoseconds. Invokes
    // the terminate callback once the process has been crashing continuously for at least
    // `timeout_seconds`. `timeout_seconds <= 0` disables it, and a non-crashing observation resets
    // the latch so the deadline only ever measures an uninterrupted crashing streak.
    void run_once(bool crashing, int64_t timeout_seconds, int64_t now_mono_ns);

private:
    bool deadline_reached(bool crashing, int64_t timeout_seconds, int64_t now_mono_ns);

    TerminateFn _terminate;
    // Monotonic time of the first crashing observation in the current streak; -1 = not crashing.
    int64_t _first_crashing_mono_ns = -1;
};

} // namespace starrocks
