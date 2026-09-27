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

#include "common/crash_watchdog.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <ctime>
#include <thread>
#include <tuple>

#include "common/process_exit.h"

namespace starrocks {

bool CrashWatchdog::deadline_reached(bool crashing, int64_t timeout_seconds, int64_t now_mono_ns) {
    if (!crashing) {
        _first_crashing_mono_ns = -1;
        return false;
    }
    if (_first_crashing_mono_ns < 0) {
        _first_crashing_mono_ns = now_mono_ns;
    }
    if (timeout_seconds <= 0) {
        return false;
    }
    // Compare in seconds to avoid overflowing timeout_seconds * 1e9 for large configured values.
    int64_t elapsed_seconds = (now_mono_ns - _first_crashing_mono_ns) / 1000000000LL;
    return elapsed_seconds >= timeout_seconds;
}

namespace {

// Runs after the process is already known to be crashing, so the heap may be corrupt: keep this
// path allocation- and LOG-free, and never let it block. This is the last-resort killer, so it
// terminates the process directly with _exit() instead of raising SIGABRT: a core dump can be slow
// or fail on large-memory hosts and must not be relied on here (a hung-state core can still be
// captured manually with gcore within the grace window before the watchdog fires).
[[noreturn]] void force_exit_after_crash_hang() {
    static const char msg[] = "[crash_watchdog] crash handler stalled; forcing process exit\n";
    // Best-effort diagnostic: make stderr non-blocking first so a full log pipe cannot stall this
    // last-resort killer (a dropped or partial write is acceptable).
    (void)fcntl(STDERR_FILENO, F_SETFL, fcntl(STDERR_FILENO, F_GETFL) | O_NONBLOCK);
    std::ignore = write(STDERR_FILENO, msg, sizeof(msg) - 1);
    _exit(1);
}

} // namespace

void CrashWatchdog::run_once(bool crashing, int64_t timeout_seconds, int64_t now_mono_ns) {
    if (deadline_reached(crashing, timeout_seconds, now_mono_ns)) {
        _terminate != nullptr ? _terminate() : force_exit_after_crash_hang();
    }
}

namespace {

void crash_watchdog_loop(int64_t timeout_seconds) {
    CrashWatchdog watchdog;
    while (true) {
        struct timespec interval = {1, 0};
        // nanosleep is a raw syscall; tolerate EINTR by simply looping on the next poll.
        (void)::nanosleep(&interval, nullptr);

        struct timespec now = {0, 0};
        clock_gettime(CLOCK_MONOTONIC, &now);
        int64_t now_mono_ns = static_cast<int64_t>(now.tv_sec) * 1000000000LL + now.tv_nsec;

        watchdog.run_once(is_process_crashing(), timeout_seconds, now_mono_ns);
    }
}

} // namespace

// File-scope (extern-able from the unit test, like k_starrocks_be_crashing in process_exit.cpp).
std::atomic<bool> g_crash_watchdog_started{false};

void may_start_crash_watchdog(int64_t timeout_seconds) {
    bool expected = false;
    if (!g_crash_watchdog_started.compare_exchange_strong(expected, true)) {
        return;
    }
    // A non-positive timeout means disabled: no thread is launched at all.
    if (timeout_seconds <= 0) {
        return;
    }
    // Detached, process-lifetime thread. Spawned before any crash so pthread_create (not
    // async-signal-safe) never runs from the handler.
    std::thread([timeout_seconds] { crash_watchdog_loop(timeout_seconds); }).detach();
}

} // namespace starrocks
