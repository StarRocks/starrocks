// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "util/gc_helper_smoothstep.h"
#include "util/monotime.h"

namespace starrocks {

#define GCBYTES_ONE_STEP (16 * 1024 * 1024) // minimun GC bytes in one step

// GCHelper is for tcmalloc GC, it accepts a period and a start time for initialization,
// for each gc, call `bytes_should_gc` to calculate how many bytes should gc for current time
class GCHelper {
public:
    GCHelper(const size_t period, const size_t interval, const MonoTime& now);

    size_t bytes_should_gc(const MonoTime& now, const size_t current_bytes);

private:
    void _backlog_update(const uint64_t nadvance, const size_t current_bytes);

    size_t _backlog_bytes_limit();

    size_t _period;                     // gc period in second
    size_t _interval;                   // gc interval for period
    MonoTime _epoch;                    // last timestamp `bytes_should_gc` is called
    size_t _bytes_limit;                // how many bytes should limit to
    size_t _remained_bytes;             // how many bytes there are remained
    size_t _backlog[SMOOTHSTEP_NSTEPS]; // preserve last period gc info, each interval occupies one
};

} // namespace starrocks
