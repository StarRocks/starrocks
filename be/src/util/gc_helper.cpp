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

#include "util/gc_helper.h"

#include <iostream>

namespace starrocks {

GCHelper::GCHelper(const size_t period, const size_t interval, const MonoTime& now)
        : _interval(interval * period * 1000 * 1000 * 1000 / SMOOTHSTEP_NSTEPS),
          _epoch(now),
          _bytes_limit(0),
          _remained_bytes(0) {
    memset(_backlog, 0, SMOOTHSTEP_NSTEPS * sizeof(size_t));
}

// compute how many bytes we should gc based on `current_bytes`
size_t GCHelper::bytes_should_gc(const MonoTime& now, const size_t current_bytes) {
    // step 1: compute how many steps we can move forward since last call
    if (_epoch >= now) // time went backward, defensive code
        return 0;

    MonoDelta delta = now - _epoch;

    uint64_t nadvance = delta.ToNanoseconds() / _interval;
    if (nadvance == 0) // step too small
        return 0;

    delta = MonoDelta::FromNanoseconds(_interval * nadvance);
    _epoch += delta;

    // step 2: update backlog accroding to steps `nadvance`
    _backlog_update(nadvance, current_bytes);

    // step 3: compute bytes limit according to backlog
    _bytes_limit = _backlog_bytes_limit();

    // step 4: record how many bytes there will be left
    _remained_bytes = (_bytes_limit > current_bytes) ? _bytes_limit : current_bytes;

    return current_bytes > _bytes_limit ? (current_bytes - _bytes_limit) : 0;
}

void GCHelper::_backlog_update(const uint64_t nadvance, const size_t current_bytes) {
    if (nadvance >= SMOOTHSTEP_NSTEPS) {
        // nadvance too big, clear all `_backlog` info
        memset(_backlog, 0, (SMOOTHSTEP_NSTEPS - 1) * sizeof(size_t));
    } else {
        // move `_backlog` elements `nadvance` step forward
        memmove(_backlog, &_backlog[nadvance], (SMOOTHSTEP_NSTEPS - nadvance) * sizeof(size_t));
        if (nadvance > 1) {
            // clear remained `_backlog` info
            memset(&_backlog[SMOOTHSTEP_NSTEPS - nadvance], 0, (nadvance - 1) * sizeof(size_t));
        }
    }

    // set `_backlog[last]` based on `current_bytes`
    size_t bytes_delta = (current_bytes > _remained_bytes) ? current_bytes - _remained_bytes : 0;
    _backlog[SMOOTHSTEP_NSTEPS - 1] = bytes_delta;
}

size_t GCHelper::_backlog_bytes_limit() {
    /*
     * For each element of backlog, multiply by the corresponding
     * fixed-point smoothstep factor.  Sum the products, then divide
     * to round down to the nearest whole number of bytes.
     */
    uint64_t sum = 0;
    for (unsigned i = 0; i < SMOOTHSTEP_NSTEPS; i++) {
        sum += _backlog[i] * get_smoothstep_at(i);
    }
    auto bytes = (size_t)(sum >> SMOOTHSTEP_BFP);
    return bytes;
}

} // namespace starrocks
