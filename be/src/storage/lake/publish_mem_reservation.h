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
#include <cstdint>

#include "runtime/mem_tracker.h"

namespace starrocks::lake {

// RAII admission reservation for the shared-data (lake) publish path (part 3 of the publish-stall hardening series).
//
// It reserves an estimate of the transient per-publish metadata footprint against an off-tree,
// byte-limited MemTracker and releases exactly what it consumed on scope exit. Correctness is
// structural (in the ctor/dtor), not positional at call sites.
//
// Routing in the constructor, IN THIS ORDER (the order is load-bearing):
//   1. tracker == nullptr    -> gate not wired: admit, reserve nothing (never block publishing).
//   2. estimate <= 0         -> clamp to 0 and admit, reserve nothing.
//   3. tracker->limit() <= 0 -> kill switch / unlimited (pct==0 maps the limit to -1): gate disabled,
//                               always admit; consume for observability ONLY if try_consume actually
//                               accepts it (it adds unconditionally when limit<0, but is a no-op reject
//                               when limit==0), keeping release symmetric. MUST precede the oversized
//                               check: with limit==-1, estimate>limit is always true and every publish
//                               would otherwise serialize through the oversized slot.
//   4. estimate > limit      -> oversized: one publish alone exceeds the whole budget. A plain
//                               try_consume would reject it forever (FE retries indefinitely) -> a
//                               permanent partition wedge. Admit at most ONE oversized publish
//                               process-wide via the atomic slot, WITHOUT charging the shared tracker,
//                               so normal-sized publishes keep flowing, AND only when the process has
//                               headroom for the estimate (the slot bounds how many oversized publishes
//                               run, not how big they are). Peak bound: limit + one oversized.
//   5. otherwise             -> normal: atomic try_consume(estimate) reserve-or-fail; reject -> FE retries.
//
// Route selection never depends on live consumption(), so there is no check-then-act race in choosing a
// route: "oversized" is a comparison of two stable values (estimate this call, limit fixed at init), and
// admission of an oversized publish is a single compare_exchange on the slot. The one consumption-based
// test is the process-headroom check inside route 4, which can only turn an admit into a reject, never
// the reverse, so a concurrent allocation racing it is strictly safer than not checking at all.
class PublishMemReservation {
public:
    // `oversized_slot` is a process-wide atomic shared by all publishes on this node (the caller passes
    // the file-global g_lake_publish_oversized_inflight; tests pass a local).
    // `process_tracker` (optional) is the process-wide tracker. It is consulted ONLY on the oversized
    // route, to require that the process can actually absorb a publish that the shared budget cannot.
    PublishMemReservation(MemTracker* tracker, int64_t estimate, std::atomic<bool>& oversized_slot,
                          MemTracker* process_tracker = nullptr)
            : _tracker(tracker), _oversized_slot(&oversized_slot) {
        if (_tracker == nullptr) {
            _admitted = true; // gate not wired: never block publishing
            return;
        }
        if (estimate < 0) {
            estimate = 0; // clamp: never release(-N) into a bounded tracker
        }
        if (estimate == 0) {
            _admitted = true; // nothing to reserve
            return;
        }
        const int64_t limit = _tracker->limit();
        if (limit <= 0) {
            // Kill switch / unlimited (or a pathological 0). Gate disabled: always admit. Consume for
            // observability only if it actually lands, so the dtor's release stays symmetric.
            if (_tracker->try_consume(estimate) == nullptr) {
                _consumed = estimate;
            }
            _admitted = true;
            return;
        }
        if (estimate > limit) {
            // Oversized: admit at most one process-wide, without charging the shared tracker.
            //
            // The slot bounds the COUNT of concurrent oversized publishes (one) but not their SIZE, and
            // this route charges nothing, so on its own it admits an arbitrarily large publish as long as
            // the entry backstop has not already tripped. That backstop asks "is usage already above the
            // urgent percent", not "does this fit": at 80% usage with an 85% threshold, a publish
            // estimated well above the remaining 20% is still let through and can drive the node into
            // OOM. That is the exact failure this gate exists to prevent, in the case where the metadata
            // is largest. So require the process to have room for the estimate first.
            //
            // This does not reintroduce the wedge route 4 exists to avoid. Rejection here is retryable
            // backpressure, and an oversized estimate is by definition only a fraction of the process
            // limit, so an idle node always has the headroom to admit it. The only case that never fits
            // is an estimate larger than the whole process, which would OOM rather than succeed.
            if (process_tracker != nullptr) {
                const int64_t process_limit = process_tracker->limit();
                if (process_limit > 0 && estimate > process_limit - process_tracker->consumption()) {
                    _admitted = false; // not enough process headroom -> reject (retryable), take no slot
                    return;
                }
            }
            bool expected = false;
            if (_oversized_slot->compare_exchange_strong(expected, true)) {
                _holds_oversized_slot = true;
                _admitted = true;
            } else {
                _admitted = false; // another oversized already in flight -> reject, FE retries
            }
            return;
        }
        // Normal path: atomic reserve-or-fail against the shared budget.
        _admitted = (_tracker->try_consume(estimate) == nullptr);
        if (_admitted) {
            _consumed = estimate;
        }
    }

    ~PublishMemReservation() {
        if (_consumed > 0) {
            _tracker->release(_consumed); // release exactly what was consumed (0 on the oversized-slot path)
        }
        if (_holds_oversized_slot) {
            _oversized_slot->store(false, std::memory_order_seq_cst); // only the CAS winner clears the slot
        }
    }

    PublishMemReservation(const PublishMemReservation&) = delete;
    PublishMemReservation& operator=(const PublishMemReservation&) = delete;
    PublishMemReservation(PublishMemReservation&&) = delete;
    PublishMemReservation& operator=(PublishMemReservation&&) = delete;

    bool admitted() const { return _admitted; }
    int64_t consumed_bytes() const { return _consumed; }                // for tests / metrics
    bool holds_oversized_slot() const { return _holds_oversized_slot; } // for tests

private:
    MemTracker* _tracker;
    std::atomic<bool>* _oversized_slot;
    int64_t _consumed = 0;
    bool _admitted = false;
    bool _holds_oversized_slot = false;
};

} // namespace starrocks::lake
