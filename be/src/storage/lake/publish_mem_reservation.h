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
//   4. no process headroom   -> reject (retryable) before either admitting route. Applies to BOTH the
//                               oversized and the normal route, because the lake tracker is off-tree and
//                               says nothing about process pressure: a node already near the urgent line
//                               can otherwise admit a publish that fits the mostly empty lake budget and
//                               then allocate past the process limit. Disabled by process_urgent_pct <= 0.
//   5. estimate > limit      -> oversized: one publish alone exceeds the whole budget. A plain
//                               try_consume would reject it forever (FE retries indefinitely) -> a
//                               permanent partition wedge. Admit at most ONE oversized publish
//                               process-wide via the atomic slot, WITHOUT charging the shared tracker,
//                               so normal-sized publishes keep flowing. The slot bounds how many
//                               oversized publishes run, not how big they are; route 4 bounds the size.
//                               Peak bound: limit + one oversized.
//   6. otherwise             -> normal: atomic try_consume(estimate) reserve-or-fail; reject -> FE retries.
//
// Route selection never depends on live consumption(), so there is no check-then-act race in choosing a
// route: "oversized" is a comparison of two stable values (estimate this call, limit fixed at init), and
// admission of an oversized publish is a single compare_exchange on the slot.
//
// Route 4 is the one consumption-based test, and it is a backstop rather than a reservation. It cannot be
// made atomic here: the only shared counter that could be reserved against is the process tracker itself,
// and the allocations this publish goes on to make are already charged there, so consuming the estimate up
// front would double count every publish. So two publishes can pass route 4 concurrently and both be
// admitted. What bounds that case is the lake tracker, which IS atomic: concurrent normal publishes still
// contend for one budget and the loser is rejected. Route 4 only narrows the window in which a node that is
// already near the urgent line admits more work, and it can only ever turn an admit into a reject, so a
// concurrent allocation racing it is strictly safer than not checking at all.
class PublishMemReservation {
public:
    // `oversized_slot` is a process-wide atomic shared by all publishes on this node (the caller passes
    // the file-global g_lake_publish_oversized_inflight; tests pass a local).
    // `process_tracker` (optional) is the process-wide tracker, consulted on every admitting route so a
    // publish cannot be let through on lake-budget room alone while the process is already near its limit.
    // `process_urgent_pct` is the ceiling for that check, as a percent of the process limit. It is the
    // check's kill switch: <= 0 skips it entirely. Callers pass config::lake_publish_process_memory_urgent_pct,
    // which is mutable, so the check can be disabled live without a restart.
    PublishMemReservation(MemTracker* tracker, int64_t estimate, std::atomic<bool>& oversized_slot,
                          MemTracker* process_tracker = nullptr, int32_t process_urgent_pct = 0)
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
        // Process headroom, checked before EITHER admitting route.
        //
        // The lake tracker is off-tree, so fitting inside it says nothing about whether the process can
        // absorb the allocation. The entry backstop asks "is usage already above the urgent percent", not
        // "does this fit", so a node at 84% with an 85% threshold passes it and then admits a publish
        // estimated well above the remaining headroom, on either route. On the oversized route nothing is
        // charged at all, and on the normal route the estimate only has to fit the mostly empty lake
        // budget. Both drive the node past its process limit. So ask whether this publish still fits under
        // the urgent line before letting it through.
        //
        // This does not reintroduce the wedge the oversized route exists to avoid. Rejection here is
        // retryable backpressure, not a permanent refusal, and an estimate is a fraction of the process
        // limit, so a node with room always admits it. The only estimate that never fits is one larger
        // than the whole process, which would OOM rather than succeed.
        if (lacks_process_headroom(process_tracker, process_urgent_pct, estimate)) {
            _admitted = false; // reject (retryable), take no slot and charge nothing
            return;
        }
        if (estimate > limit) {
            // Oversized: admit at most one process-wide, without charging the shared tracker. The slot
            // bounds the COUNT of concurrent oversized publishes (one), not their SIZE. The size is
            // bounded by the headroom check above.
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
    // True when `estimate` would push process consumption past `urgent_pct` of the process limit.
    // Returns false (admit) whenever the check is not wired or is switched off, so a missing tracker or a
    // zeroed percent can never block publishing.
    static bool lacks_process_headroom(MemTracker* process_tracker, int32_t urgent_pct, int64_t estimate) {
        if (process_tracker == nullptr || urgent_pct <= 0) {
            return false; // not wired, or kill switch engaged
        }
        // Clamp here rather than trusting the caller, so a mistyped config can never make the ceiling
        // exceed the process limit and silently turn the check into a no-op.
        if (urgent_pct > 100) {
            urgent_pct = 100;
        }
        const int64_t process_limit = process_tracker->limit();
        if (process_limit <= 0) {
            return false; // unlimited process tracker: nothing to be near the edge of
        }
        // Divide before multiplying so a large limit cannot overflow. Costs at most 99 bytes of precision.
        const int64_t ceiling = process_limit / 100 * urgent_pct;
        return process_tracker->consumption() + estimate > ceiling;
    }

    MemTracker* _tracker;
    std::atomic<bool>* _oversized_slot;
    int64_t _consumed = 0;
    bool _admitted = false;
    bool _holds_oversized_slot = false;
};

} // namespace starrocks::lake
