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

// CodecSelector: the decision-tree walker for spill column encoding.
//
//   layer 1 (schema prior)   : CodecRegistry::candidates(column) prunes to <=N codecs
//   layer 2 (sampled evidence): the first kSamples chunks of every kWindow-chunk window
//                               trial-encode ALL candidates; sizes are recorded here
//   layer 3 (algorithm)      : at the end of the sampling phase the per-column winner is
//                               locked for the rest of the window; re-sampling every window
//                               provides drift adaptation and the demotion path
//
// This mirrors (and for the spill path supersedes) serde::EncodeContext's windowed
// ratio-driven on/off logic, generalized from {encode, don't} to an arbitrary candidate
// set. Same concurrency model: one instance per Spiller, shared by flush threads, guarded
// by a shared_mutex.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <shared_mutex>
#include <sstream>
#include <vector>

#include "common/logging.h"
#include "compute_env/spill/codec/spill_codec.h"
#include "compute_env/spill/common.h"

namespace starrocks::spill {

class CodecSelector {
public:
    // Sampling cadence (window size mirrors serde::EncodeContext's frequency; three
    // trial chunks per sampled window are enough since trials are prefix-sliced).
    static constexpr uint64_t kSamples = 3;
    static constexpr uint64_t kWindow = 64;
    // Spilled bytes pay TWO IO passes -- written once and read back once on restore -- so cost
    // models weigh byte savings twice. The CPU term needs no doubling: the sampling trial times
    // encode AND decode together (see serialize_v2), and stage-2's block decode is ~free.
    static constexpr double kIoPasses = 2.0;
    // Mild hysteresis: a non-RAW codec must beat RAW's score by this factor, so ~free
    // RAW never loses to a marginally-smaller-but-costlier encoding.
    static constexpr double kScoreMargin = 0.98;
    // A candidate scoring worse than kPruneFactor x the column's best is skipped in the
    // next sampling window (it re-enters after the pruned set resets on decision change).
    static constexpr double kPruneFactor = 2.0;
    // Incumbent stickiness: a challenger must beat the current choice's score by this
    // factor to take over. Without it, encode-time jitter flips near-tied decisions
    // between windows, permanently resetting the stability backoff (measured: string
    // columns kept dense-sampling zstd/lz4/dict forever, serialize +40x).
    static constexpr double kSwitchMargin = 0.9;
    // A codec that carries a per-column encode context (a trained table, a set of tuned
    // parameters) builds it ONCE per sampling window from that window's first sampled chunk, then
    // reuses it for every chunk until the next sampling window -- up to kWindow x kMaxBackoff
    // chunks. Trials always run against a freshly built context, so the selector's scores never
    // observe how a context ages inside its own reign: without this check the backoff can keep
    // ramping while the reused parameters drift out of tune. So the locked phase reports what it
    // actually achieved, and a column that encodes this much worse than it did at decision time
    // pulls the next sampling window forward.
    static constexpr double kRatioDriftFactor = 1.15;
    // Max sampling interval (in windows) reached by the stability backoff. Sized so that
    // even expensive trial encodes (block compressors on strings) cost <0.1% at steady
    // state: 64 windows x 64 chunks = re-check every ~4096 chunks.
    static constexpr uint64_t kMaxBackoff = 64;
    // The backoff is reached by doubling, so the stability counter only ever needs log2(kMaxBackoff)
    // states; keeping the two in a static_assert stops the shift below from silently drifting out of
    // step with the ceiling above.
    static constexpr uint64_t kMaxBackoffShift = 6;
    static_assert(kMaxBackoff == (uint64_t{1} << kMaxBackoffShift));

    // `disk_bytes_per_ns` prices encode CPU in written-bytes units:
    //     score = kIoPasses * encoded_bytes + encode_ns * disk_bytes_per_ns
    // It is a POLICY input, not a measurement: it answers "how many bytes of IO is one
    // nanosecond of encode+decode CPU worth", and the caller supplies it from
    // spill_codec_disk_bandwidth_mbps. LOWER values make CPU cheap relative to bytes and bias
    // toward heavier compression; HIGHER values bias toward cheap encodings.
    //
    // The value only needs to be right to within a small factor, and the curve is ASYMMETRIC --
    // measured on the spill bench (20 datasets, buffered, 1GB each), taking the best point as 0:
    //     0.5x -> -1.4 points    0.1x -> -6.9 points    0.025x -> -31.8 points
    //     2x   -> -7.0 points    5x   -> -23.7 points   20x    -> -60.3 points (slower than
    //                                                             not compressing at all)
    // Erring LOW over-compresses but the extra CPU still buys real IO savings; erring HIGH stops
    // compressing, so it loses the IO savings AND still pays to sample. When in doubt, go low.
    CodecSelector(size_t column_count, int session_encode_level, double disk_bytes_per_ns)
            : _session_encode_level(session_encode_level),
              _disk_bytes_per_ns(disk_bytes_per_ns),
              _columns(column_count) {}

    int session_encode_level() const { return _session_encode_level; }

    // Advances the chunk counter; returns true if the caller must run this chunk in
    // sampling mode (trial-encode candidates and report via record_sample()).
    //
    // Stability backoff: while decisions keep coming out unchanged, the sampling window
    // interval doubles (up to kMaxBackoff windows), so long stable spills pay ~zero
    // steady-state sampling cost; any decision change resets the cadence.
    bool begin_chunk() {
        std::unique_lock l(_mutex);
        uint64_t window = _chunk_seq / kWindow;
        uint64_t pos = _chunk_seq % kWindow;
        ++_chunk_seq;
        // Ratio-drift trigger, evaluated at every window boundary (one relaxed exchange, so it
        // costs nothing in the windows that end up not sampling). report_locked_ratio() raises
        // the flag when a locked column's achieved ratio degrades past kRatioDriftFactor.
        if (pos == 0 && _force_resample.exchange(false, std::memory_order_relaxed)) {
            _next_sample_window = window;
            _stable_windows = 0; // the data moved under the locked decision: restart the ramp
        }
        if (pos == 0 && window >= _next_sample_window) {
            _sampling_window = true;
            _window_finalizes = 0;
            for (auto& col : _columns) {
                col.samples.clear();
                col.trial_ctxs.clear(); // rebuild contexts each sampled window (data drift)
                col.window_start_chosen = col.chosen;
            }
        } else if (pos == 0) {
            _sampling_window = false;
        }
        return _sampling_window && pos < kSamples;
    }

    // Record one trial-encode outcome (payload size + measured encode time) for a
    // sampling chunk.
    void record_sample(size_t col, CodecCandidate cand, uint64_t encoded_bytes, uint64_t encode_ns) {
        std::unique_lock l(_mutex);
        auto& samples = _columns[col].samples;
        for (auto& s : samples) {
            if (s.cand.id == cand.id && s.cand.param == cand.param) {
                s.encoded_bytes += encoded_bytes;
                s.encode_ns += encode_ns;
                s.chunks += 1;
                return;
            }
        }
        samples.push_back({cand, encoded_bytes, encode_ns, 1});
    }

    // Locked-phase feedback: what the chosen codec actually achieved on a full chunk. Lock-free
    // on purpose -- this runs for every column of every non-sampling chunk. The baseline is a
    // relaxed atomic because a stale read only shifts one sampling window.
    void report_locked_ratio(size_t col, uint64_t encoded_bytes, uint64_t raw_bytes) {
        if (raw_bytes == 0) return;
        const double baseline = _columns[col].decision_ratio.load(std::memory_order_relaxed);
        if (baseline <= 0) return; // RAW, or no decision scored yet: nothing to degrade from
        const double live = static_cast<double>(encoded_bytes) / static_cast<double>(raw_bytes);
        if (live > baseline * kRatioDriftFactor) {
            _force_resample.store(true, std::memory_order_relaxed);
        }
    }

    // Returns true if `cand` is worth trial-encoding for `col` in this sampling window.
    // Candidates that scored disastrously (> kPruneFactor x the column's best) in the
    // previous sampled window are skipped; RAW is always tried (it is the denominator).
    bool should_try(size_t col, const CodecCandidate& cand) const {
        if (cand.id == CodecId::RAW) return true;
        std::shared_lock l(_mutex);
        const auto& pruned = _columns[col].pruned;
        for (const auto& p : pruned) {
            if (p.id == cand.id && p.param == cand.param) return false;
        }
        return true;
    }

    // Lock in the per-column winner from the accumulated samples (called after each
    // sampling chunk; the decision converges as samples accumulate). Also maintains the
    // stability backoff and the per-column pruned-candidate set.
    void finalize_sampling() {
        std::unique_lock l(_mutex);
        for (auto& col : _columns) {
            if (col.samples.empty()) continue;
            _decide_column(col);
        }
        _trace_window_decisions();
        _advance_stability_backoff();
    }

    CodecCandidate chosen(size_t col) const {
        std::shared_lock l(_mutex);
        return _columns[col].chosen;
    }

    std::shared_ptr<CodecContext> chosen_context(size_t col) const {
        std::shared_lock l(_mutex);
        return _columns[col].chosen_ctx;
    }

    // get-or-remember a per-candidate trial context (built by the caller, untimed)
    std::shared_ptr<CodecContext> trial_context(size_t col, const CodecCandidate& cand) const {
        std::shared_lock l(_mutex);
        for (const auto& [c, ctx] : _columns[col].trial_ctxs) {
            if (c.id == cand.id && c.param == cand.param) return ctx;
        }
        return nullptr;
    }

    void put_trial_context(size_t col, const CodecCandidate& cand, std::shared_ptr<CodecContext> ctx) {
        std::unique_lock l(_mutex);
        _columns[col].trial_ctxs.emplace_back(cand, std::move(ctx));
    }

private:
    struct Sample {
        CodecCandidate cand;
        uint64_t encoded_bytes = 0;
        uint64_t encode_ns = 0;
        uint64_t chunks = 0; // trial count; pruning makes counts unequal across candidates
    };
    struct ColumnState {
        CodecCandidate chosen{CodecId::RAW, 0};
        CodecCandidate window_start_chosen{CodecId::RAW, 0}; // for stability detection
        std::vector<Sample> samples;
        std::vector<CodecCandidate> pruned; // losers skipped in the next sampling window
        // per-candidate encode contexts built during trials (e.g. FSST symbol table);
        // the winner's context is promoted to chosen_ctx and reused in the locked phase
        std::vector<std::pair<CodecCandidate, std::shared_ptr<CodecContext>>> trial_ctxs;
        std::shared_ptr<CodecContext> chosen_ctx;
        // encoded/raw achieved by `chosen` on the trial data; 0 = check disabled (see
        // report_locked_ratio). Written under _mutex, read lock-free from the locked phase.
        std::atomic<double> decision_ratio{0.0};
    };

    // Per-chunk average score, so candidates sampled a different number of times
    // (pruning skips some) stay comparable.
    double score(const Sample& s) const {
        if (s.chunks == 0) return -1;
        double avg_bytes = static_cast<double>(s.encoded_bytes) / static_cast<double>(s.chunks);
        double avg_ns = static_cast<double>(s.encode_ns) / static_cast<double>(s.chunks);
        return kIoPasses * avg_bytes + avg_ns * _disk_bytes_per_ns;
    }

    // ---- finalize_sampling() steps; every one of these runs with _mutex held exclusively ----

    // This window's sample for one exact candidate, or nullptr if it was never trialled
    // (candidate sets differ per column type and pruning skips proven losers).
    const Sample* _sample_for(const ColumnState& col, const CodecCandidate& cand) const {
        for (const auto& s : col.samples) {
            if (s.cand.id == cand.id && s.cand.param == cand.param) return &s;
        }
        return nullptr;
    }

    // Picks the codec this column will use for the coming locked phase, then refreshes
    // everything that phase reads: the promoted context, the drift baseline, the prune set.
    void _decide_column(ColumnState& col) {
        const Sample* raw = _sample_for(col, CodecCandidate{CodecId::RAW, 0});
        const double raw_score = raw != nullptr ? score(*raw) : -1;

        CodecCandidate best{CodecId::RAW, 0};
        double best_score = raw_score;
        for (const auto& s : col.samples) {
            if (s.cand.id == CodecId::RAW) continue;
            const double sc = score(s);
            if ((best_score < 0 || sc < best_score) && (raw_score < 0 || sc < raw_score * kScoreMargin)) {
                best = s.cand;
                best_score = sc;
            }
        }
        // Incumbent stickiness: only switch on a clear win over the current choice. `best_score`
        // deliberately keeps the CHALLENGER's score even when the choice reverts below -- the prune
        // set is measured against the best score this window observed, not against what was kept.
        if (best.id != col.chosen.id || best.param != col.chosen.param) {
            const Sample* incumbent = _sample_for(col, col.chosen);
            const double incumbent_score = incumbent != nullptr ? score(*incumbent) : -1;
            if (incumbent_score >= 0 && best_score >= incumbent_score * kSwitchMargin) {
                best = col.chosen;
            }
        }
        col.chosen = best;
        _promote_trial_context(col, best);
        col.decision_ratio.store(_drift_baseline(col, best, raw), std::memory_order_relaxed);
        _rebuild_prune_set(col, best_score);
    }

    // The winner's trial context (e.g. an FSST symbol table) carries into the locked phase;
    // a codec that needs no context leaves chosen_ctx null.
    void _promote_trial_context(ColumnState& col, const CodecCandidate& best) const {
        col.chosen_ctx.reset();
        for (const auto& [cand, ctx] : col.trial_ctxs) {
            if (cand.id == best.id && cand.param == best.param) {
                col.chosen_ctx = ctx;
                return;
            }
        }
    }

    // Baseline for the ratio-drift trigger: what the winner achieved on the trial data.
    // RAW cannot degrade, so 0 disables the check for a column that stays uncompressed.
    double _drift_baseline(const ColumnState& col, const CodecCandidate& best, const Sample* raw) const {
        if (best.id == CodecId::RAW || raw == nullptr || raw->chunks == 0) return 0;
        const double raw_bytes = static_cast<double>(raw->encoded_bytes) / static_cast<double>(raw->chunks);
        if (raw_bytes <= 0) return 0;
        const Sample* s = _sample_for(col, best);
        if (s == nullptr || s->chunks == 0) return 0;
        return (static_cast<double>(s->encoded_bytes) / static_cast<double>(s->chunks)) / raw_bytes;
    }

    // Candidates scoring far worse than this window's best are skipped in the next one.
    void _rebuild_prune_set(ColumnState& col, double best_score) const {
        col.pruned.clear();
        if (best_score <= 0) return;
        for (const auto& s : col.samples) {
            if (s.cand.id != CodecId::RAW && score(s) > best_score * kPruneFactor) {
                col.pruned.push_back(s.cand);
            }
        }
    }

    // One trace line per column with the samples that produced its pick. Gated by the spill
    // trace level so the string building stays out of the way when tracing is off.
    void _trace_window_decisions() const {
        if (!VLOG_IS_ON(10)) return;
        for (size_t i = 0; i < _columns.size(); ++i) {
            const auto& col = _columns[i];
            std::ostringstream samples;
            for (const auto& s : col.samples) {
                samples << " id" << static_cast<int>(s.cand.id) << "=" << s.encoded_bytes << "B/" << s.encode_ns
                        << "ns";
            }
            TRACE_SPILL_LOG << "[codec] W=" << _disk_bytes_per_ns << " col=" << i
                            << " chosen=" << static_cast<int>(col.chosen.id) << " samples:" << samples.str();
        }
    }

    // Stability/backoff settles once per window, on its last sampling finalize (the counter is
    // approximate under concurrency, which only shifts a sample window).
    void _advance_stability_backoff() {
        if (++_window_finalizes < kSamples) return;
        _window_finalizes = 0;
        bool any_changed = false;
        for (const auto& col : _columns) {
            if (col.chosen.id != col.window_start_chosen.id || col.chosen.param != col.window_start_chosen.param) {
                any_changed = true;
                break;
            }
        }
        const uint64_t window = (_chunk_seq - 1) / kWindow;
        _stable_windows = any_changed ? 0 : std::min<uint64_t>(_stable_windows + 1, kMaxBackoffShift);
        _next_sample_window = window + (uint64_t{1} << _stable_windows);
    }

    const int _session_encode_level;
    const double _disk_bytes_per_ns; // policy: bytes of IO one ns of encode+decode CPU is worth
    mutable std::shared_mutex _mutex;
    uint64_t _chunk_seq = 0;
    bool _sampling_window = true; // window 0 always samples
    uint64_t _next_sample_window = 0;
    uint64_t _stable_windows = 0;
    uint64_t _window_finalizes = 0;
    // raised lock-free by report_locked_ratio(), consumed at the next window boundary
    std::atomic<bool> _force_resample{false};
    std::vector<ColumnState> _columns;
};

} // namespace starrocks::spill
