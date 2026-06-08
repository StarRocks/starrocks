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
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace starrocks {

// =====================================================================================
// SDCG flexible partial update -- the SET-ID spine, BE ingestion side (Agent B).
//
// A "column-set" is the exact set of value columns that a single row updates. In a
// flexible JSON/CDC load, different rows update different column subsets. We intern the
// distinct column-sets of a load into a per-load dictionary; each distinct set gets a
// dense uint16 SET-ID. The set-id is written, per row, into the hidden "__cset__"
// column (a real SMALLINT slot that FE injects immediately before "__op", so "__op"
// stays the last column). The dictionary (set-id -> column list) is later folded into
// RowsetTxnMetaPB.distinct_column_sets so the lake apply / finalize handler can decode
// each row's column mask.
//
// Why column NAMES (not tablet uids) in the scanner-side dict: the JSON scanner only
// knows column names / slot names; it does not have the tablet schema. The writer side
// (delta_writer / rowset_writer), which owns the tablet schema, translates the interned
// name-sets into tablet-column unique-ids when emitting RowsetTxnMetaPB.
// =====================================================================================

// Reserved hidden column literal name carrying the per-row uint16 SET-ID.
// Mirrors FE Load.LOAD_CSET_COLUMN. Positioned immediately before "__op".
inline const std::string LOAD_CSET_COLUMN = "__cset__";

// Reserved tablet-column unique-id for the synthetic "__cset__" column that the delta
// writer appends to the PARTIAL update schema so the set-id survives into the .upt as a
// real Segment v2 column. It must avoid real column uids (which start at 0 and grow) and
// the other sentinel uids in the codebase. We pick a high negative-adjacent sentinel that
// is stable across versions; the lake apply / finalize handler (Agent C) reads the
// "__cset__" column from the .upt by THIS uid (or by the literal name LOAD_CSET_COLUMN).
inline constexpr int32_t kCsetReservedColumnUid = 0x7FFFFFFD; // INT32_MAX - 2

// Logical type used for the __cset__ load slot / chunk / .upt column. SMALLINT carries a
// uint16 set-id; a single load is bounded to 2^16 distinct column-sets (CDC loads have a
// handful), and the interner enforces that bound.
using ColumnSetId = uint16_t;
inline constexpr ColumnSetId kInvalidColumnSetId = static_cast<ColumnSetId>(-1);
inline constexpr size_t kMaxColumnSets = 1u << 16;

// A per-load dictionary that interns distinct column-sets and hands out dense set-ids.
//
// The mask is represented as a sorted, de-duplicated list of column NAMES (the value
// columns present in a row). The interner is keyed by the canonical joined string of
// that list, so identical sets collapse to one set-id regardless of JSON key order.
//
// Thread-safety: a single load can drive multiple scanner threads (split ranges), so
// intern() takes a lock. Reads after the load finishes are single-threaded.
class ColumnSetDict {
public:
    ColumnSetDict() = default;

    // Intern the (sorted/uniqued) set of present value-column names; returns its set-id.
    // `names` is taken by value and sorted/uniqued in place. Returns kInvalidColumnSetId
    // only if the dictionary already holds kMaxColumnSets entries and this is a new set.
    ColumnSetId intern(std::vector<std::string> names);

    size_t size() const {
        std::lock_guard<std::mutex> l(_mu);
        return _sets.size();
    }

    // Snapshot of the dictionary, set-id order (index == set-id). Caller owns the copy.
    std::vector<std::vector<std::string>> snapshot() const {
        std::lock_guard<std::mutex> l(_mu);
        return _sets;
    }

private:
    static std::string canonical_key(const std::vector<std::string>& sorted_names);

    mutable std::mutex _mu;
    // index == set-id
    std::vector<std::vector<std::string>> _sets;
    std::unordered_map<std::string, ColumnSetId> _index;
};

using ColumnSetDictPtr = std::shared_ptr<ColumnSetDict>;

// Process-wide registry mapping txn_id -> ColumnSetDict. The carrier of the dictionary
// from the scanner (which interns) to the co-located tablet writer (which reads it when
// building RowsetTxnMetaPB).
//
// Why txn_id is the key: it identifies a load uniquely and is available verbatim on BOTH
// sides -- the scanner sees it as TBrokerScanRangeParams.txn_id, the writer sees it as
// RowsetWriterContext.txn_id / DeltaWriter opt.txn_id. (load_id / PUniqueId is NOT visible
// to the scanner's RuntimeState, so it is not a usable shared key here.)
//
// NOTE (POC scope / cross-node follow-up): this in-process registry is correct only when
// the scanner and the tablet writer run on the same BE/CN process -- which holds for the
// validated single-node lake stream-load path. For a distributed load (scanner on node X,
// tablet writer on node Y) the dictionary must instead ride the wire: ship the
// ColumnSetDict snapshot in PTabletWriterOpenRequest (or piggy-back on the first
// add-chunk) keyed by txn_id. The on-disk contract (__cset__ + distinct_column_sets) is
// unchanged by that follow-up; only the in-flight carrier changes.
class FlexiblePartialUpdateRegistry {
public:
    static FlexiblePartialUpdateRegistry* instance();

    // Get (creating if absent) the dictionary for a load.
    ColumnSetDictPtr get_or_create(int64_t txn_id);

    // Look up the dictionary for a load; nullptr if none was registered.
    ColumnSetDictPtr get(int64_t txn_id);

    // Drop a load's dictionary (called when the load's writers are all done).
    void erase(int64_t txn_id);

private:
    std::mutex _mu;
    std::unordered_map<int64_t, ColumnSetDictPtr> _by_txn;
};

} // namespace starrocks
